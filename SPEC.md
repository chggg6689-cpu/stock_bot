# NASDAQ Intraday Quant System — SPEC

## 1. 개요

나스닥 단타 퀀트 시스템. ORB(Opening Range Breakout) 전략 기반으로,
슬리피지·스프레드를 반영한 현실적 백테스트와 실전 자동매매를 지원한다.

현재 코드(`orb_backtest.py`, `volatility_trader.py`)를 기반으로
아래 7개 모듈로 완전 재설계한다.

---

## 2. 현재 상태 (AS-IS)

| 파일 | 역할 | 문제점 |
|------|------|--------|
| `orb_backtest.py` | ORB 백테스트 | 슬리피지 없음, 모놀리식 |
| `volatility_trader.py` | 실전 트레이더 | 체결 시뮬레이션 없음 |
| `strategy_engine.py` | 월봉 백테스트 | 고정 0.2% 비용, 일봉 기준 |
| `data_collector.py` | 데이터 수집 | 1분봉 캐시 없음 |
| `alpaca_trader.py` | Momentum 주간 | ORB와 무관 |

---

## 3. 목표 아키텍처 (TO-BE)

```
nasdaq_quant/
├── data/           # 모듈 1: 데이터 수집·캐시
├── features/       # 모듈 2: 피처 엔지니어링
├── signals/        # 모듈 3: 시그널 생성
├── risk/           # 모듈 4: 리스크 관리
├── execution/      # 모듈 5: 체결 시뮬레이션
├── backtest/       # 모듈 6: 백테스트 엔진
├── report/         # 모듈 7: 성과 리포트
└── main.py         # 진입점
```

---

## 4. 모듈별 상세 명세

### 모듈 1 — `data/` 데이터 수집

**책임:** 1분봉 데이터 수집, 로컬 캐시(SQLite), 유니버스 관리

**핵심 클래스/함수:**
```python
class DataManager:
    def get_1min(ticker, date) -> pd.DataFrame       # 캐시 우선, 없으면 yfinance
    def get_daily(tickers, start, end) -> pd.DataFrame
    def get_universe(method="score") -> list[str]    # 전일 HL×Volume 스코어 상위
    def cache_1min(ticker, date, df)                 # SQLite 저장
```

**캐시 스키마:**
```sql
CREATE TABLE bars_1min (
    ticker TEXT, date TEXT, timestamp TEXT,
    open REAL, high REAL, low REAL, close REAL, volume INTEGER,
    PRIMARY KEY (ticker, timestamp)
);
```

**유니버스:**
- NASDAQ 100 대형주 + 고변동성 레버리지 ETF (현행 유지)
- `SMAR` 제거 (상장폐지)

---

### 모듈 2 — `features/` 피처 엔지니어링

**책임:** OR 지표, 기술적 지표, 시장 레짐 피처 계산

**피처 목록:**

| 피처 | 설명 |
|------|------|
| `or_high`, `or_low`, `or_range` | 09:30~09:34 고저폭 |
| `or_vol_avg` | OR 거래량 평균 (첫 바 제외) |
| `prev_hl_pct` | 전일 고저폭 / 종가 |
| `prev_vol_score` | 전일 HL% × log10(Volume) |
| `vwap` | 당일 VWAP (09:30~현재) |
| `rsi_5m` | 5분봉 RSI(14) |
| `spy_trend` | SPY 당일 등락률 (시장 레짐) |
| `gap_pct` | 전일 종가 대비 오늘 오픈 갭 |

```python
class FeatureBuilder:
    def build_or_features(bars_1min) -> dict          # OR 피처
    def build_intraday_features(bars_1min, ts) -> dict # 실시간 피처
    def market_regime(spy_bars) -> str                 # "bull"/"bear"/"neutral"
```

---

### 모듈 3 — `signals/` 시그널 생성

**책임:** 진입·청산 시그널 결정

**ORB 시그널 조건:**
```
진입 (LONG):
  - 현재가 > or_high × (1 + ORB_BUFFER)
  - 현재 바 거래량 > or_vol_avg × VOL_SURGE_MULT
  - 시간 09:35 ~ ENTRY_DEADLINE (13:00)
  - spy_trend > -0.01 (옵션: 시장 레짐 필터)

청산:
  - TP: 진입가 × (1 + TAKE_PROFIT)        # 7%
  - SL: max(or_low, 진입가 × (1 - SL_PCT)) # -3.5%
  - EOD: 15:30 강제 청산
```

```python
class ORBSignal:
    def check_entry(features, current_bar) -> bool
    def check_exit(position, current_bar) -> str | None  # "TP"/"SL"/"EOD"/None
```

**파라미터 (config.py 중앙 관리):**
```python
TAKE_PROFIT     = 0.07
STOP_LOSS_PCT   = 0.035
ORB_BUFFER      = 0.0
VOL_SURGE_MULT  = 1.0
ENTRY_DEADLINE  = (13, 0)
FORCE_CLOSE     = (15, 30)
TOP_N           = 5
```

---

### 모듈 4 — `risk/` 리스크 관리

**책임:** 포지션 사이징, 포트폴리오 레벨 리스크 제한

**규칙:**

| 규칙 | 값 |
|------|-----|
| 종목당 자본 비중 | 동일비중 (1/TOP_N) |
| 하루 최대 손실 | 총자본의 -5% 초과 시 당일 매매 중단 |
| 동시 최대 포지션 | TOP_N개 |
| 최소 주가 | $5 이상 |
| 최소 평균 거래량 | 1,000,000주/일 |

```python
class RiskManager:
    def position_size(capital, n_positions) -> float   # 종목당 투자금
    def daily_loss_ok(realized_pnl, capital) -> bool   # 당일 매매 계속 여부
    def filter_universe(candidates) -> list[str]       # 가격·유동성 필터
```

---

### 모듈 5 — `execution/` 체결 시뮬레이션

**책임:** 슬리피지, 스프레드, 시장충격 모델링

**슬리피지 모델:**

```
체결가 = 기준가 × (1 + slip_dir × total_slip)

total_slip = spread_pct/2 + market_impact

spread_pct:
  - 대형주 (NVDA, AAPL 등):  0.01% (1bp)
  - 중형주 (RKLB, SOUN 등):  0.05% (5bp)
  - 레버리지 ETF (SOXL 등):  0.03% (3bp)

market_impact = 0.1 × (order_size / avg_daily_volume)
  # 일평균 거래량의 0.1% 주문 → +0.01% 추가 슬리피지

매수 slip_dir = +1 (불리하게)
매도 slip_dir = -1 (불리하게)
```

**수수료:**
```
commission = max($1.0, shares × $0.005)   # Alpaca 기준
```

```python
class ExecutionSimulator:
    def fill_price(ticker, side, ref_price, shares, avg_vol) -> float
    def commission(shares, price) -> float
    def round_trip_cost(ticker, shares, entry, exit_price, avg_vol) -> float
```

**티어 분류 (config):**
```python
SPREAD_TIER = {
    "large":     ["NVDA","AMD","META","TSLA","AAPL","AMZN","MSFT","NFLX","GOOGL"],
    "etf":       ["SOXL","TQQQ","UVXY","SQQQ","SPXL","TECL","FNGU","LABU"],
    "mid":       [],   # 나머지 전부
}
SPREAD_BPS = {"large": 0.0001, "etf": 0.0003, "mid": 0.0005}
```

---

### 모듈 6 — `backtest/` 백테스트 엔진

**책임:** 전체 시뮬레이션 루프, 성과 계산

**흐름:**
```
for each trading_day:
    1. DataManager.get_universe()  → candidates
    2. DataManager.get_1min()      → bars
    3. FeatureBuilder.build_or_features() → features
    4. for each bar (09:35~15:30):
         ORBSignal.check_entry() → enter if True
         ExecutionSimulator.fill_price() → entry_fill
         ORBSignal.check_exit()  → exit if not None
         ExecutionSimulator.fill_price() → exit_fill
         ExecutionSimulator.commission() → cost
    5. RiskManager.daily_loss_ok() → 초과 시 중단
    6. 결과 Trade 객체 저장
```

**Trade 데이터클래스:**
```python
@dataclass
class Trade:
    ticker: str
    date: date
    entry_time: datetime
    exit_time: datetime
    entry_ref: float       # OR 돌파 기준가
    entry_fill: float      # 실제 체결가 (슬리피지 반영)
    exit_ref: float        # TP/SL/EOD 기준가
    exit_fill: float       # 실제 체결가
    shares: int
    commission: float
    result: str            # "TP"/"SL"/"EOD"
    pnl_gross: float       # 슬리피지 전
    pnl_net: float         # 슬리피지 + 수수료 후
    slip_cost: float       # 슬리피지 비용
```

**Walk-forward 지원:**
```python
class Backtester:
    def run(start, end, top_n, capital) -> list[Trade]
    def walk_forward(periods) -> dict[str, list[Trade]]  # 구간별 결과
```

---

### 모듈 7 — `report/` 성과 리포트

**책임:** 지표 계산, 콘솔 출력, CSV/HTML 저장

**출력 지표:**

| 지표 | 설명 |
|------|------|
| 총 거래 수 | 진입 / 미진입 분리 |
| 승률 | 수익 거래 / 전체 진입 |
| 평균 수익률 | gross / net 분리 |
| 누적 수익률 | gross / net 분리 |
| Sharpe Ratio | 일별 수익률 기준 |
| Max Drawdown | 누적 고점 대비 |
| 슬리피지 비용 합계 | gross - net 차이 |
| TP/SL/EOD 건수 | 청산 유형별 |
| 종목별 승률 | 종목별 분해 |

```python
class Reporter:
    def summary(trades: list[Trade]) -> dict
    def print_console(trades)
    def to_csv(trades, path)
    def to_html(trades, path)          # 차트 포함 (plotly)
```

---

## 5. 설정 파일 구조

```python
# config.py
UNIVERSE = [...]          # 현행 유지 (SMAR 제거)
TOP_N           = 5
TAKE_PROFIT     = 0.07
STOP_LOSS_PCT   = 0.035
ORB_BUFFER      = 0.0
VOL_SURGE_MULT  = 1.0
ENTRY_DEADLINE  = (13, 0)
FORCE_CLOSE     = (15, 30)
MIN_PRICE       = 5.0
MIN_AVG_VOLUME  = 1_000_000
CAPITAL         = 100_000   # 백테스트 초기 자본 (달러)
MAX_DAILY_LOSS  = 0.05      # 일일 최대 손실 5%
```

---

## 6. 의존성

```
yfinance>=0.2
pandas>=2.0
numpy>=1.26
pandas-ta>=0.3
alpaca-py>=0.8
plotly>=5.0
python-dotenv
```

---

## 7. 기존 코드 처리 방침

| 파일 | 처리 |
|------|------|
| `orb_backtest.py` | `backtest/` + `execution/`로 대체 |
| `volatility_trader.py` | `signals/` + 실전 연동 레이어로 대체 |
| `strategy_engine.py` | 월봉 전략은 별도 유지 (수정 없음) |
| `alpaca_trader.py` | 별도 유지 (Momentum 전략) |
| `data_collector.py` | `data/`로 흡수·확장 |
