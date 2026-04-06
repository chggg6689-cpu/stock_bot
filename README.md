# NASDAQ Intraday Quant System

ORB(Opening Range Breakout) 전략 기반 나스닥 단타 퀀트 시스템.
슬리피지·스프레드·수수료를 반영한 현실적 백테스트 엔진과 리포트 레이어를 포함한다.

---

## 아키텍처

```
config.py                    ← 전체 파라미터 중앙 관리
nasdaq_quant/
├── data/        (Phase 1)   ← 1분봉 수집·캐시(SQLite), 유니버스 선정
├── features/    (Phase 2)   ← OR 피처, VWAP, 레짐 피처, 장중 RS
├── signals/     (Phase 3)   ← ORB 진입·청산 시그널, 레짐 필터
├── risk/        (Phase 4)   ← 포지션 사이징, 일일 손실 한도, 유니버스 필터
├── execution/   (Phase 5)   ← 슬리피지·스프레드·수수료 시뮬레이션
├── backtest/    (Phase 6)   ← Backtester, walk-forward, Trade 결과
└── report/      (Phase 7)   ← Reporter (summary, CSV, HTML)
main.py                      ← CLI 진입점
```

---

## 전략 개요

| 항목 | 값 |
|------|----|
| 전략 | Opening Range Breakout (ORB) |
| OR 구간 | 09:30 ~ 09:34 ET |
| 진입 조건 | close > or_high AND volume > or_vol_avg × 1.0 |
| 진입 창 | 09:35 ~ 13:00 ET |
| Take Profit | +7% (진입 체결가 기준) |
| Stop Loss | max(or_low, 진입가 × -3.5%) |
| 강제 청산 | 15:30 ET |
| 유니버스 | NASDAQ 대형주 + 고변동성 ETF 88종목 |
| 포지션 수 | 상위 5종목 (동일비중) |
| 일일 손실 한도 | 자본의 -5% 초과 시 당일 매매 중단 |

---

## 슬리피지 모델

```
체결가 = 기준가 × (1 ± (half_spread + market_impact))

half_spread:
  대형주 (NVDA, AMD 등)    1bp  (0.01%)
  레버리지 ETF (SOXL 등)   3bp  (0.03%)
  중소형주 (SOUN, RKLB 등) 5bp  (0.05%)

market_impact = 0.10 × (주문금액 / 일평균거래대금)
수수료 = max($1.00, 주수 × $0.005)   # Alpaca 기준
```

---

## 설치

```bash
pip install -r requirements.txt
```

필수 패키지: `pandas`, `numpy`, `yfinance`, `plotly`, `python-dotenv`

---

## 실행 방법

### 백테스트

```bash
# 최근 14일 (기본)
python main.py backtest

# 날짜 지정
python main.py backtest --start 2026-03-01 --end 2026-03-31

# 파라미터 지정 + 결과 저장
python main.py backtest --start 2026-03-01 --end 2026-03-31 \
    --top-n 5 --capital 100000 --output results/march

# 상세 로그
python main.py backtest --days 7 --verbose
```

결과 파일:
- `results/march.csv`  — 거래 상세 + 요약 행
- `results/march.html` — plotly 누적 수익 곡선 (브라우저로 열기)

### walk-forward (Python API)

```python
from datetime import date
from nasdaq_quant.backtest import Backtester
from nasdaq_quant.report import Reporter

bt = Backtester(capital=100_000, top_n=5)
periods = [
    (date(2026, 3, 1),  date(2026, 3, 15)),
    (date(2026, 3, 16), date(2026, 3, 31)),
]
results = bt.walk_forward(periods)

rp = Reporter(capital=100_000)
for key, trades in results.items():
    print(f"\n=== {key} ===")
    rp.print_console(trades)
```

---

## 테스트 방법

```bash
# 레이어별 단위 테스트 (순서대로)
python test_phase1.py   # data
python test_phase2.py   # features + execution
python test_phase3.py   # signals
python test_phase4.py   # risk
python test_phase5.py   # backtest 엔진
python test_phase6.py   # report
python test_phase7.py   # 통합 E2E

# 전체 한 번에
python -m pytest test_phase*.py -v   # pytest 설치 시
```

모든 테스트는 **네트워크 없이** `bars_override` / `universe_override` 주입으로 실행된다.

---

## 리포트 지표

| 지표 | 설명 |
|------|------|
| Win Rate | 수익 거래 / 전체 진입 거래 |
| Profit Factor | 총 수익 / 총 손실 (net 기준) |
| Sharpe Ratio | 일별 net 수익률 기반, 연환산 (√252) |
| MDD | 누적 equity 고점 대비 최대 낙폭 ($) |
| Gross PnL | 슬리피지 전 손익 |
| Net PnL | 슬리피지 + 수수료 후 실질 손익 |
| 비용 drag | (슬리피지 + 수수료) / \|gross PnL\| |

---

## 현재 한계점

### 데이터
- **실시간 데이터 없음**: 백테스트는 yfinance 일봉/1분봉만 사용. 틱 데이터 미지원.
- **데이터 품질**: yfinance는 분할·배당 조정 정확도 보장 안 됨. 실전 전 Alpaca/Polygon 데이터 교체 권장.
- **공휴일 처리**: `get_trading_days()`가 US 공휴일을 제외하지 않음 (주말만 제외). 데이터 없는 날은 자동 스킵되나, 명시적 거래소 캘린더 필요.

### 전략
- **롱 전용**: 숏(공매도) 시그널 없음.
- **레버리지 ETF 리밸런싱 드래그**: SOXL 등의 일중 복리 손실(decay) 미반영.
- **갭 리스크**: 오버나이트 갭은 시뮬레이션 대상 아님 (당일 장중만 포지션 보유).
- **OR 품질 필터 미흡**: or_range가 극단적으로 좁거나 넓은 날 필터 없음.

### 체결 시뮬레이션
- **고정 슬리피지 티어**: 장 중 변동성·시간대별 스프레드 변화 미반영.
- **시장충격 선형 가정**: 대량 주문의 비선형 충격 미반영.
- **부분 체결 없음**: 유동성 부족 시 체결 실패 시나리오 없음.

### 리스크
- **상관관계 미고려**: 동일 섹터(예: SOXL+AMD) 동시 진입 시 실질 리스크 과소평가.
- **레짐 필터 단순**: QQQ 당일 등락률만 사용. VIX, 섹터 지수 미활용.

---

## 추후 개선 사항

### 단기 (1~2주)
- [ ] US 거래소 공휴일 캘린더 적용 (`pandas_market_calendars` 또는 하드코딩)
- [ ] OR range 필터 추가 (너무 좁은 OR 제외, 예: or_range < 0.3% 스킵)
- [ ] 숏 ORB 시그널 (or_low 이탈 + 거래량 급증)

### 중기 (1개월)
- [ ] Alpaca 실시간 데이터 연동 (WebSocket 스트림)
- [ ] 실전 주문 실행 레이어 (`execution/live.py`) 구현
- [ ] 섹터 상관관계 기반 동시 포지션 제한
- [ ] VIX 레짐 필터 추가 (VIX > 30 → 포지션 사이즈 50% 축소)
- [ ] 결과 DB 저장 + 대시보드 (Streamlit)

### 장기
- [ ] ML 기반 OR 품질 스코어 (진입 여부 필터링)
- [ ] 파라미터 최적화 (Optuna walk-forward)
- [ ] 틱 데이터 기반 정밀 슬리피지 모델
- [ ] PM 2.0 워크플로우 (Alpaca paper → live 단계적 전환)

---

## 실거래 전 체크리스트

### 시스템 검증
- [ ] 30일 이상 walk-forward 백테스트 완료 (구간별 일관된 성과)
- [ ] net Sharpe > 1.0 (일별 수익률 기준)
- [ ] Max Drawdown < 자본의 15%
- [ ] Profit Factor > 1.3
- [ ] 승률 > 40% (ORB 특성상 TP 7% vs SL 3.5% → 손익비 2:1)

### 데이터 검증
- [ ] yfinance → Alpaca 마켓 데이터 API 교체 완료
- [ ] 공휴일 캘린더 적용 확인
- [ ] 1분봉 SQLite 캐시 무결성 검증

### 브로커 연동
- [ ] Alpaca Paper Trading 30일 실전 모의투자 완료
- [ ] 평균 체결가 vs 시뮬레이션 체결가 오차 < 0.05% 확인
- [ ] 주문 거부·부분 체결 케이스 처리 로직 구현
- [ ] API Rate Limit 핸들링 확인
- [ ] 장 시작 전 포지션 0 확인 로직 (오버나이트 포지션 방지)

### 운영
- [ ] 시스템 시간 ET(Eastern Time) 동기화 확인
- [ ] 09:30 이전 프리마켓 데이터 수집 자동화
- [ ] 에러 알림 (Slack/Telegram) 연동
- [ ] 일일 손실 한도 도달 시 자동 매매 중단 확인
- [ ] 긴급 전체 청산 스크립트 준비

### 자본 관리
- [ ] 초기 실거래 자본 < $10,000 (검증 기간)
- [ ] 실거래 전 최소 3개월 paper trading 수익률 기록
- [ ] 세금·수수료 실비용 재계산 (Alpaca PFOF 등)
