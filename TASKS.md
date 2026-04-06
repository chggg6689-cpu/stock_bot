# TASKS — NASDAQ Intraday Quant System

구현 순서: 의존성 순 (하위 모듈 → 상위 모듈)
승인 후 Task 단위로 순차 구현.

---

## Phase 0 — 준비

- [ ] **T0-1** `config.py` 작성
  - 전체 파라미터 중앙 집중 (UNIVERSE, TP, SL, TOP_N 등)
  - SMAR 유니버스에서 제거
  - SPREAD_TIER 딕셔너리 정의

- [ ] **T0-2** `nasdaq_quant/` 패키지 디렉토리 생성
  - 각 모듈 폴더 + `__init__.py`
  - `requirements.txt` 업데이트

---

## Phase 1 — 모듈 1: 데이터 (`data/`)

- [ ] **T1-1** `data/manager.py` — DataManager 클래스
  - `get_1min(ticker, date)`: SQLite 캐시 우선 조회, 미스 시 yfinance
  - `get_daily(tickers, start, end)`: 일봉 배치 조회
  - `cache_1min(ticker, date, df)`: SQLite 저장

- [ ] **T1-2** `data/universe.py` — 유니버스 선정
  - `get_universe(for_date, top_n)`: 전일 HL×log(Volume) 스코어 상위 top_n
  - MIN_PRICE, MIN_AVG_VOLUME 필터 적용
  - 현행 `select_candidates()` 로직 이식

- [ ] **T1-3** SQLite 스키마 초기화
  - `bars_1min` 테이블 생성 (ticker, timestamp PK)
  - `bars_daily` 테이블 생성

---

## Phase 2 — 모듈 2: 피처 (`features/`)

- [ ] **T2-1** `features/builder.py` — FeatureBuilder 클래스
  - `build_or_features(bars_1min)`: OR 고저, OR 범위, OR 거래량(첫 바 제외)
  - `build_intraday_features(bars_1min, ts)`: VWAP, 갭%, 전일 HL%
  - `market_regime(spy_bars)`: SPY 당일 등락 기반 bull/bear/neutral

- [ ] **T2-2** 피처 단위 테스트
  - OR 첫 바 제외 로직 검증
  - VWAP 계산 정확도 확인

---

## Phase 3 — 모듈 5: 체결 시뮬레이션 (`execution/`)
> 리스크보다 먼저: 백테스트에서 즉시 필요

- [ ] **T3-1** `execution/simulator.py` — ExecutionSimulator 클래스
  - `fill_price(ticker, side, ref_price, shares, avg_vol)`: 슬리피지 반영 체결가
    - spread = SPREAD_BPS[tier] / 2
    - market_impact = 0.1 × (shares × ref_price / (avg_vol × ref_price))
    - 매수: ref × (1 + spread + market_impact)
    - 매도: ref × (1 - spread - market_impact)
  - `commission(shares, price)`: max($1.0, shares × $0.005)
  - `round_trip_cost(...)`: 왕복 비용 합계

- [ ] **T3-2** 슬리피지 단위 테스트
  - 대형주/ETF/중형주 각 시나리오
  - 매수/매도 방향 확인

---

## Phase 4 — 모듈 3: 시그널 (`signals/`)

- [ ] **T4-1** `signals/orb.py` — ORBSignal 클래스
  - `check_entry(features, current_bar)`: 돌파 + 거래량 조건
  - `check_exit(position, current_bar, ts)`: TP/SL/EOD 조건
  - 현행 `simulate_orb()` 로직 이식 + 분리

---

## Phase 5 — 모듈 4: 리스크 (`risk/`)

- [ ] **T5-1** `risk/manager.py` — RiskManager 클래스
  - `position_size(capital, n_positions)`: 동일비중 계산 → 주수 환산
  - `daily_loss_ok(realized_pnl, capital)`: 일일 -5% 초과 시 False
  - `filter_universe(candidates, daily_bars)`: 가격·유동성 필터

---

## Phase 6 — 모듈 6: 백테스트 (`backtest/`)

- [ ] **T6-1** `backtest/engine.py` — Backtester 클래스
  - `run(start, end, top_n, capital)`: 전체 루프
    - 유니버스 선정 → 1분봉 조회 → OR 피처 → 시그널 → 체결 시뮬레이션 → Trade 기록
  - `Trade` 데이터클래스 정의 (gross/net PnL 분리)
  - 일일 최대 손실 초과 시 당일 중단

- [ ] **T6-2** `backtest/engine.py` — Walk-forward
  - `walk_forward(periods)`: 구간 리스트 입력 → 각 구간 결과 반환

- [ ] **T6-3** 기존 `orb_backtest.py` 결과와 cross-check
  - gross PnL 기준으로 동일 파라미터 → 동일 결과 확인
  - net PnL 확인 (슬리피지 반영 후 차이 측정)

---

## Phase 7 — 모듈 7: 리포트 (`report/`)

- [ ] **T7-1** `report/reporter.py` — Reporter 클래스
  - `summary(trades)`: 승률, 평균/누적 수익률(gross/net), Sharpe, MDD, 슬리피지 합계
  - `print_console(trades)`: 현행 출력 형식 유지 + net PnL 추가

- [ ] **T7-2** CSV 저장
  - `to_csv(trades, path)`: 거래 상세 + 요약 행

- [ ] **T7-3** HTML 리포트 (optional)
  - `to_html(trades, path)`: plotly 누적 수익 곡선 + 히트맵

---

## Phase 8 — 통합 & 검증

- [ ] **T8-1** `main.py` 작성
  - CLI: `python main.py backtest --days 30 --top-n 5 --capital 100000`
  - CLI: `python main.py live` (실전 모드)

- [ ] **T8-2** 슬리피지 임팩트 분석
  - gross vs net 누적 수익 비교 출력
  - 종목별 슬리피지 비용 순위

- [ ] **T8-3** 30일 walk-forward 검증
  - 구간 1: 3/6~3/20 / 구간 2: 3/21~4/3

---

## 우선순위 & 의존성

```
T0-1,2 → T1-1,2,3 → T2-1,2
                  → T3-1,2
                  → T4-1
                  → T5-1
                       ↓
                    T6-1,2,3
                       ↓
                    T7-1,2,3
                       ↓
                    T8-1,2,3
```

---

## 슬리피지 예상 임팩트

현행 백테스트 결과 기준 추정:

| 구분 | gross | net (추정) | 슬리피지 |
|------|-------|-----------|---------|
| top5 / 7일 | +29.05% | ~+25% | ~4%p |
| top5 / 14일 | -0.81% | ~-3% | ~2%p |

> 종목당 왕복 슬리피지 약 0.1~0.2% 예상 (대형주 기준)
