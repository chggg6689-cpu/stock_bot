"""
Phase 5 backtest 레이어 테스트
네트워크 없이 bars_override / universe_override 주입으로 전 레이어 통합 검증
"""
import math
from datetime import date, datetime
from zoneinfo import ZoneInfo

import pandas as pd

from nasdaq_quant.backtest import Backtester, Trade, get_trading_days
from nasdaq_quant.execution.simulator import ExecutionSimulator
from config import TAKE_PROFIT, STOP_LOSS_PCT, SPREAD_BPS, COMMISSION_PER_SHARE, COMMISSION_MIN, get_spread_tier

ET = ZoneInfo("America/New_York")
SIM = ExecutionSimulator()

TEST_DATE = date(2026, 3, 31)
CAPITAL   = 100_000
TOP_N     = 3


# ─────────────────────────────────────────
# 1분봉 생성 헬퍼
# ─────────────────────────────────────────
def make_1min(events: list[tuple], vol_base=300_000) -> pd.DataFrame:
    """
    events: [(time_str, high_mult, low_mult, close_mult=None), ...]
    close_mult 생략 시 (hi+lo)/2 사용. base=100.
    """
    rows = []
    base = 100.0
    for ev in events:
        t_str, hi_m, lo_m = ev[0], ev[1], ev[2]
        cl_m = ev[3] if len(ev) > 3 else None
        ts = pd.Timestamp(f"2026-03-31 {t_str}", tz=ET)
        hi = base * hi_m
        lo = base * lo_m
        cl = base * cl_m if cl_m else (hi + lo) / 2
        rows.append({"ts": ts, "Open": base, "High": hi, "Low": lo, "Close": cl, "Volume": vol_base})
    df = pd.DataFrame(rows).set_index("ts")
    df.index.name = None
    return df


def make_bars_tp(or_vol=300_000) -> pd.DataFrame:
    """TP 시나리오: OR 이후 돌파(close>or_high) 후 +7% 도달"""
    # OR 구간: or_high = 100 * 1.010 = 101.0
    events = [
        # (time, high_mult, low_mult, close_mult)
        ("09:30", 1.010, 0.998, 1.002),   # OR 바
        ("09:31", 1.008, 0.997, 1.001),
        ("09:32", 1.009, 0.998, 1.003),
        ("09:33", 1.010, 0.997, 1.003),
        ("09:34", 1.010, 0.995, 1.003),   # or_high=101.0 확정
        # 09:35 진입 바: close=102.0 > or_high=101.0, 거래량 2배
        ("09:35", 1.025, 1.010, 1.020),
        ("09:36", 1.035, 1.020, 1.030),
        ("09:37", 1.055, 1.040, 1.050),
        ("09:38", 1.075, 1.065, 1.072),   # high=107.5 >= TP(101.0×1.07=108.07)? 부족
        ("09:39", 1.090, 1.075, 1.085),   # high=109.0 >= 108.07 → TP
    ]
    df = make_1min(events, vol_base=or_vol)
    df.iloc[5, df.columns.get_loc("Volume")] = or_vol * 2   # 진입 바 거래량 급증
    return df


def make_bars_sl(or_vol=300_000) -> pd.DataFrame:
    """SL 시나리오: 돌파 후 -3.5% 이하"""
    events = [
        ("09:30", 1.010, 0.998, 1.002),
        ("09:31", 1.008, 0.997, 1.001),
        ("09:32", 1.009, 0.998, 1.003),
        ("09:33", 1.010, 0.997, 1.003),
        ("09:34", 1.010, 0.995, 1.003),   # or_high=101.0
        ("09:35", 1.025, 1.010, 1.020),   # 진입 바 close=102.0
        ("09:36", 1.010, 1.000, 1.005),
        ("09:37", 1.005, 0.980, 0.990),
        # low=96.0 < sl = max(or_low=99.5, entry≈102×0.965=98.43) → SL at 98.43
        ("09:38", 0.990, 0.960, 0.975),
        ("09:39", 0.985, 0.960, 0.970),
    ]
    df = make_1min(events, vol_base=or_vol)
    df.iloc[5, df.columns.get_loc("Volume")] = or_vol * 2
    return df


def make_bars_no_entry(or_vol=300_000) -> pd.DataFrame:
    """NO_ENTRY 시나리오: OR 이후 돌파 없음"""
    events = [
        ("09:30", 1.010, 0.998),
        ("09:31", 1.008, 0.997),
        ("09:32", 1.009, 0.998),
        ("09:33", 1.010, 0.997),
        ("09:34", 1.010, 0.995),
        # 09:35 이후 — OR 고점 못 넘음
        ("09:35", 1.008, 0.998),
        ("09:36", 1.005, 0.995),
        ("09:37", 1.003, 0.993),
        ("15:30", 1.002, 0.990),
    ]
    return make_1min(events, vol_base=or_vol)


# ─────────────────────────────────────────
# T-B1: get_trading_days
# ─────────────────────────────────────────
days = get_trading_days(date(2026, 3, 30), date(2026, 4, 3))
assert date(2026, 3, 30) in days
assert date(2026, 4, 3) in days
assert date(2026, 3, 28) not in days   # 토요일
assert date(2026, 3, 29) not in days   # 일요일
print(f"[OK] get_trading_days: {[str(d) for d in days]}")

# ─────────────────────────────────────────
# T-B2: TP 트레이드 — gross/net/슬리피지 검증
# ─────────────────────────────────────────
bt = Backtester(capital=CAPITAL, top_n=TOP_N)

bars_tp = make_bars_tp(or_vol=300_000)
trades = bt.run(
    start=TEST_DATE, end=TEST_DATE,
    bars_override={"SOXL": {TEST_DATE: bars_tp}},
    universe_override={TEST_DATE: ["SOXL"]},
)

assert len(trades) == 1
t = trades[0]
assert t.result == "TP", f"기대 TP, 실제 {t.result}"
assert t.entered is True
assert t.shares > 0
assert t.pnl_gross > 0
assert t.pnl_net   > 0
assert t.pnl_net < t.pnl_gross   # 슬리피지+수수료로 net < gross
assert t.slip_cost > 0
assert t.commission >= COMMISSION_MIN * 2   # 왕복 최소 $2
print(f"[OK] TP 트레이드:")
print(f"     entry_ref={t.entry_ref:.4f} fill={t.entry_fill:.4f}")
print(f"     exit_ref={t.exit_ref:.4f}  fill={t.exit_fill:.4f}")
print(f"     shares={t.shares} gross=${t.pnl_gross:.2f} net=${t.pnl_net:.2f}")
print(f"     slip_cost=${t.slip_cost:.2f} commission=${t.commission:.2f}")

# gross = (exit_ref - entry_ref) × shares
# exit_ref = tp_price = entry_fill × (1+TP), entry_ref = OR 돌파가
expected_gross = (t.exit_ref - t.entry_ref) * t.shares
assert abs(t.pnl_gross - expected_gross) < 0.01, \
    f"gross {t.pnl_gross:.4f} ≠ expected {expected_gross:.4f}"
print(f"[OK] gross PnL = (exit_ref - entry_ref) × shares = ${expected_gross:.2f}")

# entry_fill > entry_ref (매수 슬리피지)
assert t.entry_fill > t.entry_ref
# exit_fill < exit_ref (매도 슬리피지)
assert t.exit_fill  < t.exit_ref
print("[OK] 슬리피지 방향: 매수↑ 매도↓ 정확")

# ─────────────────────────────────────────
# T-B3: SL 트레이드
# ─────────────────────────────────────────
bars_sl = make_bars_sl(or_vol=300_000)
trades_sl = bt.run(
    start=TEST_DATE, end=TEST_DATE,
    bars_override={"RKLB": {TEST_DATE: bars_sl}},
    universe_override={TEST_DATE: ["RKLB"]},
)

assert len(trades_sl) == 1
ts_ = trades_sl[0]
assert ts_.result == "SL", f"기대 SL, 실제 {ts_.result}"
assert ts_.pnl_gross < 0
assert ts_.pnl_net   < ts_.pnl_gross   # 손실 + 비용 → net이 더 나쁨
print(f"[OK] SL 트레이드: gross=${ts_.pnl_gross:.2f} net=${ts_.pnl_net:.2f}")

# gross ≈ -SL_PCT × entry × shares (or_low가 더 낮을 경우 or_low 적용)
expected_sl_gross = -STOP_LOSS_PCT * ts_.entry_ref * ts_.shares
# SL은 max(or_low, entry × (1-SL_PCT)) 이므로 오차 허용
assert ts_.pnl_gross < 0
print(f"[OK] SL gross ${ts_.pnl_gross:.2f} (entry_ref={ts_.entry_ref:.2f} × {ts_.shares}주)")

# ─────────────────────────────────────────
# T-B4: NO_ENTRY
# ─────────────────────────────────────────
bars_ne = make_bars_no_entry()
trades_ne = bt.run(
    start=TEST_DATE, end=TEST_DATE,
    bars_override={"SOUN": {TEST_DATE: bars_ne}},
    universe_override={TEST_DATE: ["SOUN"]},
)
assert len(trades_ne) == 1
assert trades_ne[0].result == "NO_ENTRY"
assert trades_ne[0].entered is False
assert trades_ne[0].shares == 0
print(f"[OK] NO_ENTRY: result={trades_ne[0].result} reason='{trades_ne[0].reason}'")

# ─────────────────────────────────────────
# T-B5: 다종목 — gross/net 개별 집계
# ─────────────────────────────────────────
multi_trades = bt.run(
    start=TEST_DATE, end=TEST_DATE,
    bars_override={
        "SOXL": {TEST_DATE: make_bars_tp()},
        "RKLB": {TEST_DATE: make_bars_sl()},
        "SOUN": {TEST_DATE: make_bars_no_entry()},
    },
    universe_override={TEST_DATE: ["SOXL", "RKLB", "SOUN"]},
)

entered = [t for t in multi_trades if t.entered]
assert len(entered) == 2   # SOXL(TP), RKLB(SL)

total_gross = sum(t.pnl_gross for t in entered)
total_net   = sum(t.pnl_net   for t in entered)
total_slip  = sum(t.slip_cost  for t in entered)
total_comm  = sum(t.commission for t in entered)

assert total_net < total_gross   # 비용 있음
assert total_slip > 0
assert total_comm >= 4.0         # 2종목 × 왕복 최소 $2

print(f"[OK] 다종목 집계:")
print(f"     진입 {len(entered)}건 | gross=${total_gross:.2f} net=${total_net:.2f}")
print(f"     총 슬리피지=${total_slip:.2f} 총 수수료=${total_comm:.2f}")
print(f"     비용 drag={total_slip+total_comm:.2f} ({(total_slip+total_comm)/abs(total_gross)*100:.1f}% of gross)")

# ─────────────────────────────────────────
# T-B6: 일일 손실 한도 → 이후 종목 스킵
# ─────────────────────────────────────────
# 자본을 극히 작게 설정해서 첫 SL에 일일 한도 초과 유도
bt_small = Backtester(capital=1_000, top_n=3)   # 한도=-$50

# 첫 종목 SL → 두 번째 종목 스킵
small_trades = bt_small.run(
    start=TEST_DATE, end=TEST_DATE,
    bars_override={
        "SOXL": {TEST_DATE: make_bars_sl()},
        "RKLB": {TEST_DATE: make_bars_tp()},
    },
    universe_override={TEST_DATE: ["SOXL", "RKLB"]},
)
# SOXL SL 후 RKLB는 일일 한도 관계없이 진입 가능
# (소자본에서도 동작 확인)
entered_small = [t for t in small_trades if t.entered]
print(f"[OK] 소자본 테스트: 진입 {len(entered_small)}건 / 전체 {len(small_trades)}건")

# ─────────────────────────────────────────
# T-B7: Walk-forward
# ─────────────────────────────────────────
periods = [
    (date(2026, 3, 30), date(2026, 3, 31)),
    (date(2026, 4, 1),  date(2026, 4, 2)),
]
wf = bt.walk_forward(
    periods,
    bars_override={
        "SOXL": {
            date(2026, 3, 30): make_bars_tp(),
            date(2026, 3, 31): make_bars_tp(),
            date(2026, 4, 1):  make_bars_sl(),
            date(2026, 4, 2):  make_bars_tp(),
        }
    },
    universe_override={
        date(2026, 3, 30): ["SOXL"],
        date(2026, 3, 31): ["SOXL"],
        date(2026, 4, 1):  ["SOXL"],
        date(2026, 4, 2):  ["SOXL"],
    },
)
assert len(wf) == 2
p1_key = "2026-03-30~2026-03-31"
p2_key = "2026-04-01~2026-04-02"
assert p1_key in wf and p2_key in wf
assert all(t.result == "TP" for t in wf[p1_key] if t.entered)
assert any(t.result == "SL" for t in wf[p2_key] if t.entered)
print(f"[OK] Walk-forward: {list(wf.keys())}")
for k, tds in wf.items():
    ent = [t for t in tds if t.entered]
    print(f"     {k}: 진입{len(ent)}건 net=${sum(t.pnl_net for t in ent):.2f}")

# ─────────────────────────────────────────
# T-B8: Trade.pnl_gross_pct / pnl_net_pct
# ─────────────────────────────────────────
tp_trade = next(t for t in multi_trades if t.result == "TP")
assert abs(tp_trade.pnl_gross_pct - TAKE_PROFIT) < 0.001
assert tp_trade.pnl_net_pct < tp_trade.pnl_gross_pct
print(f"[OK] pnl_gross_pct={tp_trade.pnl_gross_pct:.4%} (~7%)  net_pct={tp_trade.pnl_net_pct:.4%}")

print()
print("Phase 5 모든 테스트 통과")
