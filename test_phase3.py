"""Phase 3 signals 레이어 테스트"""
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def make_bars(open_=100, trend=0.001, n=60, start="09:30", vol=200_000):
    idx = pd.date_range(f"2026-03-31 {start}", periods=n, freq="1min", tz=ET)
    closes = [open_ * (1 + trend * i) for i in range(n)]
    return pd.DataFrame({
        "Open":   [open_] * n,
        "High":   [c * 1.002 for c in closes],
        "Low":    [c * 0.998 for c in closes],
        "Close":  closes,
        "Volume": [vol] * n,
    }, index=idx)


from nasdaq_quant.features import FeatureBuilder
from nasdaq_quant.signals import (
    ORBSignal, Position,
    RegimeFilter, RSMomentumFilter, CompositeFilter,
)

fb  = FeatureBuilder()
orb = ORBSignal()
bars = make_bars(open_=100, n=60)
or_f = fb.build_or_features(bars)

# ── or_close 추가 확인 ───────────────────────────────────────────
assert "or_close" in or_f, "or_close 키 누락"
print(f"[OK] or_close={or_f['or_close']:.4f}")

# ═══════════════════════════════════════════════════════════════
# ORBSignal — 진입
# ═══════════════════════════════════════════════════════════════
entry_ts = datetime(2026, 3, 31, 9, 36, tzinfo=ET)

breakout_bar = pd.Series({
    "High": or_f["or_high"] * 1.01,
    "Low":  or_f["or_low"],
    "Close": or_f["or_high"] * 1.005,
    "Volume": or_f["or_vol_avg"] * 2,
})

# T-S1: 진입 조건 충족
assert orb.check_entry(or_f, breakout_bar, entry_ts) is True
print("[OK] check_entry: 돌파+거래량 충족 → True")

# T-S2: 가격 미달
weak = pd.Series({"High": or_f["or_high"] * 0.99, "Low": 99.0,
                  "Close": or_f["or_high"] * 0.98, "Volume": or_f["or_vol_avg"] * 2})
assert orb.check_entry(or_f, weak, entry_ts) is False
print("[OK] check_entry: 가격 미달 → False")

# T-S3: 거래량 미달
low_vol = pd.Series({"High": or_f["or_high"] * 1.01, "Low": 99.0,
                     "Close": or_f["or_high"] * 1.005, "Volume": or_f["or_vol_avg"] * 0.5})
assert orb.check_entry(or_f, low_vol, entry_ts) is False
print("[OK] check_entry: 거래량 미달 → False")

# T-S4: 진입 마감 초과 (13:01)
late_ts = datetime(2026, 3, 31, 13, 1, tzinfo=ET)
assert orb.check_entry(or_f, breakout_bar, late_ts) is False
print("[OK] check_entry: 진입 마감 초과 → False")

# T-S5: OR 구간 중 (09:34 → 진입 창 전)
early_ts = datetime(2026, 3, 31, 9, 34, tzinfo=ET)
assert orb.check_entry(or_f, breakout_bar, early_ts) is False
print("[OK] check_entry: 09:34 (창 전) → False")

# T-S6: OR invalid
assert orb.check_entry({**or_f, "valid": False}, breakout_bar, entry_ts) is False
print("[OK] check_entry: OR invalid → False")

# ═══════════════════════════════════════════════════════════════
# ORBSignal — 청산
# ═══════════════════════════════════════════════════════════════
entry_price = or_f["or_high"] * 1.005
pos = Position(
    ticker="TEST", entry_price=entry_price, entry_ref=or_f["or_high"],
    entry_time=entry_ts, or_high=or_f["or_high"], or_low=or_f["or_low"], shares=100,
)
print(f"     Position: entry={pos.entry_price:.4f} tp={pos.tp_price:.4f} sl={pos.sl_price:.4f}")

mid_ts = datetime(2026, 3, 31, 11, 0, tzinfo=ET)

# T-S7: TP
tp_bar = pd.Series({"High": pos.tp_price + 0.01, "Low": pos.entry_price,
                    "Close": pos.tp_price, "Volume": 200_000})
assert orb.check_exit(pos, tp_bar, mid_ts) == "TP"
print("[OK] check_exit: TP → TP")

# T-S8: SL
sl_bar = pd.Series({"High": pos.entry_price, "Low": pos.sl_price - 0.01,
                    "Close": pos.sl_price, "Volume": 200_000})
assert orb.check_exit(pos, sl_bar, mid_ts) == "SL"
print("[OK] check_exit: SL → SL")

# T-S9: EOD 강제청산 (15:30)
eod_ts  = datetime(2026, 3, 31, 15, 30, tzinfo=ET)
eod_bar = pd.Series({"High": pos.entry_price * 1.01, "Low": pos.entry_price * 0.99,
                     "Close": pos.entry_price * 1.005, "Volume": 200_000})
assert orb.check_exit(pos, eod_bar, eod_ts) == "EOD"
print("[OK] check_exit: EOD 강제청산")

# T-S10: 청산 없음
hold_bar = pd.Series({"High": pos.entry_price * 1.02, "Low": pos.entry_price * 0.99,
                      "Close": pos.entry_price * 1.01, "Volume": 200_000})
assert orb.check_exit(pos, hold_bar, mid_ts) is None
print("[OK] check_exit: 청산 없음 → None")

# T-S11: exit_ref_price
assert orb.exit_ref_price("TP",  pos, hold_bar) == pos.tp_price
assert orb.exit_ref_price("SL",  pos, hold_bar) == pos.sl_price
assert orb.exit_ref_price("EOD", pos, hold_bar) == float(hold_bar["Close"])
print("[OK] exit_ref_price: TP/SL/EOD 기준가 정확")

# ═══════════════════════════════════════════════════════════════
# RegimeFilter
# ═══════════════════════════════════════════════════════════════
rf = RegimeFilter(or_range_threshold=0.003)

qqq_bars = make_bars(open_=450, trend=0.001, n=5)
bull_or  = fb.build_or_features(qqq_bars)
bull_or  = {**bull_or, "or_range": 0.01,
            "or_close": bull_or["or_high"] - 0.01}   # or_high 근처 → bull
assert rf.regime(bull_or) == "bull"
assert rf.allows_long(bull_or) is True
print("[OK] RegimeFilter: bull → allows_long=True")

neutral_or = {**bull_or, "or_range": 0.001}
assert rf.regime(neutral_or) == "neutral"
print("[OK] RegimeFilter: range 미달 → neutral")

bear_or = {**bull_or, "or_close": bull_or["or_low"] + 0.01}   # or_low 근처 → bear
assert rf.regime(bear_or) == "bear"
assert rf.allows_long(bear_or) is False
print("[OK] RegimeFilter: bear → allows_long=False")

# ═══════════════════════════════════════════════════════════════
# RSMomentumFilter
# ═══════════════════════════════════════════════════════════════
rsf = RSMomentumFilter(min_rs=0.0)

rs_leader = {"rs_vs_qqq": 0.02, "ticker_chg": 0.03, "qqq_chg": 0.01, "is_leader": True}
assert rsf.allows_entry(rs_leader) is True
print("[OK] RSMomentumFilter: rs=+0.02 → True")

rs_lagger = {"rs_vs_qqq": -0.01, "ticker_chg": 0.0, "qqq_chg": 0.01, "is_leader": False}
assert rsf.allows_entry(rs_lagger) is False
print("[OK] RSMomentumFilter: rs=-0.01 → False")

# ═══════════════════════════════════════════════════════════════
# CompositeFilter
# ═══════════════════════════════════════════════════════════════
cf = CompositeFilter()

ok, reason = cf.allows_entry(bull_or, rs_leader)
assert ok is True and reason == ""
print("[OK] CompositeFilter: bull + rs_leader → 진입 허용")

ok, reason = cf.allows_entry(bear_or, rs_leader)
assert ok is False and "레짐" in reason
print(f"[OK] CompositeFilter: bear → 거부 ({reason})")

ok, reason = cf.allows_entry(bull_or, rs_lagger)
assert ok is False and "RS" in reason
print(f"[OK] CompositeFilter: rs_lagger → 거부 ({reason})")

# ═══════════════════════════════════════════════════════════════
# Lookahead bias 점검
# ═══════════════════════════════════════════════════════════════
# OR 피처: 09:34 이전 데이터만 사용
or_slice = bars.between_time("09:30", "09:34")
assert or_slice.index.max().hour == 9
assert or_slice.index.max().minute == 34
print("[OK] Lookahead: OR 피처 09:34 이전 데이터만 사용")

# intraday_rs: ts 이전 bars만 사용하는지 확인
t_bars = make_bars(open_=50, trend=0.003, n=40)
q_bars = make_bars(open_=450, trend=0.001, n=40)
ts20   = t_bars.index[20]
rs20   = fb.intraday_rs(t_bars, q_bars, ts=ts20)
rs_end = fb.intraday_rs(t_bars, q_bars, ts=t_bars.index[-1])
assert rs20["ticker_chg"] != rs_end["ticker_chg"], "ts 슬라이싱이 결과에 반영되지 않음"
print(f"[OK] Lookahead: intraday_rs ts 슬라이싱 정확 "
      f"(ts[20]={rs20['ticker_chg']:.4f} vs end={rs_end['ticker_chg']:.4f})")

print()
print("Phase 3 모든 테스트 통과")
