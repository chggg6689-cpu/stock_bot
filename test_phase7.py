"""
Phase 7 통합 테스트
config → data → features → signals → risk → execution → backtest → report
전 레이어 엔드투엔드 연결 검증 (네트워크 없이 bars_override 사용)
"""
import math
import tempfile
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

# ── 전 레이어 임포트 ─────────────────────────────────────────
from config import (
    CAPITAL, TOP_N, TAKE_PROFIT, STOP_LOSS_PCT,
    SPREAD_BPS, COMMISSION_MIN, COMMISSION_PER_SHARE,
    get_spread_tier,
)
from nasdaq_quant.data.manager import DataManager
from nasdaq_quant.features.builder import FeatureBuilder
from nasdaq_quant.signals.orb import ORBSignal, Position
from nasdaq_quant.risk.manager import RiskManager, DailyRiskState
from nasdaq_quant.execution.simulator import ExecutionSimulator
from nasdaq_quant.backtest import Backtester, Trade, get_trading_days
from nasdaq_quant.report import Reporter

ET = ZoneInfo("America/New_York")


# ─────────────────────────────────────────
# 공통 헬퍼: 1분봉 DataFrame 생성
# ─────────────────────────────────────────
def make_bars(events: list[tuple], day=date(2026, 3, 31), vol_base=500_000) -> pd.DataFrame:
    rows = []
    base = 100.0
    for ev in events:
        t_str = ev[0]; hi_m = ev[1]; lo_m = ev[2]
        cl_m  = ev[3] if len(ev) > 3 else None
        ts = pd.Timestamp(f"{day} {t_str}", tz=ET)
        hi = base * hi_m; lo = base * lo_m
        cl = base * cl_m if cl_m else (hi + lo) / 2
        rows.append({"ts": ts, "Open": base, "High": hi, "Low": lo, "Close": cl, "Volume": vol_base})
    df = pd.DataFrame(rows).set_index("ts")
    df.index.name = None
    return df


def make_tp_bars(day=date(2026, 3, 31)):
    events = [
        ("09:30", 1.010, 0.998, 1.002),
        ("09:31", 1.008, 0.997, 1.001),
        ("09:32", 1.009, 0.998, 1.003),
        ("09:33", 1.010, 0.997, 1.003),
        ("09:34", 1.010, 0.995, 1.003),  # or_high=101.0
        ("09:35", 1.025, 1.010, 1.020),  # 진입: close=102.0>101.0
        ("09:36", 1.035, 1.020, 1.030),
        ("09:37", 1.055, 1.040, 1.050),
        ("09:38", 1.075, 1.065, 1.072),
        ("09:39", 1.090, 1.075, 1.085),  # high=109.0 >= TP(101.0×1.07≈108.07)
    ]
    df = make_bars(events, day=day)
    df.iloc[5, df.columns.get_loc("Volume")] = 1_000_000  # 진입 바 거래량 급증
    return df


def make_sl_bars(day=date(2026, 3, 31)):
    events = [
        ("09:30", 1.010, 0.998, 1.002),
        ("09:31", 1.008, 0.997, 1.001),
        ("09:32", 1.009, 0.998, 1.003),
        ("09:33", 1.010, 0.997, 1.003),
        ("09:34", 1.010, 0.995, 1.003),
        ("09:35", 1.025, 1.010, 1.020),
        ("09:36", 1.010, 1.000, 1.005),
        ("09:37", 1.005, 0.980, 0.990),
        ("09:38", 0.990, 0.960, 0.975),
        ("09:39", 0.985, 0.960, 0.970),
    ]
    df = make_bars(events, day=day)
    df.iloc[5, df.columns.get_loc("Volume")] = 1_000_000
    return df


def make_no_entry_bars(day=date(2026, 3, 31)):
    events = [
        ("09:30", 1.010, 0.998),
        ("09:31", 1.008, 0.997),
        ("09:32", 1.009, 0.998),
        ("09:33", 1.010, 0.997),
        ("09:34", 1.010, 0.995),
        ("09:35", 1.008, 0.998),
        ("09:36", 1.005, 0.995),
        ("15:30", 1.002, 0.990),
    ]
    return make_bars(events, day=day)


# ─────────────────────────────────────────
# T-I1: config 레이어 — 파라미터 임포트 검증
# ─────────────────────────────────────────
assert CAPITAL    == 100_000
assert TOP_N      == 5
assert TAKE_PROFIT == 0.07
assert STOP_LOSS_PCT == 0.035
assert COMMISSION_MIN == 1.00
assert get_spread_tier("NVDA") == "large"
assert get_spread_tier("SOXL") == "etf"
assert get_spread_tier("SOUN") == "mid"
print("[OK] T-I1 config: 파라미터 정상 임포트")


# ─────────────────────────────────────────
# T-I2: features 레이어 — OR 피처 계산
# ─────────────────────────────────────────
fb = FeatureBuilder()
bars_tp = make_tp_bars()
or_feat = fb.build_or_features(bars_tp)
assert or_feat["valid"]
assert abs(or_feat["or_high"] - 101.0) < 0.01
assert or_feat["or_low"]  <= 99.8
assert or_feat["or_range"] > 0
print(f"[OK] T-I2 features: or_high={or_feat['or_high']:.2f}  or_low={or_feat['or_low']:.2f}  range={or_feat['or_range']:.2f}")


# ─────────────────────────────────────────
# T-I3: execution 레이어 — 슬리피지 방향 검증
# ─────────────────────────────────────────
sim = ExecutionSimulator()

# SOXL(ETF) 매수: fill > ref
buy_fill = sim.fill_price("SOXL", "buy",  101.0, 100, avg_daily_volume=500_000)
sell_fill = sim.fill_price("SOXL", "sell", 108.0, 100, avg_daily_volume=500_000)
assert buy_fill.fill_price  > 101.0, "ETF 매수: fill > ref"
assert sell_fill.fill_price < 108.0, "ETF 매도: fill < ref"

# NVDA(large) 슬리피지 < SOUN(mid) 슬리피지
nvda_buy  = sim.fill_price("NVDA", "buy", 100.0, 100, avg_daily_volume=10_000_000)
soun_buy  = sim.fill_price("SOUN", "buy", 100.0, 100, avg_daily_volume=500_000)
nvda_slip = nvda_buy.fill_price - 100.0
soun_slip = soun_buy.fill_price - 100.0
assert nvda_slip < soun_slip, f"large slip({nvda_slip:.4f}) should be < mid slip({soun_slip:.4f})"

# 수수료: max($1, shares × $0.005)
comm_small = sim.commission(10, 100.0)   # 10×0.005=0.05 → $1.00
comm_large = sim.commission(500, 100.0)  # 500×0.005=$2.50
assert comm_small == COMMISSION_MIN
assert abs(comm_large - 500 * COMMISSION_PER_SHARE) < 0.001
print(f"[OK] T-I3 execution: ETF slip={buy_fill.fill_price-101.0:.4f}  NVDA<SOUN slip  comm OK")


# ─────────────────────────────────────────
# T-I4: signals 레이어 — 진입/청산 로직
# ─────────────────────────────────────────
orb = ORBSignal()

# 진입 조건: close > or_high AND volume 충분
entry_bar = bars_tp.iloc[5]  # 09:35
ts_entry  = bars_tp.index[5]
assert orb.check_entry(or_feat, entry_bar, ts_entry) is True, "진입 바에서 check_entry True 기대"

# 돌파 없는 바 → False
no_entry_bar = bars_tp.iloc[0]
ts_no        = bars_tp.index[0]
assert orb.check_entry(or_feat, no_entry_bar, ts_no) is False

# TP 청산
entry_ref  = orb.entry_ref_price(or_feat)
buy_result = sim.fill_price("SOXL", "buy", entry_ref, 100, 500_000)
pos = Position(
    ticker="SOXL", entry_price=buy_result.fill_price,
    entry_ref=entry_ref, entry_time=ts_entry,
    or_high=or_feat["or_high"], or_low=or_feat["or_low"],
    shares=100,
)
# 09:39 바 — high=109.0 >= pos.tp_price
tp_bar = bars_tp.iloc[9]
reason = orb.check_exit(pos, tp_bar, bars_tp.index[9])
assert reason == "TP", f"check_exit 기대 TP, 실제 {reason}"
print(f"[OK] T-I4 signals: entry OK / TP exit OK (tp_price={pos.tp_price:.2f})")


# ─────────────────────────────────────────
# T-I5: risk 레이어 — 포지션 사이징 & 검증
# ─────────────────────────────────────────
rm = RiskManager(capital=CAPITAL, max_positions=TOP_N)

# 동일비중: 100k/5 = 20k, 20k/101=197주
shares_calc = rm.position_size(price=101.0, n_positions=TOP_N)
assert shares_calc == int(CAPITAL / TOP_N / 101.0)

state = rm.new_daily_state()
ok, reason = rm.validate_entry("SOXL", 101.0, shares_calc, state)
assert ok, f"validate_entry 실패: {reason}"

# 주가 미달 거부
ok_low, r_low = rm.validate_entry("PENNY", 3.0, 100, state)
assert not ok_low and "주가" in r_low

# bear 레짐 거부
ok_bear, r_bear = rm.validate_entry("SOXL", 101.0, 100, state,
                                     regime_features={"qqq_regime": "bear", "qqq_chg_pct": -0.015})
assert not ok_bear and "bear" in r_bear
print(f"[OK] T-I5 risk: position_size={shares_calc}주  validate OK  penny/bear 거부 OK")


# ─────────────────────────────────────────
# T-I6: 단일 레이어 체인 — OR→Entry→Exit 수동
# ─────────────────────────────────────────
# features → signals → execution 직접 연결
or_f    = fb.build_or_features(bars_tp)
assert or_f["valid"]

watch = bars_tp.between_time("09:35", "23:59")
entered = False
for ts, row in watch.iterrows():
    if orb.check_entry(or_f, row, ts):
        e_ref  = orb.entry_ref_price(or_f)
        sh     = rm.position_size(e_ref, TOP_N)
        b_fill = sim.fill_price("SOXL", "buy", e_ref, sh, 500_000)
        pos2   = Position(ticker="SOXL", entry_price=b_fill.fill_price,
                          entry_ref=e_ref, entry_time=ts,
                          or_high=or_f["or_high"], or_low=or_f["or_low"], shares=sh)
        entered = True
        continue
    if entered:
        exit_r = orb.check_exit(pos2, row, ts)
        if exit_r:
            ex_ref   = orb.exit_ref_price(exit_r, pos2, row)
            s_fill   = sim.fill_price("SOXL", "sell", ex_ref, sh, 500_000)
            comm     = b_fill.commission + s_fill.commission
            pnl_net  = (s_fill.fill_price - b_fill.fill_price) * sh - comm
            pnl_gros = (ex_ref - e_ref) * sh
            assert exit_r == "TP"
            assert pnl_gros > 0
            assert pnl_net  < pnl_gros  # 비용 drag
            print(f"[OK] T-I6 chain: {exit_r} gross=${pnl_gros:.2f} net=${pnl_net:.2f}")
            break
else:
    assert False, "T-I6: 체인 진입 안됨"


# ─────────────────────────────────────────
# T-I7: 백테스트 — 다일 다종목 엔드투엔드
# ─────────────────────────────────────────
D1 = date(2026, 3, 31)
D2 = date(2026, 4, 1)
D3 = date(2026, 4, 2)

bt = Backtester(capital=CAPITAL, top_n=3)

bars_ov = {
    "SOXL": {D1: make_tp_bars(D1), D2: make_sl_bars(D2), D3: make_tp_bars(D3)},
    "RKLB": {D1: make_sl_bars(D1), D2: make_tp_bars(D2), D3: make_no_entry_bars(D3)},
    "SOUN": {D1: make_no_entry_bars(D1), D2: make_no_entry_bars(D2), D3: make_sl_bars(D3)},
}
univ_ov = {
    D1: ["SOXL", "RKLB", "SOUN"],
    D2: ["SOXL", "RKLB", "SOUN"],
    D3: ["SOXL", "RKLB", "SOUN"],
}

trades = bt.run(start=D1, end=D3, bars_override=bars_ov, universe_override=univ_ov)

# 3일 × 3종목 = 최대 9개 Trade
assert len(trades) == 9, f"trade count={len(trades)}"

entered = [t for t in trades if t.entered]
assert len(entered) >= 4, f"entered count={len(entered)}"  # SOUN D1/D2는 NO_ENTRY

# 날짜별 분포 확인
by_date = {}
for t in trades:
    by_date.setdefault(t.date, []).append(t)
assert len(by_date) == 3, f"날짜 수={len(by_date)}"

print(f"[OK] T-I7 backtest E2E: {len(trades)}건 trade  진입={len(entered)}건")
for d, ts_ in sorted(by_date.items()):
    ent = [t for t in ts_ if t.entered]
    net = sum(t.pnl_net for t in ent)
    print(f"     {d}: 진입={len(ent)}건  net=${net:.2f}")


# ─────────────────────────────────────────
# T-I8: report 레이어 — 전체 지표 정합성
# ─────────────────────────────────────────
rp = Reporter(capital=CAPITAL)
s = rp.summary(trades)

assert s["total"]   == 9
assert s["entered"] == len(entered)
assert 0 <= s["win_rate"] <= 1
assert math.isfinite(s["profit_factor"]) or s["profit_factor"] == float("inf")
# net < gross (비용 존재)
if s["entered"] > 0:
    assert s["total_net"] < s["total_gross"], "net < gross (슬리피지+수수료)"
    assert s["total_slip"] > 0
    assert s["total_commission"] >= COMMISSION_MIN * s["entered"]

print(f"[OK] T-I8 report summary: WR={s['win_rate']:.1%}  PF={s['profit_factor']:.2f}"
      f"  Sharpe={s['sharpe']:.2f}  MDD=${s['mdd_net']:.2f}")


# ─────────────────────────────────────────
# T-I9: walk-forward + report 통합
# ─────────────────────────────────────────
periods = [(D1, D1), (D2, D3)]
wf = bt.walk_forward(periods, bars_override=bars_ov, universe_override=univ_ov)
assert len(wf) == 2

for key, wf_trades in wf.items():
    ent = [t for t in wf_trades if t.entered]
    wf_s = rp.summary(wf_trades)
    print(f"[OK] T-I9 walk-forward [{key}]: entered={len(ent)}  net=${wf_s['total_net']:.2f}")


# ─────────────────────────────────────────
# T-I10: CSV + HTML 출력 검증
# ─────────────────────────────────────────
with tempfile.TemporaryDirectory() as tmpdir:
    base = Path(tmpdir) / "e2e_report"
    csv_path  = rp.to_csv(trades, base.with_suffix(".csv"))
    html_path = rp.to_html(trades, base.with_suffix(".html"))
    assert csv_path.exists()
    assert html_path.exists()
    # CSV 행 수 = 거래 수 + 요약 행
    import csv
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    assert len(rows) == len(trades) + 1
    assert rows[-1]["ticker"] == "SUMMARY"
    print(f"[OK] T-I10 CSV={len(rows)-1}행  HTML={html_path.stat().st_size//1024}KB")


# ─────────────────────────────────────────
# T-I11: main.py CLI — --help 정상 동작
# ─────────────────────────────────────────
import subprocess, sys
result = subprocess.run(
    [sys.executable, "main.py", "--help"],
    capture_output=True, text=True
)
assert result.returncode == 0, f"main.py --help failed: {result.stderr}"
assert "backtest" in result.stdout
assert "live"     in result.stdout
print(f"[OK] T-I11 main.py --help: {result.stdout.splitlines()[0]}")

result_bt = subprocess.run(
    [sys.executable, "main.py", "backtest", "--help"],
    capture_output=True, text=True
)
assert result_bt.returncode == 0
assert "--start" in result_bt.stdout and "--top-n" in result_bt.stdout
print(f"[OK] T-I11 main.py backtest --help: --start/--top-n 옵션 확인")


# ─────────────────────────────────────────
# T-I12: 슬리피지 임팩트 분석
# ─────────────────────────────────────────
ent_trades = [t for t in trades if t.entered]
cost_drag_pct = (s["total_slip"] + s["total_commission"]) / abs(s["total_gross"]) * 100 if s["total_gross"] != 0 else 0
by_ticker_cost = {}
for t in ent_trades:
    by_ticker_cost.setdefault(t.ticker, 0.0)
    by_ticker_cost[t.ticker] += t.slip_cost + t.commission

print()
print("  ── 슬리피지 임팩트 분석 ──────────────────────────")
print(f"  Total Gross : ${s['total_gross']:>10,.2f}")
print(f"  Total Net   : ${s['total_net']:>10,.2f}")
print(f"  Slip + Comm : ${s['total_slip']+s['total_commission']:>10,.2f}  ({cost_drag_pct:.1f}% of |gross|)")
print("  종목별 비용:")
for tkr, cost in sorted(by_ticker_cost.items(), key=lambda x: -x[1]):
    print(f"    {tkr:<6}  ${cost:.2f}")

# 비용이 gross의 합리적 범위 (0~50%) 내인지 확인
assert 0 <= cost_drag_pct <= 50, f"cost_drag_pct={cost_drag_pct:.1f}% out of range"
print(f"[OK] T-I12 slip impact {cost_drag_pct:.1f}% - ok range")


print()
print("Phase 7 모든 통합 테스트 통과")
