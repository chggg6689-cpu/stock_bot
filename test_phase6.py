"""
Phase 6 report 레이어 테스트
네트워크 없이 Trade 픽스처로 Reporter 전 기능 검증
"""
import math
import tempfile
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

from nasdaq_quant.backtest.engine import Trade
from nasdaq_quant.report import Reporter
from config import COMMISSION_MIN

ET = ZoneInfo("America/New_York")
CAPITAL = 100_000

rp = Reporter(capital=CAPITAL)


# ─────────────────────────────────────────
# 픽스처 — Trade 목록 직접 생성
# ─────────────────────────────────────────
def make_trade(
    ticker="SOXL",
    result="TP",
    pnl_gross=700.0,
    pnl_net=670.0,
    slip_cost=20.0,
    commission=10.0,
    shares=100,
    entry_ref=100.0,
    entry_fill=100.1,
    exit_ref=107.0,
    exit_fill=106.9,
    or_high=101.0,
    or_low=99.5,
    day=date(2026, 3, 31),
) -> Trade:
    return Trade(
        ticker=ticker, date=day, result=result,
        entry_ref=entry_ref, entry_fill=entry_fill,
        exit_ref=exit_ref, exit_fill=exit_fill,
        shares=shares, commission=commission, slip_cost=slip_cost,
        pnl_gross=pnl_gross, pnl_net=pnl_net,
        or_high=or_high, or_low=or_low, or_range=or_high - or_low,
        entry_time=datetime(2026, 3, 31, 9, 35, tzinfo=ET),
        exit_time=datetime(2026, 3, 31, 10, 15, tzinfo=ET),
    )


# 5 거래: TP×3, SL×1, NO_ENTRY×1
trades = [
    make_trade("SOXL", "TP",       pnl_gross=700,  pnl_net=670,  slip_cost=20, commission=10, day=date(2026, 3, 31)),
    make_trade("RKLB", "TP",       pnl_gross=500,  pnl_net=478,  slip_cost=12, commission=10, day=date(2026, 3, 31)),
    make_trade("SOUN", "SL",       pnl_gross=-350, pnl_net=-382, slip_cost=18, commission=14, day=date(2026, 4, 1)),
    make_trade("SOXL", "TP",       pnl_gross=420,  pnl_net=404,  slip_cost=8,  commission=8,  day=date(2026, 4, 2)),
    make_trade("NVDA", "NO_ENTRY", pnl_gross=0,    pnl_net=0,    slip_cost=0,  commission=0,  shares=0,
               entry_ref=0, entry_fill=0, exit_ref=0, exit_fill=0, day=date(2026, 4, 2)),
]
# NO_ENTRY는 entered=False가 되어야 함 (result="NO_ENTRY")


# ─────────────────────────────────────────
# T-R1: summary 기본 지표
# ─────────────────────────────────────────
s = rp.summary(trades)

assert s["total"] == 5,    f"total={s['total']}"
assert s["entered"] == 4,  f"entered={s['entered']}"
assert s["no_entry"] == 1, f"no_entry={s['no_entry']}"
assert s["tp_count"] == 3, f"tp_count={s['tp_count']}"
assert s["sl_count"] == 1, f"sl_count={s['sl_count']}"
assert s["eod_count"] == 0

print(f"[OK] T-R1 summary counts: total={s['total']} entered={s['entered']} TP={s['tp_count']} SL={s['sl_count']}")


# ─────────────────────────────────────────
# T-R2: 승률
# ─────────────────────────────────────────
# 진입 4건 중 net>0인 건: TP×3(670,478,404 모두>0), SL×1(-382<0) → 3/4 = 75%
assert abs(s["win_rate"] - 0.75) < 1e-9, f"win_rate={s['win_rate']}"
print(f"[OK] T-R2 win_rate={s['win_rate']:.1%}")


# ─────────────────────────────────────────
# T-R3: gross / net PnL 합계
# ─────────────────────────────────────────
expected_gross = 700 + 500 + (-350) + 420   # = 1270
expected_net   = 670 + 478 + (-382) + 404   # = 1170
assert abs(s["total_gross"] - expected_gross) < 0.01, f"gross={s['total_gross']}"
assert abs(s["total_net"]   - expected_net)   < 0.01, f"net={s['total_net']}"
assert s["total_net"] < s["total_gross"], "net < gross (비용 반영)"
print(f"[OK] T-R3 gross=${s['total_gross']:.2f}  net=${s['total_net']:.2f}")


# ─────────────────────────────────────────
# T-R4: 슬리피지 / 수수료 합계
# ─────────────────────────────────────────
expected_slip = 20 + 12 + 18 + 8   # = 58
expected_comm = 10 + 10 + 14 + 8   # = 42
assert abs(s["total_slip"]       - expected_slip) < 0.01
assert abs(s["total_commission"] - expected_comm) < 0.01
print(f"[OK] T-R4 slip=${s['total_slip']:.2f}  commission=${s['total_commission']:.2f}")


# ─────────────────────────────────────────
# T-R5: Profit Factor
# ─────────────────────────────────────────
wins_net = 670 + 478 + 404     # 1552
loss_net = abs(-382)           # 382
expected_pf = wins_net / loss_net
assert abs(s["profit_factor"] - expected_pf) < 0.001, f"pf={s['profit_factor']:.4f} expected={expected_pf:.4f}"
print(f"[OK] T-R5 profit_factor={s['profit_factor']:.3f}")


# ─────────────────────────────────────────
# T-R6: Sharpe — 거래일 3일 기준
# ─────────────────────────────────────────
# daily net: 3/31=1148, 4/1=-382, 4/2=404
# returns: 1148/100k, -382/100k, 404/100k
daily_rets = [(670+478)/CAPITAL, -382/CAPITAL, 404/CAPITAL]
mean_r = sum(daily_rets) / 3
std_r  = math.sqrt(sum((r - mean_r)**2 for r in daily_rets) / 2)
expected_sharpe = mean_r / std_r * math.sqrt(252)
assert math.isfinite(s["sharpe"]), "Sharpe should be finite"
assert abs(s["sharpe"] - expected_sharpe) < 0.001, f"sharpe={s['sharpe']:.4f} expected={expected_sharpe:.4f}"
print(f"[OK] T-R6 Sharpe={s['sharpe']:.4f}")


# ─────────────────────────────────────────
# T-R7: MDD (net)
# ─────────────────────────────────────────
# equity: 100000 → 101148 → 100766 → 101170
# peak 101148, drawdown 101148-100766=382
assert s["mdd_net"] >= 0
assert s["mdd_net"] == 382.0, f"mdd_net={s['mdd_net']}"
print(f"[OK] T-R7 mdd_net=${s['mdd_net']:.2f}")


# ─────────────────────────────────────────
# T-R8: 종목별 승률
# ─────────────────────────────────────────
bt = s["by_ticker"]
assert "SOXL" in bt
assert bt["SOXL"]["trades"] == 2
assert bt["SOXL"]["wins"]   == 2
assert bt["SOXL"]["win_rate"] == 1.0

assert "SOUN" in bt
assert bt["SOUN"]["trades"] == 1
assert bt["SOUN"]["wins"]   == 0
assert bt["SOUN"]["win_rate"] == 0.0

# NVDA는 NO_ENTRY → by_ticker에 없어야 함
assert "NVDA" not in bt, f"NVDA should not be in by_ticker (NO_ENTRY)"
print(f"[OK] T-R8 by_ticker: SOXL 2/2=100%, SOUN 0/1=0%, NVDA 제외")


# ─────────────────────────────────────────
# T-R9: print_console 정상 출력 (예외 없음)
# ─────────────────────────────────────────
print()
rp.print_console(trades)
print()
print("[OK] T-R9 print_console 정상 출력")


# ─────────────────────────────────────────
# T-R10: to_csv 저장 검증
# ─────────────────────────────────────────
with tempfile.TemporaryDirectory() as tmpdir:
    csv_path = Path(tmpdir) / "trades.csv"
    rp.to_csv(trades, csv_path)
    assert csv_path.exists()
    import csv
    rows = list(csv.DictReader(open(csv_path, encoding="utf-8")))
    # 마지막 행은 SUMMARY
    assert rows[-1]["ticker"] == "SUMMARY"
    # 거래 행 수 = len(trades)
    assert len(rows) == len(trades) + 1, f"rows={len(rows)}"
    # net PnL 합계 검증
    data_rows = rows[:-1]
    csv_net_sum = sum(float(r["pnl_net"]) for r in data_rows)
    assert abs(csv_net_sum - expected_net) < 0.01
    print(f"[OK] T-R10 to_csv: {len(data_rows)}행 저장, SUMMARY 행 포함, net 합계=${csv_net_sum:.2f}")


# ─────────────────────────────────────────
# T-R11: to_html 저장 검증 (plotly 유무 무관)
# ─────────────────────────────────────────
with tempfile.TemporaryDirectory() as tmpdir:
    html_path = Path(tmpdir) / "report.html"
    rp.to_html(trades, html_path)
    assert html_path.exists()
    content = html_path.read_text(encoding="utf-8")
    assert len(content) > 100, "HTML 내용이 너무 짧음"
    print(f"[OK] T-R11 to_html: {len(content)} bytes 생성")


# ─────────────────────────────────────────
# T-R12: 빈 거래 목록 — 예외 없이 처리
# ─────────────────────────────────────────
s_empty = rp.summary([])
assert s_empty["total"] == 0
assert s_empty["entered"] == 0
assert s_empty["win_rate"] == 0.0
assert s_empty["total_gross"] == 0.0
assert not math.isfinite(s_empty["sharpe"]) or s_empty["sharpe"] == 0.0
print(f"[OK] T-R12 empty trades: 예외 없이 처리")


print()
print("Phase 6 모든 테스트 통과")
