"""
report/reporter.py
성과 지표 계산 및 출력 — gross/net 분리, Sharpe, MDD, Profit Factor
"""
from __future__ import annotations

import math
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from nasdaq_quant.backtest.engine import Trade


# ────────────────────────────────────────────────────────────
# 내부 헬퍼
# ────────────────────────────────────────────────────────────
def _entered(trades: list["Trade"]) -> list["Trade"]:
    return [t for t in trades if t.entered]


def _daily_pnl(trades: list["Trade"], use_gross: bool = False) -> dict[date, float]:
    """날짜별 PnL 합계 (entered 거래만)"""
    daily: dict[date, float] = defaultdict(float)
    for t in _entered(trades):
        daily[t.date] += t.pnl_gross if use_gross else t.pnl_net
    return daily


def _equity_curve(daily: dict[date, float], capital: float) -> list[float]:
    """누적 equity (capital 기준). 거래일 순 정렬."""
    curve = [capital]
    for d in sorted(daily):
        curve.append(curve[-1] + daily[d])
    return curve


def _mdd(equity: list[float]) -> float:
    """Maximum Drawdown (절대값 달러)"""
    peak = equity[0]
    mdd = 0.0
    for v in equity:
        if v > peak:
            peak = v
        dd = peak - v
        if dd > mdd:
            mdd = dd
    return mdd


def _sharpe(daily: dict[date, float], capital: float, ann_factor: float = 252.0) -> float:
    """
    일별 수익률 기반 Sharpe (risk-free = 0).
    거래 없는 날은 0% 수익으로 포함하지 않음 (conservative: 거래일만 사용).
    """
    rets = [v / capital for v in daily.values()]
    if len(rets) < 2:
        return float("nan")
    mean = sum(rets) / len(rets)
    std = math.sqrt(sum((r - mean) ** 2 for r in rets) / (len(rets) - 1))
    if std == 0:
        return float("nan")
    return mean / std * math.sqrt(ann_factor)


def _profit_factor(entered: list["Trade"]) -> float:
    """Profit Factor = gross wins / |gross losses| (net 기준)"""
    wins = sum(t.pnl_net for t in entered if t.pnl_net > 0)
    loss = sum(t.pnl_net for t in entered if t.pnl_net < 0)
    if loss == 0:
        return float("inf")
    return wins / abs(loss)


# ────────────────────────────────────────────────────────────
# Reporter
# ────────────────────────────────────────────────────────────
class Reporter:
    def __init__(self, capital: float = 100_000.0):
        self.capital = capital

    # ── 핵심 지표 ─────────────────────────────────────────
    def summary(self, trades: list["Trade"]) -> dict:
        """
        성과 요약 딕셔너리 반환.

        Keys:
            total, entered, no_entry,
            tp_count, sl_count, eod_count,
            win_rate,
            avg_pnl_gross, avg_pnl_net,
            total_gross, total_net,
            total_slip, total_commission,
            profit_factor,
            sharpe, mdd,
            by_ticker: {ticker: {"trades":int,"wins":int,"win_rate":float}}
        """
        ent = _entered(trades)
        n_total   = len(trades)
        n_entered = len(ent)
        n_no_entry = n_total - n_entered

        tp_count  = sum(1 for t in ent if t.result == "TP")
        sl_count  = sum(1 for t in ent if t.result == "SL")
        eod_count = sum(1 for t in ent if t.result == "EOD")

        winners = [t for t in ent if t.pnl_net > 0]
        win_rate = len(winners) / n_entered if n_entered else 0.0

        total_gross = sum(t.pnl_gross for t in ent)
        total_net   = sum(t.pnl_net   for t in ent)
        total_slip  = sum(t.slip_cost  for t in ent)
        total_comm  = sum(t.commission for t in ent)

        avg_gross = total_gross / n_entered if n_entered else 0.0
        avg_net   = total_net   / n_entered if n_entered else 0.0

        pf = _profit_factor(ent)

        daily_net   = _daily_pnl(trades, use_gross=False)
        daily_gross = _daily_pnl(trades, use_gross=True)
        sharpe = _sharpe(daily_net, self.capital)

        eq_net   = _equity_curve(daily_net,   self.capital)
        eq_gross = _equity_curve(daily_gross, self.capital)
        mdd_net   = _mdd(eq_net)
        mdd_gross = _mdd(eq_gross)

        # 종목별 승률
        by_ticker: dict[str, dict] = defaultdict(lambda: {"trades": 0, "wins": 0})
        for t in ent:
            by_ticker[t.ticker]["trades"] += 1
            if t.pnl_net > 0:
                by_ticker[t.ticker]["wins"] += 1
        for v in by_ticker.values():
            v["win_rate"] = v["wins"] / v["trades"] if v["trades"] else 0.0

        return {
            "total":           n_total,
            "entered":         n_entered,
            "no_entry":        n_no_entry,
            "tp_count":        tp_count,
            "sl_count":        sl_count,
            "eod_count":       eod_count,
            "win_rate":        win_rate,
            "avg_pnl_gross":   avg_gross,
            "avg_pnl_net":     avg_net,
            "total_gross":     total_gross,
            "total_net":       total_net,
            "total_slip":      total_slip,
            "total_commission":total_comm,
            "profit_factor":   pf,
            "sharpe":          sharpe,
            "mdd_net":         mdd_net,
            "mdd_gross":       mdd_gross,
            "by_ticker":       dict(by_ticker),
        }

    # ── 콘솔 출력 ─────────────────────────────────────────
    def print_console(self, trades: list["Trade"]) -> None:
        s = self.summary(trades)
        sep = "=" * 52

        print(sep)
        print("  BACKTEST PERFORMANCE REPORT")
        print(sep)
        print(f"  총 거래    : {s['total']}건 (진입 {s['entered']}, 미진입 {s['no_entry']})")
        print(f"  청산 유형  : TP={s['tp_count']}  SL={s['sl_count']}  EOD={s['eod_count']}")
        print(f"  승  률     : {s['win_rate']:.1%}")
        print(f"  Profit Factor: {s['profit_factor']:.2f}" if math.isfinite(s["profit_factor"])
              else f"  Profit Factor: inf (손실 거래 없음)")
        print()
        print("  ── PnL (진입 거래 합계) ──────────────────")
        print(f"  Gross PnL  : ${s['total_gross']:>10,.2f}  (avg ${s['avg_pnl_gross']:,.2f}/tr)")
        print(f"  Net PnL    : ${s['total_net']:>10,.2f}  (avg ${s['avg_pnl_net']:,.2f}/tr)")
        print(f"  슬리피지   : ${s['total_slip']:>10,.2f}")
        print(f"  수 수 료   : ${s['total_commission']:>10,.2f}")
        cost_drag = s["total_slip"] + s["total_commission"]
        if s["total_gross"] != 0:
            print(f"  비용 drag  : ${cost_drag:,.2f}  ({cost_drag/abs(s['total_gross'])*100:.1f}% of |gross|)")
        print()
        print("  ── 리스크 지표 ───────────────────────────")
        sharpe_str = f"{s['sharpe']:.3f}" if math.isfinite(s["sharpe"]) else "N/A"
        print(f"  Sharpe     : {sharpe_str}")
        print(f"  MDD (net)  : ${s['mdd_net']:,.2f}  ({s['mdd_net']/self.capital:.2%})")
        print(f"  MDD (gross): ${s['mdd_gross']:,.2f}  ({s['mdd_gross']/self.capital:.2%})")
        print()
        print("  ── 종목별 승률 ───────────────────────────")
        for tkr, v in sorted(s["by_ticker"].items(), key=lambda x: -x[1]["trades"]):
            print(f"  {tkr:<6}  {v['trades']}건  {v['wins']}승  ({v['win_rate']:.0%})")
        print(sep)

    # ── CSV 저장 ──────────────────────────────────────────
    def to_csv(self, trades: list["Trade"], path: str | Path) -> Path:
        """
        거래 상세 CSV 저장.
        마지막 행에 합계/평균 요약 추가.
        """
        path = Path(path)
        rows = []
        for t in trades:
            rows.append({
                "ticker":       t.ticker,
                "date":         str(t.date),
                "result":       t.result,
                "entry_time":   str(t.entry_time) if t.entry_time else "",
                "exit_time":    str(t.exit_time)  if t.exit_time  else "",
                "entry_ref":    t.entry_ref,
                "entry_fill":   t.entry_fill,
                "exit_ref":     t.exit_ref,
                "exit_fill":    t.exit_fill,
                "shares":       t.shares,
                "pnl_gross":    t.pnl_gross,
                "pnl_net":      t.pnl_net,
                "slip_cost":    t.slip_cost,
                "commission":   t.commission,
                "or_high":      t.or_high,
                "or_low":       t.or_low,
                "or_range":     t.or_range,
                "reason":       t.reason,
            })

        df = pd.DataFrame(rows)

        # 요약 행
        s = self.summary(trades)
        summary_row = {
            "ticker": "SUMMARY",
            "date": "",
            "result": f"WR={s['win_rate']:.1%} PF={s['profit_factor']:.2f}",
            "entry_time": "",
            "exit_time": "",
            "entry_ref": "",
            "entry_fill": "",
            "exit_ref": "",
            "exit_fill": "",
            "shares": s["entered"],
            "pnl_gross": s["total_gross"],
            "pnl_net":   s["total_net"],
            "slip_cost": s["total_slip"],
            "commission":s["total_commission"],
            "or_high": "",
            "or_low": "",
            "or_range": "",
            "reason": f"Sharpe={s['sharpe']:.3f} MDD=${s['mdd_net']:.0f}" if math.isfinite(s["sharpe"]) else "",
        }
        df = pd.concat([df, pd.DataFrame([summary_row])], ignore_index=True)

        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(path, index=False)
        return path

    # ── HTML 리포트 ───────────────────────────────────────
    def to_html(self, trades: list["Trade"], path: str | Path) -> Path:
        """
        누적 수익 곡선 HTML (plotly). plotly 없으면 텍스트 요약만 저장.
        """
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)

        s = self.summary(trades)
        daily_net   = _daily_pnl(trades, use_gross=False)
        daily_gross = _daily_pnl(trades, use_gross=True)

        dates  = sorted(set(daily_net) | set(daily_gross))
        gross_curve, net_curve = [], []
        g_cum = n_cum = 0.0
        for d in dates:
            g_cum += daily_gross.get(d, 0.0)
            n_cum += daily_net.get(d, 0.0)
            gross_curve.append(g_cum)
            net_curve.append(n_cum)

        try:
            import plotly.graph_objects as go
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=[str(d) for d in dates], y=gross_curve,
                name="Gross PnL", line=dict(color="royalblue"),
            ))
            fig.add_trace(go.Scatter(
                x=[str(d) for d in dates], y=net_curve,
                name="Net PnL", line=dict(color="orangered"),
            ))
            fig.update_layout(
                title=(
                    f"ORB Backtest | WR={s['win_rate']:.1%}  PF={s['profit_factor']:.2f}"
                    f"  Sharpe={s['sharpe']:.2f}  MDD=${s['mdd_net']:.0f}"
                    if math.isfinite(s["sharpe"]) else
                    f"ORB Backtest | WR={s['win_rate']:.1%}  PF={s['profit_factor']:.2f}"
                ),
                xaxis_title="Date",
                yaxis_title="Cumulative PnL ($)",
                hovermode="x unified",
            )
            fig.write_html(str(path))
        except ImportError:
            # plotly 없으면 텍스트 요약
            with open(path, "w", encoding="utf-8") as f:
                f.write("<pre>\n")
                f.write(f"Win Rate     : {s['win_rate']:.1%}\n")
                f.write(f"Total Gross  : ${s['total_gross']:,.2f}\n")
                f.write(f"Total Net    : ${s['total_net']:,.2f}\n")
                f.write(f"Profit Factor: {s['profit_factor']:.2f}\n")
                f.write(f"Sharpe       : {s['sharpe']:.3f}\n" if math.isfinite(s["sharpe"]) else "Sharpe: N/A\n")
                f.write(f"MDD (net)    : ${s['mdd_net']:.2f}\n")
                f.write("</pre>\n")

        return path
