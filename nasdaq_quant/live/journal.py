"""
live/journal.py
PaperJournal — paper trading 거래 일지

기록 항목:
  signal_time, order_time, fill_time, fill_price, gross/net PnL,
  slippage, partial fill, exit reason, anomaly flags
"""
import csv
import logging
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 데이터 클래스
# ─────────────────────────────────────────
@dataclass
class TradeRecord:
    date:             date
    ticker:           str

    # 진입
    signal_time:      datetime | None = None   # check_entry 시그널 감지 시각
    order_time:       datetime | None = None   # broker.buy() 호출 시각
    fill_time:        datetime | None = None   # get_fill_info() 반환 시각
    shares_requested: int   = 0
    shares_filled:    int   = 0
    entry_ref:        float = 0.0              # OR high × buffer
    entry_fill:       float = 0.0              # 실제 체결가
    entry_slip:       float = 0.0              # fill - ref ($)
    entry_slip_pct:   float = 0.0              # slip / ref (%)
    tp_price:         float = 0.0
    sl_price:         float = 0.0

    # 청산
    exit_signal_time: datetime | None = None
    exit_order_time:  datetime | None = None
    exit_fill_time:   datetime | None = None
    exit_ref:         float = 0.0
    exit_fill:        float = 0.0
    exit_slip:        float = 0.0
    exit_slip_pct:    float = 0.0
    exit_reason:      str   = ""               # TP / SL / EOD / INTERRUPT / ERROR

    # 손익 (Alpaca paper: commission-free → net = gross)
    pnl_gross:        float = 0.0
    pnl_net:          float = 0.0

    # 상태
    partial_fill:     bool  = False
    status:           str   = "OPEN"           # OPEN / CLOSED / ORDER_FAILED


@dataclass
class AnomalyFlag:
    time:   datetime
    code:   str      # DUPLICATE_ORDER / UNCLOSED_POSITION / EOD_FAIL / BROKER_ERROR / POLL_FAIL
    ticker: str
    detail: str


# ─────────────────────────────────────────
# PaperJournal
# ─────────────────────────────────────────
class PaperJournal:
    """
    paper trading 거래 일지.

    사용법:
        journal = PaperJournal(log_dir=Path("logs"))
        # 진입 시
        journal.open_trade(date=..., ticker=..., ...)
        # 청산 시
        journal.close_trade(ticker=..., ...)
        # 이상 감지 시
        journal.flag_anomaly("EOD_FAIL", ticker, detail)
        # 장 마감 후
        journal.save_daily(day)
        # 기간 종료 후
        journal.print_period_report()
    """

    def __init__(self, log_dir: Path = Path("logs")):
        self.log_dir = Path(log_dir)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        self.trades:    list[TradeRecord] = []
        self.anomalies: list[AnomalyFlag] = []
        self._open:     dict[str, TradeRecord] = {}  # ticker → 진행중 레코드

    # ── 진입 기록 ──────────────────────────────────────────
    def open_trade(
        self,
        *,
        date:             date,
        ticker:           str,
        signal_time:      datetime,
        order_time:       datetime,
        fill_time:        datetime,
        shares_requested: int,
        shares_filled:    int,
        entry_ref:        float,
        entry_fill:       float,
        tp_price:         float,
        sl_price:         float,
    ) -> None:
        if ticker in self._open:
            self.flag_anomaly(
                "DUPLICATE_ORDER", ticker,
                f"open_trade called while {ticker} already has an open record",
            )
            return

        slip     = entry_fill - entry_ref
        slip_pct = slip / entry_ref if entry_ref else 0.0

        rec = TradeRecord(
            date             = date,
            ticker           = ticker,
            signal_time      = signal_time,
            order_time       = order_time,
            fill_time        = fill_time,
            shares_requested = shares_requested,
            shares_filled    = shares_filled,
            entry_ref        = entry_ref,
            entry_fill       = entry_fill,
            entry_slip       = slip,
            entry_slip_pct   = slip_pct,
            tp_price         = tp_price,
            sl_price         = sl_price,
            partial_fill     = shares_filled < shares_requested,
            status           = "OPEN",
        )
        self._open[ticker] = rec
        self.trades.append(rec)
        log.info(
            "[JOURNAL] ENTRY  %-6s  fill=%.2f  slip=%+.3f(%+.3f%%)",
            ticker, entry_fill, slip, slip_pct * 100,
        )

    # ── 청산 기록 ──────────────────────────────────────────
    def close_trade(
        self,
        *,
        ticker:           str,
        exit_signal_time: datetime,
        exit_order_time:  datetime,
        exit_fill_time:   datetime,
        exit_ref:         float,
        exit_fill:        float,
        exit_reason:      str,
    ) -> None:
        rec = self._open.pop(ticker, None)
        if rec is None:
            self.flag_anomaly(
                "UNCLOSED_POSITION", ticker,
                "close_trade called but no open record found",
            )
            return

        slip     = exit_fill - exit_ref
        slip_pct = slip / exit_ref if exit_ref else 0.0
        pnl      = (exit_fill - rec.entry_fill) * rec.shares_filled

        rec.exit_signal_time = exit_signal_time
        rec.exit_order_time  = exit_order_time
        rec.exit_fill_time   = exit_fill_time
        rec.exit_ref         = exit_ref
        rec.exit_fill        = exit_fill
        rec.exit_slip        = slip
        rec.exit_slip_pct    = slip_pct
        rec.exit_reason      = exit_reason
        rec.pnl_gross        = pnl
        rec.pnl_net          = pnl   # Alpaca paper: commission-free
        rec.status           = "CLOSED"
        log.info(
            "[JOURNAL] EXIT   %-6s  reason=%-3s  fill=%.2f  pnl=%+.2f",
            ticker, exit_reason, exit_fill, pnl,
        )

    # ── 이상 감지 ──────────────────────────────────────────
    def flag_anomaly(self, code: str, ticker: str, detail: str) -> None:
        flag = AnomalyFlag(
            time=datetime.now(ET), code=code, ticker=ticker, detail=detail
        )
        self.anomalies.append(flag)
        log.warning("[ANOMALY] %-20s  %-6s  %s", code, ticker, detail)

    # ── 일별 CSV 저장 ─────────────────────────────────────
    def save_daily(self, day: date) -> Path:
        day_trades  = [t for t in self.trades   if t.date == day]
        day_anomaly = [f for f in self.anomalies if f.time.date() == day]

        trade_path = self.log_dir / f"{day.isoformat()}_trades.csv"
        _write_trades_csv(trade_path, day_trades)

        if day_anomaly:
            anom_path = self.log_dir / f"{day.isoformat()}_anomalies.csv"
            _write_anomalies_csv(anom_path, day_anomaly)
            log.warning("[JOURNAL] 이상 %d건 → %s", len(day_anomaly), anom_path)

        closed = [t for t in day_trades if t.status == "CLOSED"]
        log.info(
            "[JOURNAL] 일별 저장 %s  거래=%d  이상=%d  → %s",
            day, len(closed), len(day_anomaly), trade_path,
        )
        return trade_path

    # ── 기간 요약 ─────────────────────────────────────────
    def period_summary(self) -> dict:
        closed = [t for t in self.trades if t.status == "CLOSED"]
        if not closed:
            return {"error": "거래 없음"}

        wins       = [t for t in closed if t.pnl_net > 0]
        losses     = [t for t in closed if t.pnl_net <= 0]
        total_win  = sum(t.pnl_net for t in wins)
        total_loss = abs(sum(t.pnl_net for t in losses))
        pf         = total_win / total_loss if total_loss else float("inf")

        slips = [t.entry_slip_pct for t in closed] + [t.exit_slip_pct for t in closed]
        avg_slip = sum(slips) / len(slips) if slips else 0.0

        by_reason: dict[str, int] = {}
        for t in closed:
            by_reason[t.exit_reason] = by_reason.get(t.exit_reason, 0) + 1

        anomaly_counts: dict[str, int] = {}
        for f in self.anomalies:
            anomaly_counts[f.code] = anomaly_counts.get(f.code, 0) + 1

        # MDD (누적 net PnL 기반)
        key = lambda x: x.fill_time or datetime.min.replace(tzinfo=ET)
        cumulative, peak, mdd = 0.0, 0.0, 0.0
        for t in sorted(closed, key=key):
            cumulative += t.pnl_net
            peak = max(peak, cumulative)
            mdd  = min(mdd, cumulative - peak)

        return {
            "total_trades":   len(closed),
            "win_rate":       len(wins) / len(closed),
            "profit_factor":  pf,
            "total_pnl_net":  sum(t.pnl_net for t in closed),
            "mdd":            mdd,
            "avg_slip_pct":   avg_slip,
            "partial_fills":  sum(1 for t in closed if t.partial_fill),
            "by_reason":      by_reason,
            "anomaly_counts": anomaly_counts,
            "open_remaining": len(self._open),
        }

    def print_period_report(self) -> None:
        s = self.period_summary()
        if "error" in s:
            print(f"\n[PaperJournal] {s['error']}")
            return

        print()
        print("=" * 58)
        print("  PAPER TRADING PERIOD REPORT")
        print("=" * 58)
        print(f"  총 거래        : {s['total_trades']}건")
        print(f"  Win Rate       : {s['win_rate']:.1%}")
        print(f"  Profit Factor  : {s['profit_factor']:.2f}")
        print(f"  Net PnL        : ${s['total_pnl_net']:+,.2f}")
        print(f"  MDD            : ${s['mdd']:,.2f}")
        print(f"  평균 슬리피지  : {s['avg_slip_pct']:+.4%}")
        print(f"  부분체결       : {s['partial_fills']}건")
        print(f"  청산 사유      : {s['by_reason']}")
        print(f"  미청산 잔류    : {s['open_remaining']}건")

        print()
        if s["anomaly_counts"]:
            print("  [이상 감지] " + "-" * 34)
            for code, cnt in s["anomaly_counts"].items():
                sev = "!!!" if code in ("DUPLICATE_ORDER", "UNCLOSED_POSITION", "EOD_FAIL") else "!"
                print(f"  {sev} {code}: {cnt}건")
        else:
            print("  이상 감지: 없음")

        print()
        print("  [Live 전환 판단] " + "-" * 29)
        go, reasons = self._live_readiness(s)
        print(f"  판정: {'GO - 전환 가능' if go else 'HOLD - 전환 보류'}")
        for r in reasons:
            print(f"    {r}")
        print("=" * 58)

    def _live_readiness(self, s: dict) -> tuple[bool, list[str]]:
        reasons: list[str] = []
        hard_fail = False

        hard_checks = [
            (s["anomaly_counts"].get("DUPLICATE_ORDER",   0) == 0, "DUPLICATE_ORDER 0건"),
            (s["anomaly_counts"].get("UNCLOSED_POSITION", 0) == 0, "UNCLOSED_POSITION 0건"),
            (s["anomaly_counts"].get("EOD_FAIL",          0) == 0, "EOD_FAIL 0건"),
            (s["open_remaining"] == 0,                             "미청산 포지션 0건"),
            (s["anomaly_counts"].get("BROKER_ERROR",      0) < 3,  "BROKER_ERROR 3건 미만"),
        ]
        for ok, label in hard_checks:
            reasons.append(f"  [{'OK  ' if ok else 'FAIL'}] {label}")
            if not ok:
                hard_fail = True

        soft_checks = [
            (s["profit_factor"] >= 1.0,   f"Profit Factor {s['profit_factor']:.2f} >= 1.0"),
            (s["win_rate"]      >= 0.30,  f"Win Rate {s['win_rate']:.1%} >= 30%"),
            (s["mdd"]           >= -8400, f"MDD ${s['mdd']:,.0f} >= -$8,400"),
            (s["avg_slip_pct"]  <= 0.003, f"평균 슬리피지 {s['avg_slip_pct']:+.4%} <= 0.3%"),
        ]
        for ok, label in soft_checks:
            reasons.append(f"  [{'OK  ' if ok else 'WARN'}] {label}")

        return not hard_fail, reasons


# ─────────────────────────────────────────
# CSV 헬퍼
# ─────────────────────────────────────────
_TRADE_FIELDS = [
    "date", "ticker", "status",
    "signal_time", "order_time", "fill_time",
    "shares_requested", "shares_filled", "partial_fill",
    "entry_ref", "entry_fill", "entry_slip", "entry_slip_pct",
    "tp_price", "sl_price",
    "exit_signal_time", "exit_order_time", "exit_fill_time",
    "exit_ref", "exit_fill", "exit_slip", "exit_slip_pct",
    "exit_reason", "pnl_gross", "pnl_net",
]


def _write_trades_csv(path: Path, records: list[TradeRecord]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=_TRADE_FIELDS)
        w.writeheader()
        for r in records:
            w.writerow({k: getattr(r, k, "") for k in _TRADE_FIELDS})


def _write_anomalies_csv(path: Path, flags: list[AnomalyFlag]) -> None:
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["time", "code", "ticker", "detail"])
        w.writeheader()
        for flag in flags:
            w.writerow({
                "time": flag.time.isoformat(), "code": flag.code,
                "ticker": flag.ticker, "detail": flag.detail,
            })
