"""
live/runner.py
ORBLiveRunner — 실시간 ORB paper trading 실행기

흐름:
  09:20  _prepare()          → 유니버스 선정 + 리스크 필터
  09:35  _build_or()         → OR 바 수집 (09:30-09:34) + 피처 계산
  09:36~ _watch_loop()       → 1분 폴링: 진입/청산 시그널
  15:30  _eod_close()        → 미청산 포지션 강제 청산
"""
import logging
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd
import yfinance as yf

from config import CAPITAL, TOP_N
from nasdaq_quant.data.manager import DataManager
from nasdaq_quant.data.universe import get_universe
from nasdaq_quant.features.builder import FeatureBuilder
from nasdaq_quant.signals.orb import ORBSignal, Position
from nasdaq_quant.risk.manager import RiskManager, DailyRiskState
from nasdaq_quant.live.broker import AlpacaBroker, FillInfo, OrderResult
from nasdaq_quant.live.journal import PaperJournal

ET = ZoneInfo("America/New_York")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 내부 상태 클래스
# ─────────────────────────────────────────
@dataclass
class LivePosition:
    ticker:       str
    shares:       int
    entry_fill:   float        # 실제 Alpaca 체결가
    entry_ref:    float        # OR high (기준가)
    entry_time:   datetime
    or_high:      float
    or_low:       float
    orb_position: Position     # check_exit 용
    buy_order_id: str = ""


# ─────────────────────────────────────────
# 헬퍼
# ─────────────────────────────────────────
def _wait_until(target: datetime) -> None:
    """target 시각까지 대기"""
    delta = (target - datetime.now(ET)).total_seconds()
    if delta > 0:
        log.debug("%.1f초 대기 → %s", delta, target.strftime("%H:%M:%S"))
        time.sleep(delta)


def _fetch_bars_yf(tickers: list[str], session_date: date) -> dict[str, pd.DataFrame]:
    """yfinance로 당일 1분봉 조회 → {ticker: DataFrame(Open/High/Low/Close/Volume)}"""
    if not tickers:
        return {}

    try:
        raw = yf.download(
            tickers if len(tickers) > 1 else tickers[0],
            period="1d",
            interval="1m",
            progress=False,
            auto_adjust=True,
        )
    except Exception as e:
        log.warning("yfinance 조회 실패: %s", e)
        return {}

    if raw is None or raw.empty:
        return {}

    # ET 변환
    if raw.index.tzinfo is None:
        raw.index = raw.index.tz_localize("UTC").tz_convert(ET)
    else:
        raw.index = raw.index.tz_convert(ET)

    result: dict[str, pd.DataFrame] = {}

    def _extract_single(df: pd.DataFrame, ticker: str) -> None:
        rows = df[pd.Series(df.index).apply(lambda x: x.date() == session_date).values].copy()
        rows = rows.dropna(how="all")
        if not rows.empty:
            result[ticker] = rows

    if len(tickers) == 1:
        # 단일 종목 — flat DataFrame
        _extract_single(raw, tickers[0])
    elif isinstance(raw.columns, pd.MultiIndex):
        # 다중 종목 — MultiIndex (Price, Ticker)
        available = raw.columns.get_level_values(1).unique().tolist()
        for ticker in tickers:
            if ticker not in available:
                continue
            try:
                df = raw.xs(ticker, axis=1, level=1).copy()
                _extract_single(df, ticker)
            except Exception:
                pass
    else:
        # 다중 요청이지만 flat 반환 — 단일 종목 폴백
        if tickers:
            _extract_single(raw, tickers[0])

    return result


# ─────────────────────────────────────────
# ORBLiveRunner
# ─────────────────────────────────────────
class ORBLiveRunner:
    """
    ORB 전략 실시간 실행기.

    Args:
        broker     : AlpacaBroker 인스턴스
        notifier   : .send(msg) 메서드를 가진 알림 객체 (TelegramNotifier 등)
        capital    : 운용 자본 ($)
        top_n      : 동시 최대 보유 종목 수
        dry_run    : True = 시그널만 로그, 실제 주문 없음
        db_path    : DataManager SQLite 경로 (None = 기본)
    """

    def __init__(
        self,
        broker: AlpacaBroker,
        notifier,
        capital: float = CAPITAL,
        top_n: int = TOP_N,
        dry_run: bool = False,
        db_path: Path | None = None,
        journal: PaperJournal | None = None,
    ):
        self.broker   = broker
        self.notifier = notifier
        self.capital  = capital
        self.top_n    = top_n
        self.dry_run  = dry_run
        self.journal  = journal

        self.dm     = DataManager(db_path=db_path) if db_path else DataManager()
        self.fb     = FeatureBuilder()
        self.signal = ORBSignal()
        self.risk   = RiskManager(capital=capital, max_positions=top_n)

        # 일별 리셋 상태
        self._reset()

    def _reset(self) -> None:
        self.candidates:  list[str]               = []
        self.prev_ohlcv:  pd.DataFrame            = pd.DataFrame()
        self.or_features: dict[str, dict]         = {}
        self.positions:   dict[str, LivePosition] = {}
        self.state:       DailyRiskState | None   = None

    # ── 공개 API ──────────────────────────────────────────────────
    def run_day(self, day: date | None = None) -> None:
        """하루 전체 실행. day=None이면 오늘."""
        day = day or datetime.now(ET).date()
        self._reset()

        log.info("=" * 55)
        log.info("ORB Live Runner 시작: %s  dry_run=%s", day, self.dry_run)
        log.info("=" * 55)

        try:
            self._prepare(day)
            if not self.candidates:
                self.notifier.send(f"[{day}] 후보 없음 — 오늘 거래 없음")
                return

            self._build_or(day)
            if not self.or_features:
                self.notifier.send(f"[{day}] OR 피처 유효 종목 없음 — 진입 건너뜀")
                return

            self._watch_loop(day)

        except KeyboardInterrupt:
            log.warning("수동 중단 — EOD 청산 시작")
            self.notifier.send("수동 중단 — 포지션 청산 중...")
            self._eod_close("INTERRUPT")

        except Exception as e:
            log.error("실행 오류: %s", e, exc_info=True)
            self.notifier.send(f"ORB 오류: `{e}`\n포지션 청산 중...")
            self._eod_close("ERROR")

        finally:
            if self.journal:
                self.journal.save_daily(day)

    # ── 내부 단계 ─────────────────────────────────────────────────
    def _prepare(self, day: date) -> None:
        """09:20: 유니버스 선정 + 리스크 필터"""
        log.info("[준비] 유니버스 선정 중...")
        raw_candidates = get_universe(day, top_n=self.top_n * 3, dm=self.dm)
        self.prev_ohlcv = self.dm.get_prev_day_ohlcv(raw_candidates, day)

        if not self.prev_ohlcv.empty:
            self.candidates = self.risk.filter_universe(raw_candidates, self.prev_ohlcv)
        else:
            self.candidates = raw_candidates

        self.candidates = self.candidates[: self.top_n * 2]  # 여유분 포함
        self.state = self.risk.new_daily_state()

        log.info("[준비] 후보 %d종목: %s", len(self.candidates), self.candidates)
        self.notifier.send(
            f"*ORB 준비* [{day}]\n"
            f"후보: {', '.join(f'`{t}`' for t in self.candidates)}\n"
            f"{'(DRY RUN)' if self.dry_run else ''}"
        )

    def _build_or(self, day: date) -> None:
        """09:35:05에 OR 바 일괄 수집 → 피처 계산"""
        or_end_wait = datetime(day.year, day.month, day.day, 9, 35, 10, tzinfo=ET)
        log.info("[OR] %s 까지 대기...", or_end_wait.strftime("%H:%M:%S"))
        _wait_until(or_end_wait)

        log.info("[OR] 1분봉 수집 중 (%d종목)...", len(self.candidates))
        all_bars = _fetch_bars_yf(self.candidates, day)

        for ticker in self.candidates:
            if ticker not in all_bars:
                log.debug("%s 오늘 바 없음", ticker)
                continue
            bars = all_bars[ticker]
            or_bars = bars.between_time("09:30", "09:34")
            if len(or_bars) < 3:
                log.warning("%s OR 바 부족 (%d개)", ticker, len(or_bars))
                continue
            feat = self.fb.build_or_features(or_bars)
            if feat["valid"]:
                self.or_features[ticker] = feat
                log.info(
                    "[OR] %s  high=%.2f  low=%.2f  range=%.4f",
                    ticker, feat["or_high"], feat["or_low"], feat["or_range"],
                )

        valid = list(self.or_features.keys())
        log.info("[OR] 유효 종목 %d개: %s", len(valid), valid)
        self.notifier.send(
            f"*OR 완료* {len(valid)}종목 유효\n"
            + "\n".join(
                f"  `{t}` H={self.or_features[t]['or_high']:.2f} "
                f"L={self.or_features[t]['or_low']:.2f}"
                for t in valid
            )
        )

    def _watch_loop(self, day: date) -> None:
        """09:36:05부터 1분 폴링: 진입/청산 시그널"""
        entry_deadline = datetime(day.year, day.month, day.day, 13,  0, 0, tzinfo=ET)
        eod_time       = datetime(day.year, day.month, day.day, 15, 30, 0, tzinfo=ET)
        next_poll      = datetime(day.year, day.month, day.day,  9, 36, 5, tzinfo=ET)

        log.info("[WATCH] 09:36:05 부터 1분 폴링 시작")

        while True:
            _wait_until(next_poll)
            now = datetime.now(ET)

            # EOD 강제 청산
            if now >= eod_time:
                log.info("[WATCH] 15:30 EOD → 강제 청산")
                self._eod_close("EOD")
                break

            watch_tickers = list(
                set(self.or_features.keys()) | set(self.positions.keys())
            )
            if not watch_tickers:
                next_poll += timedelta(minutes=1)
                continue

            # 1분봉 폴링
            all_bars = _fetch_bars_yf(watch_tickers, day)

            for ticker in watch_tickers:
                if ticker not in all_bars:
                    continue
                bars = all_bars[ticker]

                # 직전 완성 바 (현재 시각 - 1분 이내)
                cutoff = now - timedelta(minutes=1, seconds=30)
                recent = bars[bars.index >= cutoff]
                if recent.empty:
                    continue
                bar = recent.iloc[-1]
                ts  = recent.index[-1]

                if ticker in self.positions:
                    self._check_exit(ticker, bar, ts)
                elif (
                    ticker in self.or_features
                    and now <= entry_deadline
                    and self.risk.daily_loss_ok(self.state)
                ):
                    self._check_entry(ticker, bar, ts)

            next_poll += timedelta(minutes=1)

    def _check_entry(self, ticker: str, bar: pd.Series, ts: datetime) -> None:
        # 이미 보유 중이면 중복 진입 방지
        if ticker in self.positions:
            return

        or_feat = self.or_features[ticker]
        if not self.signal.check_entry(or_feat, bar, ts):
            return

        entry_ref = self.signal.entry_ref_price(or_feat)
        shares    = self.risk.position_size(entry_ref, self.top_n)
        if shares <= 0:
            log.warning("[ENTRY] %s 주수 0 → 건너뜀 (entry_ref=%.2f)", ticker, entry_ref)
            return

        ok, reason = self.risk.validate_entry(ticker, entry_ref, shares, self.state)
        if not ok:
            log.info("[ENTRY] %s 진입 거부: %s", ticker, reason)
            return

        log.info("[ENTRY] %s  ref=%.2f  %d주  %s",
                 ticker, entry_ref, shares, "(DRY)" if self.dry_run else "→ 매수")

        signal_time = ts
        if self.dry_run:
            order_time  = ts
            fill_time   = ts
            fill_price  = entry_ref
            filled_qty  = shares
            order_id    = "DRY_RUN"
        else:
            order_time = datetime.now(ET)
            result = self.broker.buy(ticker, shares)
            if not result.ok:
                self.notifier.send(f"매수 실패 `{ticker}`: {result.error}")
                if self.journal:
                    self.journal.flag_anomaly("BROKER_ERROR", ticker,
                                              f"buy failed: {result.error}")
                return
            fill_info  = self.broker.get_fill_info(result.order_id)
            fill_time  = datetime.now(ET)
            fill_price = fill_info.avg_price  or entry_ref
            filled_qty = fill_info.filled_qty or shares
            order_id   = result.order_id

        orb_pos = Position(
            ticker      = ticker,
            entry_price = fill_price,
            entry_ref   = entry_ref,
            entry_time  = ts,
            or_high     = or_feat["or_high"],
            or_low      = or_feat["or_low"],
            shares      = shares,
        )

        self.positions[ticker] = LivePosition(
            ticker       = ticker,
            shares       = shares,
            entry_fill   = fill_price,
            entry_ref    = entry_ref,
            entry_time   = ts,
            or_high      = or_feat["or_high"],
            or_low       = or_feat["or_low"],
            orb_position = orb_pos,
            buy_order_id = order_id,
        )
        self.state.open_position(ticker)

        if self.journal:
            self.journal.open_trade(
                date             = ts.date(),
                ticker           = ticker,
                signal_time      = signal_time,
                order_time       = order_time,
                fill_time        = fill_time,
                shares_requested = shares,
                shares_filled    = filled_qty,
                entry_ref        = entry_ref,
                entry_fill       = fill_price,
                tp_price         = orb_pos.tp_price,
                sl_price         = orb_pos.sl_price,
            )

        self.notifier.send(
            f"*매수* `{ticker}` {shares}주 @ ${fill_price:.2f}\n"
            f"TP: `${orb_pos.tp_price:.2f}`  SL: `${orb_pos.sl_price:.2f}`"
        )

    def _check_exit(self, ticker: str, bar: pd.Series, ts: datetime) -> None:
        live_pos = self.positions[ticker]
        orb_pos  = live_pos.orb_position
        reason   = self.signal.check_exit(orb_pos, bar, ts)
        if reason is None:
            return

        exit_ref = self.signal.exit_ref_price(reason, orb_pos, bar)
        shares   = live_pos.shares

        log.info("[EXIT] %s  %s  ref=%.2f  %s",
                 ticker, reason, exit_ref, "(DRY)" if self.dry_run else "→ 매도")

        exit_signal_time = ts
        if self.dry_run:
            exit_order_time = ts
            exit_fill_time  = ts
            fill_price      = exit_ref
        else:
            exit_order_time = datetime.now(ET)
            result = self.broker.sell(ticker, shares)
            if not result.ok:
                self.notifier.send(f"매도 실패 `{ticker}`: {result.error}")
                if self.journal:
                    self.journal.flag_anomaly("BROKER_ERROR", ticker,
                                              f"sell failed: {result.error}")
                return
            fill_info       = self.broker.get_fill_info(result.order_id)
            exit_fill_time  = datetime.now(ET)
            fill_price      = fill_info.avg_price or exit_ref

        pnl_net = (fill_price - live_pos.entry_fill) * shares
        self.state.close_position(ticker, pnl_net)

        if self.journal:
            self.journal.close_trade(
                ticker           = ticker,
                exit_signal_time = exit_signal_time,
                exit_order_time  = exit_order_time,
                exit_fill_time   = exit_fill_time,
                exit_ref         = exit_ref,
                exit_fill        = fill_price,
                exit_reason      = reason,
            )

        del self.positions[ticker]

        emoji = "TP" if reason == "TP" else "SL" if reason == "SL" else "EOD"
        self.notifier.send(
            f"*{emoji}* `{ticker}` {shares}주 @ ${fill_price:.2f}\n"
            f"PnL: `${pnl_net:+.2f}`"
        )

    def _eod_close(self, reason: str = "EOD") -> None:
        if not self.positions:
            return
        tickers = list(self.positions.keys())
        log.info("[EOD] 강제 청산: %s  reason=%s", tickers, reason)

        if not self.dry_run:
            self.broker.close_all()
            # EOD 후 잔류 포지션 확인
            time.sleep(3.0)
            remaining = self.broker.get_positions()
            for sym, qty in remaining.items():
                log.error("[EOD] 잔류 포지션 감지: %s qty=%s", sym, qty)
                if self.journal:
                    self.journal.flag_anomaly(
                        "EOD_FAIL", sym,
                        f"position still open after close_all: qty={qty}",
                    )
            if remaining:
                self.notifier.send(
                    f"[경고] EOD 청산 후 잔류 포지션: {list(remaining.keys())}"
                )

        for ticker in tickers:
            lp = self.positions[ticker]
            if self.journal:
                self.journal.close_trade(
                    ticker           = ticker,
                    exit_signal_time = datetime.now(ET),
                    exit_order_time  = datetime.now(ET),
                    exit_fill_time   = datetime.now(ET),
                    exit_ref         = lp.entry_fill,   # EOD: 기준가 = 진입가 (미확인)
                    exit_fill        = lp.entry_fill,
                    exit_reason      = reason,
                )
            self.notifier.send(
                f"*EOD 청산* `{ticker}` {lp.shares}주  reason={reason}"
            )
            self.state.close_position(ticker, 0.0)

        self.positions.clear()
        total_pnl = self.state.realized_pnl
        self.notifier.send(
            f"*당일 완료*  실현PnL: `${total_pnl:+.2f}`"
        )
