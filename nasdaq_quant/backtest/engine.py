"""
backtest/engine.py
Backtester — data/features/signals/risk/execution 레이어 통합 백테스트 엔진

흐름 (거래일마다):
  1. DataManager.get_universe()        → 후보 종목
  2. RiskManager.filter_universe()     → 가격·유동성 필터
  3. DataManager.get_1min()            → 1분봉 bars
  4. FeatureBuilder.build_or_features()→ OR 피처 (09:30~09:34)
  5. bar loop (09:35~15:30):
       ORBSignal.check_entry()         → 진입 시그널
       RiskManager.validate_entry()    → 리스크 게이트
       ExecutionSimulator.fill_price() → 진입 체결가 (슬리피지)
       ORBSignal.check_exit()          → 청산 시그널
       ExecutionSimulator.fill_price() → 청산 체결가 (슬리피지)
  6. Trade 기록 (gross/net 분리)
"""
import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from config import CAPITAL, TOP_N, ET
from nasdaq_quant.data.manager import DataManager
from nasdaq_quant.data.universe import get_universe
from nasdaq_quant.features.builder import FeatureBuilder
from nasdaq_quant.signals.orb import ORBSignal, Position
from nasdaq_quant.risk.manager import RiskManager, DailyRiskState
from nasdaq_quant.execution.simulator import ExecutionSimulator

log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# Trade 결과 데이터클래스
# ─────────────────────────────────────────
@dataclass
class Trade:
    ticker:      str
    date:        date
    result:      str        # "TP" / "SL" / "EOD" / "NO_ENTRY"

    # 가격 정보
    entry_ref:   float = 0.0   # 돌파 기준가 (슬리피지 전)
    entry_fill:  float = 0.0   # 실제 체결가 (슬리피지 후)
    exit_ref:    float = 0.0   # 청산 기준가
    exit_fill:   float = 0.0   # 실제 청산 체결가

    # 수량 & 비용
    shares:      int   = 0
    commission:  float = 0.0
    slip_cost:   float = 0.0   # 왕복 슬리피지 비용 ($)

    # 손익
    pnl_gross:   float = 0.0   # (exit_ref  - entry_ref)  × shares
    pnl_net:     float = 0.0   # (exit_fill - entry_fill) × shares - commission

    # OR 정보 (분석용)
    or_high:     float = 0.0
    or_low:      float = 0.0
    or_range:    float = 0.0
    entry_time:  datetime | None = None
    exit_time:   datetime | None = None

    # 미진입 이유
    reason:      str   = ""

    @property
    def pnl_gross_pct(self) -> float:
        return self.pnl_gross / (self.entry_ref * self.shares) if self.entry_ref and self.shares else 0.0

    @property
    def pnl_net_pct(self) -> float:
        return self.pnl_net / (self.entry_fill * self.shares) if self.entry_fill and self.shares else 0.0

    @property
    def entered(self) -> bool:
        return self.result not in ("NO_ENTRY", "SKIP_OR")


# ─────────────────────────────────────────
# 거래일 목록
# ─────────────────────────────────────────
def get_trading_days(start: date, end: date) -> list[date]:
    """start~end 사이 평일 목록 (오름차순)"""
    days = []
    d = start
    while d <= end:
        if d.weekday() < 5:
            days.append(d)
        d += timedelta(days=1)
    return days


# ─────────────────────────────────────────
# 핵심 백테스트 엔진
# ─────────────────────────────────────────
class Backtester:
    def __init__(
        self,
        capital: float = CAPITAL,
        top_n: int = TOP_N,
        db_path: Path | None = None,
        regime_features_fn=None,   # 선택적: 레짐 피처 공급 함수
    ):
        self.capital    = capital
        self.top_n      = top_n
        self.dm         = DataManager(db_path=db_path) if db_path else DataManager()
        self.fb         = FeatureBuilder()
        self.signal     = ORBSignal()
        self.risk       = RiskManager(capital=capital, max_positions=top_n)
        self.exec       = ExecutionSimulator()
        self.regime_fn  = regime_features_fn   # callable(date) → dict | None

    # ── 공개 API ──────────────────────────────────────────────
    def run(
        self,
        start: date,
        end: date,
        bars_override: dict[str, dict[date, pd.DataFrame]] | None = None,
        universe_override: dict[date, list[str]] | None = None,
    ) -> list[Trade]:
        """
        기간 백테스트 실행.

        Args:
            start/end        : 테스트 기간
            bars_override    : {ticker: {date: df}} — 테스트용 1분봉 주입 (네트워크 불필요)
            universe_override: {date: [tickers]}   — 테스트용 유니버스 주입

        Returns:
            list[Trade]
        """
        all_trades: list[Trade] = []
        trading_days = get_trading_days(start, end)

        for day in trading_days:
            log.info("== %s ==", day)
            day_trades = self._run_day(day, bars_override, universe_override)
            all_trades.extend(day_trades)

        return all_trades

    def walk_forward(
        self,
        periods: list[tuple[date, date]],
        **kwargs,
    ) -> dict[str, list[Trade]]:
        """
        구간별 walk-forward 백테스트.

        Args:
            periods: [(start1, end1), (start2, end2), ...]

        Returns:
            {"2026-03-01~2026-03-14": [Trade, ...], ...}
        """
        results = {}
        for start, end in periods:
            key = f"{start}~{end}"
            results[key] = self.run(start, end, **kwargs)
        return results

    # ── 하루 시뮬레이션 ───────────────────────────────────────
    def _run_day(
        self,
        day: date,
        bars_override: dict | None,
        universe_override: dict | None,
    ) -> list[Trade]:
        trades: list[Trade] = []
        state: DailyRiskState = self.risk.new_daily_state()

        # 1. 유니버스 선정
        if universe_override and day in universe_override:
            candidates = universe_override[day]
        else:
            candidates = get_universe(day, top_n=self.top_n, dm=self.dm)

        if not candidates:
            log.info("%s: 후보 없음", day)
            return trades

        # 2. 전일 OHLCV (avg_vol 등 리스크 필터용)
        prev_ohlcv = self.dm.get_prev_day_ohlcv(candidates, day)

        # 3. 리스크 필터 (가격·유동성)
        if not prev_ohlcv.empty:
            candidates = self.risk.filter_universe(candidates, prev_ohlcv)

        # 4. 레짐 피처 (선택)
        regime_features = self.regime_fn(day) if self.regime_fn else None

        # 5. 종목별 시뮬레이션
        for ticker in candidates:
            if not self.risk.daily_loss_ok(state):
                log.info("%s: 일일 손실 한도 → 잔여 종목 스킵", day)
                break

            # 1분봉 조회
            if bars_override and ticker in bars_override and day in bars_override[ticker]:
                # override bars는 이미 해당일 데이터 → 날짜 필터 생략
                day_bars = bars_override[ticker][day]
            else:
                bars = self.dm.get_1min(ticker, day)
                if bars is None or bars.empty:
                    log.debug("%s %s: 1분봉 없음", ticker, day)
                    continue
                day_bars = bars[
                    pd.Series(bars.index).apply(lambda x: x.date() == day).values
                ]

            if day_bars is None or day_bars.empty:
                continue

            avg_vol = float(prev_ohlcv.loc[ticker, "volume"]) if (
                not prev_ohlcv.empty and ticker in prev_ohlcv.index
            ) else 1_000_000

            trade = self._simulate_ticker(ticker, day, day_bars, avg_vol, state, regime_features)
            if trade:
                trades.append(trade)

        return trades

    # ── 종목별 ORB 시뮬레이션 ─────────────────────────────────
    def _simulate_ticker(
        self,
        ticker: str,
        day: date,
        bars: pd.DataFrame,
        avg_vol: float,
        state: DailyRiskState,
        regime_features: dict | None,
    ) -> Trade | None:

        # OR 피처
        or_features = self.fb.build_or_features(bars)
        if not or_features["valid"]:
            return Trade(ticker=ticker, date=day, result="SKIP_OR",
                         reason="OR 피처 invalid", or_high=0, or_low=0, or_range=0)

        # 진입 탐색 (09:35 이후 bars)
        watch_bars = bars.between_time("09:35", "23:59")
        entry_bar  = None
        entry_ts   = None
        position   = None

        for ts, row in watch_bars.iterrows():
            # 진입 체크
            if position is None:
                if not self.signal.check_entry(or_features, row, ts):
                    continue

                entry_ref = self.signal.entry_ref_price(or_features)
                shares    = self.risk.position_size(entry_ref, self.top_n)
                ok, reason = self.risk.validate_entry(
                    ticker, entry_ref, shares, state, regime_features
                )
                if not ok:
                    return Trade(ticker=ticker, date=day, result="NO_ENTRY",
                                 reason=reason, or_high=or_features["or_high"],
                                 or_low=or_features["or_low"],
                                 or_range=or_features["or_range"])

                # 진입 체결 (슬리피지 반영)
                buy_fill = self.exec.fill_price(ticker, "buy", entry_ref, shares, avg_vol)

                position = Position(
                    ticker=ticker,
                    entry_price=buy_fill.fill_price,   # 체결가를 TP/SL 기준으로
                    entry_ref=entry_ref,
                    entry_time=ts,
                    or_high=or_features["or_high"],
                    or_low=or_features["or_low"],
                    shares=shares,
                )
                entry_bar  = row
                entry_ts   = ts
                state.open_position(ticker)
                log.debug("%s 진입: ref=%.2f fill=%.2f %d주", ticker, entry_ref, buy_fill.fill_price, shares)
                continue

            # 청산 체크
            reason = self.signal.check_exit(position, row, ts)
            if reason is None:
                continue

            exit_ref  = self.signal.exit_ref_price(reason, position, row)
            sell_fill = self.exec.fill_price(ticker, "sell", exit_ref, position.shares, avg_vol)
            comm      = buy_fill.commission + sell_fill.commission

            pnl_gross = (exit_ref            - entry_ref)          * position.shares
            pnl_net   = (sell_fill.fill_price - buy_fill.fill_price) * position.shares - comm
            slip_cost = (buy_fill.spread_cost + buy_fill.impact_cost +
                         sell_fill.spread_cost + sell_fill.impact_cost)

            state.close_position(ticker, pnl_net)

            return Trade(
                ticker=ticker, date=day, result=reason,
                entry_ref=entry_ref,       entry_fill=buy_fill.fill_price,
                exit_ref=exit_ref,         exit_fill=sell_fill.fill_price,
                shares=position.shares,    commission=comm,
                slip_cost=slip_cost,
                pnl_gross=pnl_gross,       pnl_net=pnl_net,
                or_high=or_features["or_high"],
                or_low=or_features["or_low"],
                or_range=or_features["or_range"],
                entry_time=entry_ts,       exit_time=ts,
            )

        # 진입했으나 청산 못 한 경우 (bars 종료) → 마지막 바 EOD
        if position is not None:
            last_row  = bars.iloc[-1]
            exit_ref  = float(last_row["Close"])
            sell_fill = self.exec.fill_price(ticker, "sell", exit_ref, position.shares, avg_vol)
            comm      = buy_fill.commission + sell_fill.commission
            pnl_gross = (exit_ref            - entry_ref)          * position.shares
            pnl_net   = (sell_fill.fill_price - buy_fill.fill_price) * position.shares - comm
            slip_cost = (buy_fill.spread_cost + buy_fill.impact_cost +
                         sell_fill.spread_cost + sell_fill.impact_cost)
            state.close_position(ticker, pnl_net)
            return Trade(
                ticker=ticker, date=day, result="EOD",
                entry_ref=entry_ref,       entry_fill=buy_fill.fill_price,
                exit_ref=exit_ref,         exit_fill=sell_fill.fill_price,
                shares=position.shares,    commission=comm,
                slip_cost=slip_cost,
                pnl_gross=pnl_gross,       pnl_net=pnl_net,
                or_high=or_features["or_high"],
                or_low=or_features["or_low"],
                or_range=or_features["or_range"],
                entry_time=entry_ts,       exit_time=bars.index[-1],
            )

        # 미진입
        return Trade(ticker=ticker, date=day, result="NO_ENTRY",
                     reason="진입 창 내 조건 미충족",
                     or_high=or_features["or_high"],
                     or_low=or_features["or_low"],
                     or_range=or_features["or_range"])
