"""
strategy_engine.py
미국/한국 주식 백테스트 엔진

전략:
  1. Momentum Enhanced  — 모멘텀 20/60/120일 + 거래량 필터 + 시장 지수 필터
  2. Factor Combo       — 모멘텀 + 저변동성 복합 스코어
  3. Mean Reversion     — 단기 낙폭 과대 종목 매수 (KR 평균 회귀 특성 활용)

백테스트 기간: 학습 2020~2022 / 검증 2023~2024
거래비용: 0.2% (수수료 + 슬리피지)
데이터 소스: market_data.db (data_collector.py 생성)
"""

import sqlite3
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional
from dataclasses import dataclass, field

import numpy as np
import pandas as pd
import pandas_ta as ta

DB_PATH       = Path("market_data.db")
CSV_PATH      = Path("backtest_results.csv")
BT_START      = "2020-01-01"
BT_END        = "2024-12-31"
TRAIN_START   = "2020-01-01"
TRAIN_END     = "2022-12-31"
VAL_START     = "2023-01-01"
VAL_END       = "2024-12-31"
DEFAULT_COST  = 0.002   # 수수료 + 슬리피지 0.2%

# 시장 지수 (regime filter)
MARKET_INDEX  = {"KR": "069500", "US": "SPY"}   # 069500 = KODEX 200

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# 데이터 로더
# ─────────────────────────────────────────
def _load_pivot(
    col: str,
    tickers: Optional[list[str]],
    market: str,
    start: str,
    end: str,
    db_path: Path,
) -> pd.DataFrame:
    """지정 컬럼의 피벗 테이블 반환 (index=date, columns=ticker)"""
    table = "us_ohlcv" if market == "US" else "kr_ohlcv"
    with sqlite3.connect(db_path) as conn:
        if tickers:
            ph = ",".join("?" * len(tickers))
            df = pd.read_sql_query(
                f"SELECT ticker, date, {col} FROM {table} "
                f"WHERE ticker IN ({ph}) AND date BETWEEN ? AND ? ORDER BY date",
                conn, params=tickers + [start, end],
            )
        else:
            df = pd.read_sql_query(
                f"SELECT ticker, date, {col} FROM {table} "
                f"WHERE date BETWEEN ? AND ? ORDER BY date",
                conn, params=[start, end],
            )
    if df.empty:
        raise ValueError(f"데이터 없음: {col}, market={market}, {start}~{end}")
    pivot = df.pivot(index="date", columns="ticker", values=col)
    pivot.index = pd.to_datetime(pivot.index)
    return pivot.sort_index().ffill()


def load_prices(tickers, market, start, end, db_path) -> pd.DataFrame:
    return _load_pivot("close", tickers, market, start, end, db_path)


def load_volumes(tickers, market, start, end, db_path) -> pd.DataFrame:
    return _load_pivot("volume", tickers, market, start, end, db_path)


def load_market_cap(tickers, market, start, end, db_path) -> pd.DataFrame:
    """시가총액 피벗 테이블 반환 (KR 전용, market_cap 컬럼)"""
    if market != "KR":
        return pd.DataFrame()
    try:
        return _load_pivot("market_cap", tickers, market, start, end, db_path)
    except Exception as e:
        log.warning("[QualityFilter] market_cap 로드 실패: %s", e)
        return pd.DataFrame()


def load_market_regime(
    market: str,
    start: str,
    end: str,
    ma_window: int = 200,
    db_path: Path = DB_PATH,
) -> pd.Series:
    """
    시장 지수가 MA(ma_window) 위에 있으면 True 반환 (regime filter)
    KR: 069500(KODEX 200), US: SPY
    """
    index_ticker = MARKET_INDEX[market]
    # 워밍업 포함 확장 조회
    try:
        prices = _load_pivot("close", [index_ticker], market, "2018-01-01", end, db_path)
        s = prices[index_ticker].dropna()
        ma = ta.sma(s, length=ma_window)
        regime = (s > ma).reindex(
            pd.date_range(start, end, freq="B"), method="ffill"
        ).fillna(False)
        log.info("[Regime] %s MA%d 필터 로드 완료 (True 비율=%.1f%%)",
                 index_ticker, ma_window, regime.mean() * 100)
        return regime
    except Exception as e:
        log.warning("[Regime] %s 로드 실패 (%s) → 필터 비활성", index_ticker, e)
        return pd.Series(True, index=pd.date_range(start, end, freq="B"))


def load_universe(market: str = "KR", db_path: Path = DB_PATH) -> list[str]:
    table = "kr_ohlcv" if market == "KR" else "us_ohlcv"
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute(f"SELECT DISTINCT ticker FROM {table}").fetchall()
    return [r[0] for r in rows]


# ─────────────────────────────────────────
# 기술 지표 (pandas_ta)
# ─────────────────────────────────────────
def calc_rsi(series: pd.Series, period: int = 14) -> pd.Series:
    r = ta.rsi(series, length=period)
    return r if r is not None else pd.Series(np.nan, index=series.index)


def calc_ma(series: pd.Series, window: int) -> pd.Series:
    r = ta.sma(series, length=window)
    return r if r is not None else series.rolling(window).mean()


# ─────────────────────────────────────────
# 백테스트 결과
# ─────────────────────────────────────────
@dataclass
class BacktestResult:
    strategy: str
    market: str
    equity: pd.Series
    trades: pd.DataFrame
    params: dict = field(default_factory=dict)

    @property
    def total_return(self) -> float:
        return self.equity.iloc[-1] / self.equity.iloc[0] - 1

    @property
    def cagr(self) -> float:
        years = (self.equity.index[-1] - self.equity.index[0]).days / 365.25
        return (1 + self.total_return) ** (1 / years) - 1 if years > 0 else 0.0

    @property
    def mdd(self) -> float:
        roll_max = self.equity.cummax()
        return ((self.equity - roll_max) / roll_max).min()

    @property
    def sharpe(self) -> float:
        rf = 0.02
        ret = self.equity.pct_change().dropna()
        std = ret.std()
        if std == 0 or pd.isna(std):
            return 0.0
        return float((ret.mean() - rf / 252) / std * np.sqrt(252))

    @property
    def win_rate(self) -> float:
        if self.trades.empty or "pnl" not in self.trades.columns:
            return 0.0
        closed = self.trades[self.trades["pnl"].notna()]
        return float((closed["pnl"] > 0).mean()) if len(closed) > 0 else 0.0

    def summary(self) -> dict:
        return {
            "strategy":     self.strategy,
            "market":       self.market,
            "period":       f"{self.equity.index[0].date()} ~ {self.equity.index[-1].date()}",
            "total_return": f"{self.total_return:.2%}",
            "cagr":         f"{self.cagr:.2%}",
            "mdd":          f"{self.mdd:.2%}",
            "sharpe":       f"{self.sharpe:.2f}",
            "win_rate":     f"{self.win_rate:.2%}",
            "trades":       len(self.trades),
        }

    def print_summary(self):
        s = self.summary()
        print("\n" + "=" * 52)
        print(f"  {s['strategy']} | {s['market']} | {s['period']}")
        print("=" * 52)
        for k, v in list(s.items())[3:]:
            print(f"  {k:<16}: {v}")
        print("=" * 52)

    def to_csv_row(self, period_label: str = "") -> dict:
        s = self.summary()
        s["period_label"] = period_label
        s["params"] = str(self.params)
        s["run_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        return s


# ─────────────────────────────────────────
# 크로스섹셔널 백테스터
# ─────────────────────────────────────────
def _run_cross_sectional(
    scores: pd.DataFrame,
    prices: pd.DataFrame,
    top_k: int = 20,
    rebalance_freq: str = "W",
    cost: float = DEFAULT_COST,
    regime_mask: Optional[pd.Series] = None,   # date → bool
    reverse: bool = False,                      # True → 하위 종목 매수 (역방향)
    vol_scale: bool = False,                    # True → 역변동성 가중
    vol_window: int = 20,
    stop_loss: float = 0.0,                     # 0 = 비활성, 0.1 = 진입가 대비 -10% 손절
) -> tuple[pd.Series, pd.DataFrame]:
    """상위(혹은 하위) top_k 종목 보유. vol_scale=True이면 역변동성 가중, 아니면 동일가중."""
    rebal_dates = set(
        prices.groupby(pd.Grouper(freq=rebalance_freq))
              .apply(lambda g: g.index[-1] if len(g) > 0 else pd.NaT)
              .dropna()
    )

    equity = pd.Series(1.0, index=prices.index)
    trade_list = []
    holdings: dict[str, float] = {}       # ticker → weight
    entry_prices: dict[str, float] = {}   # ticker → 진입가 (손절 기준)

    for i in range(1, len(prices)):
        date = prices.index[i]
        prev_date = prices.index[i - 1]

        # ── 수익률 계산 (기존 holdings 가중 수익률) ──
        if holdings:
            port_ret = 0.0
            for t, w in holdings.items():
                if t in prices.columns:
                    p0, p1 = prices.at[prev_date, t], prices.at[date, t]
                    if not (pd.isna(p0) or pd.isna(p1) or p0 == 0):
                        port_ret += w * (p1 - p0) / p0
            equity.iloc[i] = equity.iloc[i - 1] * (1 + port_ret)
        else:
            equity.iloc[i] = equity.iloc[i - 1]

        # ── 손절 룰: 진입가 대비 -stop_loss 이하 종목 강제 청산 ──
        if stop_loss > 0 and holdings:
            stopped: list[str] = []
            for t, ep in entry_prices.items():
                if t in prices.columns and ep > 0:
                    cur = prices.at[date, t]
                    if not pd.isna(cur) and (cur - ep) / ep <= -stop_loss:
                        stopped.append(t)
            if stopped:
                total_w = sum(holdings.get(t, 0) for t in stopped)
                equity.iloc[i] *= (1 - total_w * cost)  # 청산 비용
                for t in stopped:
                    holdings.pop(t, None)
                    entry_prices.pop(t, None)
                    trade_list.append({"date": date, "ticker": t, "action": "STOP", "price": prices.at[date, t]})
                # 잔여 holdings 비중 재정규화
                if holdings:
                    total = sum(holdings.values())
                    if total > 0:
                        holdings = {t: w / total for t, w in holdings.items()}

        # ── 리밸런싱 (shift(1) 적용된 scores 사용) ──
        if date in rebal_dates and date in scores.index:
            row = scores.loc[date].dropna()

            # 시장 regime 필터
            in_regime = True
            if regime_mask is not None:
                ts = regime_mask.index
                idx = ts.searchsorted(date, side="right") - 1
                in_regime = bool(regime_mask.iloc[idx]) if idx >= 0 else True

            selected: list[str] = (
                (row.nsmallest(top_k) if reverse else row.nlargest(top_k)).index.tolist()
                if in_regime else []
            )

            # 비중 결정
            if not selected:
                new_holdings: dict[str, float] = {}
            elif vol_scale:
                vols: dict[str, float] = {}
                for t in selected:
                    if t in prices.columns:
                        s = prices[t].pct_change().iloc[max(0, i - vol_window):i].dropna()
                        v = float(s.std())
                        if v > 0 and not pd.isna(v):
                            vols[t] = v
                if vols:
                    inv = {t: 1.0 / v for t, v in vols.items()}
                    total_inv = sum(inv.values())
                    new_holdings = {t: w / total_inv for t, w in inv.items()}
                    # vol 계산 불가 종목은 잔여 비중 equal 배분
                    missing = [t for t in selected if t not in new_holdings]
                    if missing:
                        residual = max(0.0, 1.0 - sum(new_holdings.values()))
                        eq_w = residual / len(missing)
                        for t in missing:
                            new_holdings[t] = eq_w
                else:
                    new_holdings = {t: 1.0 / len(selected) for t in selected}
            else:
                new_holdings = {t: 1.0 / len(selected) for t in selected}

            # ── 거래비용 (회전율 기반) ──
            old_set = set(holdings.keys())
            new_set = set(new_holdings.keys())
            exits   = old_set - new_set
            entries = new_set - old_set
            turnover = (len(exits) + len(entries)) / (2 * max(top_k, 1))
            if turnover > 0:
                equity.iloc[i] *= (1 - turnover * cost)

            for t in exits:
                p = prices.at[date, t] if t in prices.columns else np.nan
                if not pd.isna(p):
                    trade_list.append({"date": date, "ticker": t, "action": "SELL", "price": p})
                entry_prices.pop(t, None)
            for t in entries:
                p = prices.at[date, t] if t in prices.columns else np.nan
                if not pd.isna(p):
                    trade_list.append({"date": date, "ticker": t, "action": "BUY", "price": p})
                    entry_prices[t] = p if not pd.isna(p) else 0.0

            holdings = new_holdings

    trades = pd.DataFrame(trade_list)
    if not trades.empty and "pnl" not in trades.columns:
        trades["pnl"] = np.nan
    return equity, trades


# ─────────────────────────────────────────
# 전략 1: Momentum Enhanced (20/60/120일)
# ─────────────────────────────────────────
def strategy_momentum_enhanced(
    market: str = "KR",
    tickers: Optional[list[str]] = None,
    lookbacks: list = [20, 60, 120],
    top_k: int = 20,
    rebalance_freq: str = "W",
    vol_filter: bool = True,
    vol_window: int = 20,
    vol_multiplier: float = 1.5,
    market_filter: bool = True,
    market_ma: int = 200,
    start: str = BT_START,
    end: str = BT_END,
    cost: float = DEFAULT_COST,
    db_path: Path = DB_PATH,
) -> dict[str, BacktestResult]:
    """
    모멘텀 전략 (lookback 20/60/120일 비교)
    - 거래량 필터: 20일 평균 대비 1.5배 이상인 종목만 매수 후보
    - 시장 필터: 지수(SPY/069500) 200일 MA 위일 때만 진입
    """
    log.info("[Momentum] 데이터 로드 중... market=%s, lookbacks=%s", market, lookbacks)
    prices = load_prices(tickers, market, start, end, db_path)
    log.info("[Momentum] 종목수=%d, 기간=%s~%s", prices.shape[1], start, end)

    # 거래량 필터 마스크 (date x ticker → bool)
    vol_mask = None
    if vol_filter:
        try:
            volumes = load_volumes(tickers, market, start, end, db_path)
            vol_avg = volumes.rolling(vol_window).mean()
            vol_mask = volumes >= (vol_avg * vol_multiplier)
            log.info("[Momentum] 거래량 필터 적용 (x%.1f, %d일)", vol_multiplier, vol_window)
        except Exception as e:
            log.warning("[Momentum] 거래량 필터 로드 실패: %s", e)

    # 시장 regime 필터
    regime = None
    if market_filter:
        regime = load_market_regime(market, start, end, market_ma, db_path)

    results: dict[str, BacktestResult] = {}

    for lb in lookbacks:
        log.info("[Momentum-%dd] 스코어 계산 중...", lb)
        scores = prices.pct_change(lb)

        # 거래량 필터 적용: 조건 미충족 종목 스코어 → NaN
        if vol_mask is not None:
            scores = scores.where(vol_mask)

        # 버그1 수정: 당일 종가 신호 → 다음날 적용 (look-ahead bias 제거)
        scores = scores.shift(1)

        equity, trades = _run_cross_sectional(
            scores, prices, top_k, rebalance_freq, cost, regime
        )
        key = f"Momentum-{lb}d"
        r = BacktestResult(
            strategy=key,
            market=market,
            equity=equity,
            trades=trades,
            params={
                "lookback": lb, "top_k": top_k,
                "vol_filter": vol_filter, "market_filter": market_filter,
            },
        )
        r.print_summary()
        results[key] = r

    return results


# ─────────────────────────────────────────
# 전략 2: 팩터 복합 (모멘텀 + 저변동성)
# ─────────────────────────────────────────
def strategy_factor_combo(
    market: str = "KR",
    tickers: Optional[list[str]] = None,
    momentum_window: int = 60,
    vol_window: int = 20,
    top_k: int = 20,
    momentum_weight: float = 0.6,
    rebalance_freq: str = "W",
    market_filter: bool = True,
    market_ma: int = 200,
    start: str = BT_START,
    end: str = BT_END,
    cost: float = DEFAULT_COST,
    db_path: Path = DB_PATH,
) -> BacktestResult:
    """모멘텀 + 저변동성 복합 스코어 (z-score 정규화 후 가중합)"""
    log.info("[FactorCombo] 데이터 로드 중... market=%s", market)
    prices = load_prices(tickers, market, start, end, db_path)
    log.info("[FactorCombo] 종목수=%d", prices.shape[1])

    rets = prices.pct_change()

    def zscore(df: pd.DataFrame) -> pd.DataFrame:
        return df.sub(df.mean(axis=1), axis=0).div(
            df.std(axis=1).replace(0, np.nan), axis=0
        )

    mom_z = zscore(rets.rolling(momentum_window).sum())
    vol_z = zscore(-rets.rolling(vol_window).std())
    scores = (mom_z * momentum_weight + vol_z * (1 - momentum_weight)).shift(1)

    regime = load_market_regime(market, start, end, market_ma, db_path) if market_filter else None

    equity, trades = _run_cross_sectional(scores, prices, top_k, rebalance_freq, cost, regime)

    result = BacktestResult(
        strategy="Factor Combo",
        market=market,
        equity=equity,
        trades=trades,
        params={
            "momentum_window": momentum_window, "vol_window": vol_window,
            "top_k": top_k, "momentum_weight": momentum_weight,
            "market_filter": market_filter,
        },
    )
    result.print_summary()
    return result


# ─────────────────────────────────────────
# 전략 3: Mean Reversion (역방향 — KR 특화)
# ─────────────────────────────────────────
def strategy_mean_reversion(
    market: str = "KR",
    tickers: Optional[list[str]] = None,
    lookbacks: list = [5, 10, 20, 60],
    top_k: int = 20,
    rebalance_freq: str = "W",
    market_filter: bool = True,
    market_ma: int = 200,
    vol_scale: bool = False,
    stop_loss: float = 0.0,
    min_market_cap: float = 0.0,    # 최소 시가총액 (원, 0=비활성)
    min_avg_volume: float = 0.0,    # 최소 20일 평균 거래량 (주, 0=비활성)
    start: str = BT_START,
    end: str = BT_END,
    cost: float = DEFAULT_COST,
    db_path: Path = DB_PATH,
) -> dict[str, "BacktestResult"]:
    """
    평균 회귀 전략 — 단기 낙폭 과대(하위 top_k) 종목 매수
    KR 시장의 음수 모멘텀(평균 회귀) 특성을 역이용.
    - 스코어: lookback 기간 수익률 하위 top_k 선택
    - 시장 필터: 지수 200일 MA 위일 때만 진입 (급락장 보호)
    - 퀄리티 필터: 시가총액/거래량 기준 저유동성 종목 제외
    """
    log.info("[MeanReversion] 데이터 로드 중... market=%s, lookbacks=%s", market, lookbacks)
    prices = load_prices(tickers, market, start, end, db_path)
    log.info("[MeanReversion] 종목수=%d, 기간=%s~%s", prices.shape[1], start, end)

    regime = load_market_regime(market, start, end, market_ma, db_path) if market_filter else None

    # ── 퀄리티 필터 마스크 생성 ──
    quality_mask = None
    if min_market_cap > 0 or min_avg_volume > 0:
        masks = []
        if min_market_cap > 0:
            mcap = load_market_cap(tickers, market, start, end, db_path)
            if not mcap.empty and mcap.notna().any().any():
                mcap = mcap.reindex(columns=prices.columns)
                masks.append(mcap >= min_market_cap)
                log.info("[QualityFilter] 시가총액 필터 적용 (일별, >= %.0f억)", min_market_cap / 1e8)
            else:
                # kr_ohlcv.market_cap이 없으면 kr_meta 스냅샷으로 fallback
                try:
                    with sqlite3.connect(db_path) as conn:
                        meta = pd.read_sql_query(
                            "SELECT ticker, market_cap FROM kr_meta WHERE market_cap IS NOT NULL",
                            conn,
                        ).set_index("ticker")["market_cap"]
                    eligible = set(meta[meta >= min_market_cap].index)
                    eligible_cols = [c for c in prices.columns if c in eligible]
                    if eligible_cols:
                        snap_mask = pd.DataFrame(False, index=prices.index, columns=prices.columns)
                        snap_mask[eligible_cols] = True
                        masks.append(snap_mask)
                        log.info("[QualityFilter] 시가총액 필터 적용 (스냅샷, >= %.0f억, %d종목)",
                                 min_market_cap / 1e8, len(eligible_cols))
                    else:
                        log.warning("[QualityFilter] 시가총액 스냅샷 없음 → 필터 비활성")
                except Exception as e:
                    log.warning("[QualityFilter] 시가총액 필터 실패: %s", e)
        if min_avg_volume > 0:
            vols = load_volumes(tickers, market, start, end, db_path)
            if not vols.empty:
                vols = vols.reindex(columns=prices.columns)
                masks.append(vols.rolling(20).mean() >= min_avg_volume)
                log.info("[QualityFilter] 거래량 필터 적용 (20일 평균 >= %.0f주)", min_avg_volume)
        if masks:
            quality_mask = masks[0]
            for m in masks[1:]:
                quality_mask = quality_mask & m

    results: dict[str, BacktestResult] = {}

    for lb in lookbacks:
        log.info("[MeanReversion-%dd] 스코어 계산 중...", lb)
        scores = prices.pct_change(lb).shift(1)

        # 퀄리티 필터 적용: 조건 미충족 종목 스코어 → NaN
        if quality_mask is not None:
            scores = scores.where(quality_mask)

        equity, trades = _run_cross_sectional(
            scores, prices, top_k, rebalance_freq, cost, regime,
            reverse=True, vol_scale=vol_scale, stop_loss=stop_loss,
        )
        key = f"MeanRev-{lb}d"
        r = BacktestResult(
            strategy=key,
            market=market,
            equity=equity,
            trades=trades,
            params={
                "lookback": lb, "top_k": top_k,
                "reverse": True, "market_filter": market_filter,
                "vol_scale": vol_scale, "stop_loss": stop_loss,
            },
        )
        r.print_summary()
        results[key] = r

    return results


# ─────────────────────────────────────────
# 포트폴리오 조합
# ─────────────────────────────────────────
def combine_strategies(
    result_a: BacktestResult,
    result_b: BacktestResult,
    weight_a: float = 0.5,
    label: Optional[str] = None,
) -> BacktestResult:
    """두 BacktestResult의 equity curve를 가중합으로 결합"""
    eq_a = result_a.equity
    eq_b = result_b.equity
    common = eq_a.index.intersection(eq_b.index)
    a = (eq_a.reindex(common) / eq_a.reindex(common).iloc[0])
    b = (eq_b.reindex(common) / eq_b.reindex(common).iloc[0])
    combined_eq = weight_a * a + (1 - weight_a) * b
    name = label or f"Portfolio({result_a.strategy}+{result_b.strategy})"
    return BacktestResult(
        strategy=name,
        market=result_a.market,
        equity=combined_eq,
        trades=pd.DataFrame(),
        params={"weight_a": weight_a,
                "a": result_a.strategy, "b": result_b.strategy},
    )


def run_portfolio_combo(
    market: str = "KR",
    tickers: Optional[list[str]] = None,
    top_k: int = 20,
    cost: float = DEFAULT_COST,
    train_start: str = TRAIN_START,
    train_end: str = TRAIN_END,
    val_start: str = VAL_START,
    val_end: str = VAL_END,
    db_path: Path = DB_PATH,
):
    """MeanRev-60d(필터 없음) + Momentum-60d 50:50 조합 비교"""
    W = 76
    for label, s, e in [("학습", train_start, train_end), ("검증", val_start, val_end)]:
        kw = dict(market=market, tickers=tickers, start=s, end=e, cost=cost, db_path=db_path)
        mom = strategy_momentum_enhanced(**kw, lookbacks=[60], top_k=top_k)["Momentum-60d"]
        rev = strategy_mean_reversion(**kw, lookbacks=[60], top_k=top_k,
                                      market_filter=False)["MeanRev-60d"]
        combo = combine_strategies(mom, rev, weight_a=0.5)
        combo.print_summary()

        print(f"\n{'─'*W}")
        print(f"  [{label}] 개별 vs 조합 비교")
        print(f"{'─'*W}")
        print(f"  {'전략':<28} {'총수익률':>10} {'MDD':>8} {'Sharpe':>8}")
        print(f"{'─'*W}")
        for r in [mom, rev, combo]:
            s2 = r.summary()
            print(f"  {s2['strategy']:<28} {s2['total_return']:>10} {s2['mdd']:>8} {s2['sharpe']:>8}")
        print(f"{'─'*W}\n")


# ─────────────────────────────────────────
# 기간별 실행
# ─────────────────────────────────────────
def _run_period(
    market, tickers, start, end, top_k, cost, db_path,
    include_mean_reversion: bool = False,
) -> dict[str, BacktestResult]:
    kw = dict(market=market, tickers=tickers, start=start, end=end, cost=cost, db_path=db_path)
    results = {}
    # Momentum 3종 (20/60/120일)
    results.update(strategy_momentum_enhanced(**kw, top_k=top_k))
    # Factor Combo
    results["Factor Combo"] = strategy_factor_combo(**kw, top_k=top_k)
    # Mean Reversion (선택)
    if include_mean_reversion:
        results.update(strategy_mean_reversion(**kw, top_k=top_k))
    return results


def _print_table(results: dict, label: str, market: str, start: str, end: str):
    W = 76
    print("\n" + "=" * W)
    print(f"  [{label}]  {market}  |  {start} ~ {end}  |  비용 0.2%")
    print("=" * W)
    print(f"  {'전략':<18} {'총수익률':>10} {'CAGR':>8} {'MDD':>8} {'Sharpe':>8} {'거래수':>7}")
    print("-" * W)
    for r in results.values():
        s = r.summary()
        print(f"  {s['strategy']:<18} {s['total_return']:>10} {s['cagr']:>8} "
              f"{s['mdd']:>8} {s['sharpe']:>8} {s['trades']:>7}")
    print("=" * W)


# ─────────────────────────────────────────
# 전체 실행 (학습/검증 분리)
# ─────────────────────────────────────────
def run_all(
    market: str = "KR",
    tickers: Optional[list[str]] = None,
    top_k: int = 20,
    cost: float = DEFAULT_COST,
    train_start: str = TRAIN_START,
    train_end: str = TRAIN_END,
    val_start: str = VAL_START,
    val_end: str = VAL_END,
    db_path: Path = DB_PATH,
    include_mean_reversion: bool = False,
) -> dict[str, dict[str, BacktestResult]]:
    """학습(2020~2022) / 검증(2023~2024) 기간 분리 실행"""

    log.info("=== 학습 기간: %s ~ %s ===", train_start, train_end)
    train = _run_period(market, tickers, train_start, train_end, top_k, cost, db_path,
                        include_mean_reversion)

    log.info("=== 검증 기간: %s ~ %s ===", val_start, val_end)
    val = _run_period(market, tickers, val_start, val_end, top_k, cost, db_path,
                      include_mean_reversion)

    _print_table(train, "학습 2020~2022", market, train_start, train_end)
    _print_table(val,   "검증 2023~2024", market, val_start,   val_end)

    # 학습 vs 검증 비교
    W = 76
    print("\n" + "=" * W)
    print("  [학습 vs 검증 비교]  총수익률 / MDD / Sharpe")
    print("=" * W)
    print(f"  {'전략':<18} {'학습수익':>10} {'검증수익':>10} "
          f"{'학습MDD':>9} {'검증MDD':>9} {'학습Sharpe':>11} {'검증Sharpe':>11}")
    print("-" * W)
    for key in train:
        if key not in val:
            continue
        ts, vs = train[key].summary(), val[key].summary()
        print(f"  {ts['strategy']:<18} {ts['total_return']:>10} {vs['total_return']:>10} "
              f"{ts['mdd']:>9} {vs['mdd']:>9} {ts['sharpe']:>11} {vs['sharpe']:>11}")
    print("=" * W)

    # CSV 저장
    csv_rows = (
        [r.to_csv_row("train") for r in train.values()] +
        [r.to_csv_row("val")   for r in val.values()]
    )
    df_csv = pd.DataFrame(csv_rows)
    csv_path = db_path.parent / CSV_PATH
    if csv_path.exists():
        df_csv = pd.concat([pd.read_csv(csv_path), df_csv], ignore_index=True)
    df_csv.to_csv(csv_path, index=False, encoding="utf-8-sig")
    log.info("결과 저장 → %s", csv_path.resolve())

    return {"train": train, "val": val}


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="전략 백테스트 엔진")
    parser.add_argument("--market",       default="KR",  choices=["KR", "US"])
    parser.add_argument("--strategy",     default="all",
                        choices=["all", "momentum", "factor_combo", "mean_reversion", "portfolio_combo"])
    parser.add_argument("--tickers",      nargs="*", help="종목 리스트 (기본: 전체)")
    parser.add_argument("--train-start",  default=TRAIN_START)
    parser.add_argument("--train-end",    default=TRAIN_END)
    parser.add_argument("--val-start",    default=VAL_START)
    parser.add_argument("--val-end",      default=VAL_END)
    parser.add_argument("--top-k",        type=int,   default=20)
    parser.add_argument("--cost",         type=float, default=DEFAULT_COST)
    parser.add_argument("--no-vol-filter",    action="store_true", help="거래량 필터 비활성")
    parser.add_argument("--no-market-filter", action="store_true", help="시장 필터 비활성")
    parser.add_argument("--mean-reversion",   action="store_true", help="all 실행 시 MeanReversion 포함")
    parser.add_argument("--lookbacks",        nargs="+", type=int, help="lookback 일수 (예: 60 또는 5 10 20 60)")
    parser.add_argument("--vol-scale",        action="store_true", help="역변동성 가중 포지션 사이징")
    parser.add_argument("--stop-loss",        type=float, default=0.0,   help="손절 비율 (예: 0.10 = -10%%)")
    parser.add_argument("--min-market-cap",   type=float, default=0.0,   help="최소 시가총액 (원, 예: 100000000000 = 1000억)")
    parser.add_argument("--min-avg-volume",   type=float, default=0.0,   help="최소 20일 평균 거래량 (주, 예: 100000)")
    parser.add_argument("--db",           default=str(DB_PATH))
    args = parser.parse_args()

    db = Path(args.db)
    base_kw = dict(market=args.market, tickers=args.tickers, cost=args.cost, db_path=db)

    if args.strategy == "all":
        run_all(**base_kw, top_k=args.top_k,
                train_start=args.train_start, train_end=args.train_end,
                val_start=args.val_start, val_end=args.val_end,
                include_mean_reversion=args.mean_reversion)

    elif args.strategy == "momentum":
        for period, s, e in [("학습", args.train_start, args.train_end),
                              ("검증", args.val_start,   args.val_end)]:
            print(f"\n▶ {period} 기간: {s} ~ {e}")
            strategy_momentum_enhanced(
                **base_kw, start=s, end=e, top_k=args.top_k,
                vol_filter=not args.no_vol_filter,
                market_filter=not args.no_market_filter,
            )

    elif args.strategy == "factor_combo":
        for period, s, e in [("학습", args.train_start, args.train_end),
                              ("검증", args.val_start,   args.val_end)]:
            print(f"\n▶ {period} 기간: {s} ~ {e}")
            strategy_factor_combo(**base_kw, start=s, end=e, top_k=args.top_k,
                                  market_filter=not args.no_market_filter)

    elif args.strategy == "mean_reversion":
        lbs = args.lookbacks or [5, 10, 20, 60]
        for period, s, e in [("학습", args.train_start, args.train_end),
                              ("검증", args.val_start,   args.val_end)]:
            print(f"\n▶ {period} 기간: {s} ~ {e}")
            strategy_mean_reversion(**base_kw, start=s, end=e, top_k=args.top_k,
                                    lookbacks=lbs,
                                    market_filter=not args.no_market_filter,
                                    vol_scale=args.vol_scale,
                                    stop_loss=args.stop_loss,
                                    min_market_cap=args.min_market_cap,
                                    min_avg_volume=args.min_avg_volume)

    elif args.strategy == "portfolio_combo":
        run_portfolio_combo(**base_kw, top_k=args.top_k,
                            train_start=args.train_start, train_end=args.train_end,
                            val_start=args.val_start, val_end=args.val_end)
