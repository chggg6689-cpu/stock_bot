"""
features/builder.py
FeatureBuilder — OR 피처, 장중 피처, 시장 레짐 계산
"""
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)
ET = ZoneInfo("America/New_York")


class FeatureBuilder:
    """
    1분봉 DataFrame으로부터 ORB 전략에 필요한 피처를 계산한다.

    OR 거래량 기준: 09:30 첫 바는 오픈 스파이크로 왜곡이 심하므로 제외.
    """

    # ─────────────────────────────────────────
    # Opening Range 피처
    # ─────────────────────────────────────────
    def build_or_features(self, bars_1min: pd.DataFrame) -> dict:
        """
        09:30~09:34 (5개 바) 기준 OR 피처 반환.

        반환 dict 키:
          or_high     : OR 구간 최고가
          or_low      : OR 구간 최저가
          or_range    : (or_high - or_low) / or_low  (비율)
          or_vol_avg  : OR 평균 거래량 (09:30 첫 바 제외)
          or_open     : 09:30 첫 바 시가 (갭 계산용)
          valid       : OR 피처가 유효한지 여부 (bool)
        """
        or_bars = bars_1min.between_time("09:30", "09:34")
        if or_bars.empty:
            return self._empty_or_features()

        or_high = float(or_bars["High"].max())
        or_low  = float(or_bars["Low"].min())
        or_range = (or_high - or_low) / or_low if or_low > 0 else 0.0

        # 첫 바 제외 → 오픈 스파이크 왜곡 방지
        vol_ref  = or_bars.iloc[1:] if len(or_bars) > 1 else or_bars
        or_vol_avg = float(vol_ref["Volume"].mean()) if not vol_ref.empty else 0.0

        or_open  = float(or_bars.iloc[0]["Open"])
        or_close = float(or_bars.iloc[-1]["Close"])   # OR 마지막 바 종가 (방향 판단용)

        return {
            "or_high":    or_high,
            "or_low":     or_low,
            "or_range":   or_range,
            "or_vol_avg": or_vol_avg,
            "or_open":    or_open,
            "or_close":   or_close,
            "valid":      or_high > 0 and or_low > 0,
        }

    # ─────────────────────────────────────────
    # 장중 실시간 피처
    # ─────────────────────────────────────────
    def build_intraday_features(
        self,
        bars_1min: pd.DataFrame,
        ts: datetime,
        prev_close: float = 0.0,
    ) -> dict:
        """
        ts 시점까지의 장중 피처 반환.

        반환 dict 키:
          vwap        : 09:30~ts 까지의 VWAP
          gap_pct     : (or_open - prev_close) / prev_close  (전일 대비 갭)
          cum_volume  : 09:30~ts 누적 거래량
        """
        bars_so_far = bars_1min[bars_1min.index <= ts]
        session = bars_so_far.between_time("09:30", "16:00")

        if session.empty:
            return {"vwap": 0.0, "gap_pct": 0.0, "cum_volume": 0}

        vwap = self._calc_vwap(session)
        cum_vol = int(session["Volume"].sum())

        or_feats = self.build_or_features(bars_1min)
        gap_pct = 0.0
        if prev_close > 0 and or_feats["valid"]:
            gap_pct = (or_feats["or_open"] - prev_close) / prev_close

        return {
            "vwap":       vwap,
            "gap_pct":    gap_pct,
            "cum_volume": cum_vol,
        }

    # ─────────────────────────────────────────
    # 시장 레짐 (QQQ / SPY 공용)
    # ─────────────────────────────────────────
    def market_regime(self, index_bars: pd.DataFrame, ticker: str = "QQQ") -> str:
        """
        당일 QQQ(또는 SPY) 1분봉 기준 시장 레짐 판단.
        OR 오픈 대비 현재 종가 변화율로 판정.

        Returns:
          "bull"    : OR 오픈 대비 +0.5% 이상
          "bear"    : OR 오픈 대비 -0.5% 이하
          "neutral" : 그 외
        """
        if index_bars is None or index_bars.empty:
            return "neutral"

        or_bars = index_bars.between_time("09:30", "09:34")
        if or_bars.empty:
            return "neutral"

        open_price = float(or_bars.iloc[0]["Open"])
        last_close = float(index_bars.iloc[-1]["Close"])

        if open_price <= 0:
            return "neutral"

        chg = (last_close - open_price) / open_price
        if chg >= 0.005:
            return "bull"
        if chg <= -0.005:
            return "bear"
        return "neutral"

    def qqq_regime_features(self, qqq_bars: pd.DataFrame) -> dict:
        """
        QQQ 1분봉 기반 레짐 피처 세트.

        반환 dict 키:
          qqq_regime      : "bull" / "bear" / "neutral"
          qqq_or_range    : QQQ OR 범위 (%)
          qqq_chg_pct     : OR 오픈 대비 현재 등락률
          qqq_above_vwap  : 현재 QQQ 종가 > QQQ VWAP 여부 (bool)
        """
        regime = self.market_regime(qqq_bars, ticker="QQQ")

        if qqq_bars is None or qqq_bars.empty:
            return {
                "qqq_regime":     "neutral",
                "qqq_or_range":   0.0,
                "qqq_chg_pct":    0.0,
                "qqq_above_vwap": False,
            }

        or_bars = qqq_bars.between_time("09:30", "09:34")
        or_high = float(or_bars["High"].max()) if not or_bars.empty else 0.0
        or_low  = float(or_bars["Low"].min())  if not or_bars.empty else 0.0
        or_range = (or_high - or_low) / or_low if or_low > 0 else 0.0

        open_price = float(or_bars.iloc[0]["Open"]) if not or_bars.empty else 0.0
        last_close = float(qqq_bars.iloc[-1]["Close"])
        chg_pct = (last_close - open_price) / open_price if open_price > 0 else 0.0

        session = qqq_bars.between_time("09:30", "16:00")
        vwap = self._calc_vwap(session) if not session.empty else 0.0
        above_vwap = last_close > vwap if vwap > 0 else False

        return {
            "qqq_regime":     regime,
            "qqq_or_range":   or_range,
            "qqq_chg_pct":    chg_pct,
            "qqq_above_vwap": above_vwap,
        }

    # ─────────────────────────────────────────
    # Intraday Relative Strength vs QQQ
    # ─────────────────────────────────────────
    def intraday_rs(
        self,
        ticker_bars: pd.DataFrame,
        qqq_bars: pd.DataFrame,
        ts: datetime | None = None,
    ) -> dict:
        """
        종목의 장중 상대 강도(Relative Strength) vs QQQ.

        RS = (종목 등락률) - (QQQ 등락률)  (OR 오픈 기준)

        반환 dict 키:
          rs_vs_qqq     : 종목 RS (양수 = 시장 대비 강함)
          ticker_chg    : 종목 OR 오픈 대비 등락률
          qqq_chg       : QQQ OR 오픈 대비 등락률
          is_leader     : RS > 0 (시장 선도 여부)
        """
        empty = {"rs_vs_qqq": 0.0, "ticker_chg": 0.0, "qqq_chg": 0.0, "is_leader": False}

        if ticker_bars is None or ticker_bars.empty:
            return empty
        if qqq_bars is None or qqq_bars.empty:
            return empty

        cutoff = ts or ticker_bars.index[-1]

        def _chg(bars: pd.DataFrame) -> float:
            or_b = bars.between_time("09:30", "09:34")
            if or_b.empty:
                return 0.0
            open_p = float(or_b.iloc[0]["Open"])
            bars_to_ts = bars[bars.index <= cutoff]
            if bars_to_ts.empty or open_p <= 0:
                return 0.0
            return (float(bars_to_ts.iloc[-1]["Close"]) - open_p) / open_p

        ticker_chg = _chg(ticker_bars)
        qqq_chg    = _chg(qqq_bars)
        rs         = ticker_chg - qqq_chg

        return {
            "rs_vs_qqq":  rs,
            "ticker_chg": ticker_chg,
            "qqq_chg":    qqq_chg,
            "is_leader":  rs > 0,
        }

    # ─────────────────────────────────────────
    # 전일 OHLCV 피처
    # ─────────────────────────────────────────
    def prev_day_features(self, high: float, low: float, close: float, volume: float) -> dict:
        """
        전일 일봉 기반 피처.

        반환 dict 키:
          prev_hl_pct   : (high - low) / close
          prev_vol_score: prev_hl_pct × log10(volume)
        """
        import math
        if close <= 0 or low <= 0:
            return {"prev_hl_pct": 0.0, "prev_vol_score": 0.0}

        prev_hl_pct   = (high - low) / close
        prev_vol_score = prev_hl_pct * math.log10(max(volume, 1))
        return {
            "prev_hl_pct":    prev_hl_pct,
            "prev_vol_score": prev_vol_score,
        }

    # ─────────────────────────────────────────
    # 내부 유틸
    # ─────────────────────────────────────────
    @staticmethod
    def _calc_vwap(bars: pd.DataFrame) -> float:
        """Typical price × Volume 기반 VWAP 계산."""
        typical = (bars["High"] + bars["Low"] + bars["Close"]) / 3
        vol = bars["Volume"].replace(0, np.nan)
        if vol.isna().all():
            return float(bars["Close"].iloc[-1])
        return float((typical * bars["Volume"]).sum() / bars["Volume"].sum())

    @staticmethod
    def _empty_or_features() -> dict:
        return {
            "or_high":    0.0,
            "or_low":     0.0,
            "or_range":   0.0,
            "or_vol_avg": 0.0,
            "or_open":    0.0,
            "valid":      False,
        }
