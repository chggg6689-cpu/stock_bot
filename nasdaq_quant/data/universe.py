"""
data/universe.py
유니버스 선정 — 전일 HL% × log10(Volume) 스코어 상위 top_n 종목
"""
import math
import logging
from datetime import date

import pandas as pd

from config import (
    UNIVERSE,
    MIN_PRICE,
    MIN_AVG_VOLUME,
    TOP_N,
)
from nasdaq_quant.data.manager import DataManager

log = logging.getLogger(__name__)


def score_ticker(high: float, low: float, close: float, volume: float) -> float | None:
    """
    단일 종목 스코어 계산.
    score = (high - low) / close × log10(volume)
    필터 미통과 시 None 반환.
    """
    if close < MIN_PRICE or volume < MIN_AVG_VOLUME or low <= 0:
        return None
    hl_pct = (high - low) / close
    return hl_pct * math.log10(max(volume, 1))


def get_universe(
    for_date: date,
    top_n: int = TOP_N,
    dm: DataManager | None = None,
    universe: list[str] = UNIVERSE,
) -> list[str]:
    """
    for_date 기준 전일 OHLCV를 조회해 스코어 상위 top_n 종목 반환.

    Args:
        for_date: 거래 당일 날짜 (전일 데이터 기준 선정)
        top_n: 반환할 종목 수
        dm: DataManager 인스턴스 (None이면 내부 생성)
        universe: 후보 유니버스 리스트

    Returns:
        스코어 상위 종목 티커 리스트 (최대 top_n개)
    """
    if dm is None:
        dm = DataManager()

    prev_ohlcv = dm.get_prev_day_ohlcv(universe, for_date)
    if prev_ohlcv.empty:
        log.warning("%s: 전일 OHLCV 없음", for_date)
        return []

    scores: list[tuple[str, float]] = []
    for ticker, row in prev_ohlcv.iterrows():
        s = score_ticker(
            high=row["high"],
            low=row["low"],
            close=row["close"],
            volume=row["volume"],
        )
        if s is not None:
            scores.append((str(ticker), s))

    scores.sort(key=lambda x: x[1], reverse=True)
    selected = [t for t, _ in scores[:top_n]]
    log.info("%s 유니버스 선정: %s", for_date, selected)
    return selected


def validate_universe_input(prev_ohlcv: pd.DataFrame) -> list[str]:
    """
    전일 OHLCV DataFrame 스키마 검증.
    반환: 오류 메시지 리스트 (빈 리스트 = 정상)
    """
    errors = []
    required_cols = {"high", "low", "close", "volume"}
    missing = required_cols - set(prev_ohlcv.columns)
    if missing:
        errors.append(f"누락 컬럼: {missing}")
    if prev_ohlcv.index.name is None and len(prev_ohlcv) == 0:
        errors.append("데이터가 비어 있습니다")
    return errors
