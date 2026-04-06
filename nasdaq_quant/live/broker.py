"""
live/broker.py
Alpaca paper/live 주문 실행 래퍼
"""
import logging
import time
from dataclasses import dataclass, field

from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce

log = logging.getLogger(__name__)


@dataclass
class OrderResult:
    ok: bool
    order_id: str = ""
    error: str = ""


@dataclass
class FillInfo:
    avg_price:  float | None = None
    filled_qty: int   | None = None
    status:     str          = "unknown"  # filled / partially_filled / pending


class AlpacaBroker:
    def __init__(self, api_key: str, secret_key: str, paper: bool = True):
        self._client = TradingClient(api_key, secret_key, paper=paper)

    # ── 시장 상태 ──────────────────────────────────
    def is_market_open(self) -> bool:
        try:
            return self._client.get_clock().is_open
        except Exception as e:
            log.warning("시장 상태 확인 실패: %s", e)
            return False

    # ── 계좌 ───────────────────────────────────────
    def get_equity(self) -> float:
        return float(self._client.get_account().equity)

    def get_positions(self) -> dict[str, float]:
        """현재 보유 포지션 {ticker: qty}"""
        return {p.symbol: float(p.qty) for p in self._client.get_all_positions()}

    # ── 주문 ───────────────────────────────────────
    def buy(self, ticker: str, shares: int) -> OrderResult:
        return self._order(ticker, shares, OrderSide.BUY)

    def sell(self, ticker: str, shares: int) -> OrderResult:
        return self._order(ticker, shares, OrderSide.SELL)

    def close_all(self) -> None:
        """전체 포지션 즉시 청산"""
        try:
            self._client.close_all_positions(cancel_orders=True)
            log.info("전체 포지션 청산 요청 완료")
        except Exception as e:
            log.error("전체 청산 실패: %s", e)

    def get_fill_price(self, order_id: str, wait_sec: float = 2.0) -> float | None:
        """주문 체결가 조회. 미체결 시 None 반환."""
        return self.get_fill_info(order_id, wait_sec).avg_price

    def get_fill_info(self, order_id: str, wait_sec: float = 2.0) -> FillInfo:
        """체결가 + 체결수량 + 상태 한 번에 조회."""
        time.sleep(wait_sec)
        try:
            order = self._client.get_order_by_id(order_id)
            price  = float(order.filled_avg_price) if order.filled_avg_price else None
            qty    = int(float(order.filled_qty))  if order.filled_qty        else None
            status = str(order.status)             if hasattr(order, "status") else "unknown"
            return FillInfo(avg_price=price, filled_qty=qty, status=status)
        except Exception as e:
            log.warning("체결 정보 조회 실패 (order_id=%s): %s", order_id, e)
            return FillInfo()

    def _order(self, ticker: str, shares: int, side: OrderSide) -> OrderResult:
        try:
            req = MarketOrderRequest(
                symbol=ticker,
                qty=shares,
                side=side,
                time_in_force=TimeInForce.DAY,
            )
            order = self._client.submit_order(req)
            log.info("[주문] %s %s %d주 (id=%s)", side.value.upper(), ticker, shares, order.id)
            return OrderResult(ok=True, order_id=str(order.id))
        except Exception as e:
            log.error("[주문 실패] %s %s %d주: %s", side.value.upper(), ticker, shares, e)
            return OrderResult(ok=False, error=str(e))
