"""
kr_trader.py
KIS (한국투자증권) 모의투자 — MeanReversion-60d 전략

- 매주 월요일 09:05 KST 리밸런싱
- SQLite DB에서 가격 로딩 -> 60일 낙폭 과대 종목 선정
- 역변동성 가중 | Top-20 | 거래량 필터
- Telegram 알림
"""

import os
import time
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import numpy as np
import pandas as pd
import requests
import schedule
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

# ── 설정 ──────────────────────────────────────────────────────────────
KIS_APP_KEY    = os.getenv("KIS_APP_KEY", "")
KIS_APP_SECRET = os.getenv("KIS_APP_SECRET", "")
KIS_ACCOUNT_NO = os.getenv("KIS_ACCOUNT_NO", "")
TELEGRAM_TOKEN   = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

KST        = ZoneInfo("Asia/Seoul")
DB_PATH    = Path(__file__).parent / "market_data.db"
LOOKBACK   = 60
TOP_K      = 20
MIN_VOLUME = 50_000
REBAL_TIME = "09:05"
PAPER      = True

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ── Telegram ──────────────────────────────────────────────────────────
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token   = token
        self.chat_id = chat_id
        self.enabled = bool(token and chat_id)

    def send(self, msg: str) -> bool:
        if not self.enabled:
            log.warning("[Telegram] 미설정 - 스킵")
            return False
        try:
            resp = requests.post(
                f"https://api.telegram.org/bot{self.token}/sendMessage",
                json={"chat_id": self.chat_id, "text": msg, "parse_mode": "Markdown"},
                timeout=10,
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            log.error("[Telegram] 전송 실패: %s", e)
            return False


# ── KIS API Client ─────────────────────────────────────────────────────
class KISClient:
    BASE_PAPER = "https://openapivts.koreainvestment.com:29443"
    BASE_REAL  = "https://openapi.koreainvestment.com:9443"

    def __init__(self, app_key: str, app_secret: str, account_no: str, paper: bool = True):
        self.app_key        = app_key
        self.app_secret     = app_secret
        self.account_no     = account_no
        self.account_suffix = "01"
        self.base_url       = self.BASE_PAPER if paper else self.BASE_REAL
        self.paper          = paper
        self._token: str | None = None
        self._token_exp: datetime | None = None

    # ── 토큰 ────────────────────────────────────────────────────────
    def _get_token(self) -> str:
        if self._token and self._token_exp and datetime.now() < self._token_exp:
            return self._token
        resp = requests.post(
            f"{self.base_url}/oauth2/tokenP",
            json={
                "grant_type": "client_credentials",
                "appkey": self.app_key,
                "appsecret": self.app_secret,
            },
            headers={"content-type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()
        self._token = data["access_token"]
        self._token_exp = datetime.now() + timedelta(
            seconds=data.get("expires_in", 86400) - 300
        )
        log.info("[KIS] 토큰 발급 완료 (만료: %s)", self._token_exp.strftime("%H:%M"))
        return self._token

    def _h(self, tr_id: str) -> dict:
        return {
            "content-type": "application/json",
            "authorization": f"Bearer {self._get_token()}",
            "appkey": self.app_key,
            "appsecret": self.app_secret,
            "tr_id": tr_id,
            "custtype": "P",
        }

    # ── 잔고 조회 ────────────────────────────────────────────────────
    def get_balance(self) -> tuple[float, dict[str, int]]:
        """(예수금, {종목코드: 보유수량}) 반환"""
        tr_id = "VTTC8434R" if self.paper else "TTTC8434R"
        params = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_suffix,
            "AFHR_FLPR_YN": "N",
            "OFL_YN": "",
            "INQR_DVSN": "01",
            "UNPR_DVSN": "01",
            "FUND_STTL_ICLD_YN": "N",
            "FNCG_AMT_AUTO_RDPT_YN": "N",
            "PRCS_DVSN": "01",
            "CTX_AREA_FK100": "",
            "CTX_AREA_NK100": "",
        }
        resp = requests.get(
            f"{self.base_url}/uapi/domestic-stock/v1/trading/inquire-balance",
            headers=self._h(tr_id),
            params=params,
            timeout=10,
        )
        resp.raise_for_status()
        data = resp.json()

        cash = float(data["output2"][0].get("dnca_tot_amt", 0))
        holdings = {
            item["pdno"]: int(item["hldg_qty"])
            for item in data.get("output1", [])
            if int(item.get("hldg_qty", 0)) > 0
        }
        return cash, holdings

    # ── 현재가 ──────────────────────────────────────────────────────
    def get_price(self, ticker: str) -> float | None:
        try:
            resp = requests.get(
                f"{self.base_url}/uapi/domestic-stock/v1/quotations/inquire-price",
                headers=self._h("FHKST01010100"),
                params={"FID_COND_MRKT_DIV_CODE": "J", "FID_INPUT_ISCD": ticker},
                timeout=10,
            )
            resp.raise_for_status()
            return float(resp.json()["output"]["stck_prpr"])
        except Exception as e:
            log.error("[KIS] %s 현재가 조회 실패: %s", ticker, e)
            return None

    # ── 매수 ────────────────────────────────────────────────────────
    def buy(self, ticker: str, qty: int) -> bool:
        tr_id = "VTTC0802U" if self.paper else "TTTC0802U"
        body = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_suffix,
            "PDNO": ticker,
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0",
        }
        try:
            resp = requests.post(
                f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash",
                headers=self._h(tr_id),
                json=body,
                timeout=10,
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("rt_cd") == "0":
                log.info("[매수 완료] %s %d주", ticker, qty)
                return True
            log.error("[매수 실패] %s: %s", ticker, result.get("msg1", ""))
            return False
        except Exception as e:
            log.error("[매수 오류] %s: %s", ticker, e)
            return False

    # ── 매도 ────────────────────────────────────────────────────────
    def sell(self, ticker: str, qty: int) -> bool:
        tr_id = "VTTC0801U" if self.paper else "TTTC0801U"
        body = {
            "CANO": self.account_no,
            "ACNT_PRDT_CD": self.account_suffix,
            "PDNO": ticker,
            "ORD_DVSN": "01",  # 시장가
            "ORD_QTY": str(qty),
            "ORD_UNPR": "0",
        }
        try:
            resp = requests.post(
                f"{self.base_url}/uapi/domestic-stock/v1/trading/order-cash",
                headers=self._h(tr_id),
                json=body,
                timeout=10,
            )
            resp.raise_for_status()
            result = resp.json()
            if result.get("rt_cd") == "0":
                log.info("[매도 완료] %s %d주", ticker, qty)
                return True
            log.error("[매도 실패] %s: %s", ticker, result.get("msg1", ""))
            return False
        except Exception as e:
            log.error("[매도 오류] %s: %s", ticker, e)
            return False


# ── MeanReversion-60d 시그널 ──────────────────────────────────────────
class MeanReversionSignal:
    def __init__(self, lookback: int = 60, top_k: int = 20, min_volume: int = 50_000):
        self.lookback   = lookback
        self.top_k      = top_k
        self.min_volume = min_volume

    def _load_prices(self) -> pd.DataFrame:
        """SQLite DB에서 최근 가격 로딩"""
        if not DB_PATH.exists():
            raise FileNotFoundError(f"DB 없음: {DB_PATH} — data_collector.py 먼저 실행")
        cutoff = (
            datetime.now(KST) - timedelta(days=int(self.lookback * 2))
        ).strftime("%Y-%m-%d")
        con = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(
            "SELECT ticker, date, close, volume FROM kr_ohlcv WHERE date >= ? ORDER BY date",
            con,
            params=(cutoff,),
        )
        con.close()
        log.info("[Signal] DB 로딩 완료: %d행", len(df))
        return df

    def get_signals(self) -> pd.DataFrame:
        """60일 낙폭 과대 종목 선정 + 역변동성 가중치"""
        raw = self._load_prices()
        if raw.empty:
            return pd.DataFrame()

        pivot     = raw.pivot(index="date", columns="ticker", values="close").sort_index().ffill()
        vol_pivot = raw.pivot(index="date", columns="ticker", values="volume").sort_index().ffill()

        results = []
        for ticker in pivot.columns:
            prices  = pivot[ticker].dropna()
            volumes = vol_pivot[ticker].dropna()
            if len(prices) < self.lookback:
                continue
            avg_vol = float(volumes.iloc[-20:].mean()) if len(volumes) >= 20 else 0.0
            if avg_vol < self.min_volume:
                continue
            ret = float(prices.iloc[-1] / prices.iloc[-self.lookback] - 1)
            daily_rets = prices.pct_change().iloc[-self.lookback:]
            vol = float(daily_rets.std() * np.sqrt(252))
            results.append({
                "ticker":     ticker,
                "return_60d": ret,
                "volatility": max(vol, 0.01),
                "avg_volume": avg_vol,
            })

        if not results:
            log.warning("[Signal] 조건 충족 종목 없음")
            return pd.DataFrame()

        df = pd.DataFrame(results).set_index("ticker")
        # 낙폭 과대 순 정렬 (return_60d 낮은 순)
        df = df.sort_values("return_60d").head(self.top_k)
        # 역변동성 가중
        df["inv_vol"] = 1.0 / df["volatility"]
        df["weight"]  = df["inv_vol"] / df["inv_vol"].sum()

        log.info("[Signal] 선정 종목 %d개:", len(df))
        for t, row in df.iterrows():
            log.info("  %s  60d=%+.1f%%  vol=%.2f  weight=%.1f%%",
                     t, row["return_60d"] * 100, row["volatility"], row["weight"] * 100)
        return df


# ── KR Trader ─────────────────────────────────────────────────────────
class KRTrader:
    def __init__(
        self,
        client:   KISClient,
        signal:   MeanReversionSignal,
        notifier: TelegramNotifier,
    ):
        self.client   = client
        self.signal   = signal
        self.notifier = notifier

    def rebalance(self) -> None:
        now = datetime.now(KST)
        log.info("===== KR 리밸런싱 시작 (%s) =====", now.strftime("%Y-%m-%d %H:%M"))

        # 1. 시그널 계산
        targets = self.signal.get_signals()
        if targets.empty:
            msg = "KR 리밸런싱: 시그널 없음 - 스킵"
            log.warning(msg)
            self.notifier.send(msg)
            return

        # 2. 잔고 조회
        cash, holdings = self.client.get_balance()
        total_equity = cash
        for ticker, qty in holdings.items():
            price = self.client.get_price(ticker)
            if price:
                total_equity += price * qty
        log.info("총 자산: %,.0f원 | 예수금: %,.0f원", total_equity, cash)

        target_tickers  = set(targets.index)
        current_tickers = set(holdings.keys())

        # 3. 매도 (목표 외 종목)
        exits = current_tickers - target_tickers
        for ticker in exits:
            qty = holdings[ticker]
            log.info("[매도] %s %d주", ticker, qty)
            if self.client.sell(ticker, qty):
                self.notifier.send(f"매도 `{ticker}` {qty}주")

        if exits:
            time.sleep(3)
            cash, _ = self.client.get_balance()

        # 4. 매수 (역변동성 비중)
        for ticker, row in targets.iterrows():
            weight     = float(row["weight"])
            alloc      = total_equity * weight
            price      = self.client.get_price(ticker)
            if price is None or price <= 0:
                log.warning("[매수 스킵] %s 가격 조회 실패", ticker)
                continue
            target_qty  = int(alloc / price)
            current_qty = holdings.get(ticker, 0)
            delta       = target_qty - current_qty

            if delta > 0:
                if self.client.buy(ticker, delta):
                    self.notifier.send(
                        f"매수 `{ticker}` {delta}주 @ {price:,.0f}원\n"
                        f"60d: {row['return_60d']:+.1%} | 비중: {weight:.1%}"
                    )
            elif delta < 0:
                self.client.sell(ticker, abs(delta))

        # 5. 완료 리포트
        cash_f, holdings_f = self.client.get_balance()
        pos_lines = "\n".join(
            f"  `{t}`: {q}주" for t, q in holdings_f.items()
        ) or "  (없음)"
        summary = (
            f"KR 리밸런싱 완료\n"
            f"예수금: {cash_f:,.0f}원\n"
            f"보유 종목:\n{pos_lines}\n"
            f"{now.strftime('%Y-%m-%d %H:%M KST')}"
        )
        log.info(summary)
        self.notifier.send(summary)


# ── 스케줄러 ──────────────────────────────────────────────────────────
def run_scheduler(trader: KRTrader) -> None:
    log.info("스케줄러 시작 - 매주 월요일 %s KST 리밸런싱", REBAL_TIME)
    trader.notifier.send(
        f"KR Trader 시작\n"
        f"전략: MeanReversion-{LOOKBACK}d | Top-{TOP_K} | 역변동성 가중\n"
        f"매주 월요일 {REBAL_TIME} KST"
    )
    schedule.every().monday.at(REBAL_TIME).do(trader.rebalance)
    while True:
        schedule.run_pending()
        time.sleep(30)


# ── CLI ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="KIS Paper Trader - MeanReversion-60d")
    parser.add_argument("--dry-run",    action="store_true", help="시그널만 계산, 주문 없음")
    parser.add_argument("--run-now",    action="store_true", help="즉시 리밸런싱 실행")
    parser.add_argument("--top-k",      type=int, default=TOP_K,      help=f"상위 종목 수 (기본: {TOP_K})")
    parser.add_argument("--min-volume", type=int, default=MIN_VOLUME, help=f"최소 일평균 거래량 (기본: {MIN_VOLUME})")
    args = parser.parse_args()

    notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    signal   = MeanReversionSignal(
        lookback=LOOKBACK, top_k=args.top_k, min_volume=args.min_volume
    )
    client   = KISClient(KIS_APP_KEY, KIS_APP_SECRET, KIS_ACCOUNT_NO, paper=PAPER)
    trader   = KRTrader(client, signal, notifier)

    if args.dry_run:
        print("\n=== Dry Run: 시그널 계산 ===")
        targets = signal.get_signals()
        if targets.empty:
            print("시그널 없음 (DB 데이터 확인 필요)")
        else:
            print(targets[["return_60d", "volatility", "weight"]].to_string())
            print(f"\n선정 종목 ({len(targets)}개): {targets.index.tolist()}")
    elif args.run_now:
        trader.rebalance()
    else:
        run_scheduler(trader)
