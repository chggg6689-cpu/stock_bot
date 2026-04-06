"""
kiwoom_trader.py
키움 OpenAPI+ 기반 KR 자동매매 — MeanRev-60d 전략

전략: MeanReversion-60d | top_k=20 | 동일비중 | 필터 OFF
리밸런싱: 매월 첫째 월요일 09:05 KST
손절: 포트폴리오 고점 대비 -15% → 전량 현금화
데이터: market_data.db (data_collector.py 증분 업데이트)

요구사항:
  pip install PyQt5 pywin32 python-dotenv requests
  키움증권 영웅문 HTS 설치 + OpenAPI+ 사용 신청

모의투자 모드:
  로그인 창에서 "모의투자 서버" 선택 → 계좌번호 8xxxxxxxx 사용
  (코드 내에서 real/mock 강제 전환 불가 — 로그인 시 서버 선택으로 결정)

환경변수 (.env):
  KIWOOM_ACCOUNT   = 8xxxxxxxxx  # 모의투자 계좌번호
  TELEGRAM_TOKEN   = ...
  TELEGRAM_CHAT_ID = ...
"""

import os
import sys
import subprocess
import logging
from datetime import datetime, date
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from PyQt5.QtWidgets import QApplication
from PyQt5.QtCore import QTimer, QEventLoop
from PyQt5.QAxContainer import QAxWidget

# 로컬 모듈
sys.path.insert(0, str(Path(__file__).parent))
from strategy_engine import strategy_mean_reversion, DB_PATH

# ─────────────────────────────────────────
# 설정
# ─────────────────────────────────────────
load_dotenv(Path(__file__).parent / ".env")

ACCOUNT        = os.getenv("KIWOOM_ACCOUNT", "")
TELEGRAM_TOKEN = os.getenv("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "")

TOP_K          = 20
LOOKBACK       = 60
REBAL_HOUR     = 9
REBAL_MINUTE   = 5
STOP_LOSS_PCT  = 0.15      # 고점 대비 -15%
TRADE_COST     = 0.0015    # 수수료 0.15% (키움 모의투자 기준)

# 화면번호 (TR 요청별 구분)
SCR_BALANCE    = "2000"
SCR_ORDER_BUY  = "2001"
SCR_ORDER_SELL = "2002"
SCR_PRICE      = "2003"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)


# ─────────────────────────────────────────
# Telegram 알림
# ─────────────────────────────────────────
class TelegramNotifier:
    def __init__(self, token: str, chat_id: str):
        self.token    = token
        self.chat_id  = chat_id
        self.enabled  = bool(token and chat_id)

    def send(self, message: str) -> bool:
        if not self.enabled:
            log.warning("[Telegram] 미설정 — 건너뜀")
            return False
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        try:
            resp = requests.post(
                url,
                json={"chat_id": self.chat_id, "text": message, "parse_mode": "Markdown"},
                timeout=10,
            )
            resp.raise_for_status()
            return True
        except Exception as e:
            log.error("[Telegram] 전송 실패: %s", e)
            return False


# ─────────────────────────────────────────
# 키움 OpenAPI+ 래퍼
# ─────────────────────────────────────────
class KiwoomAPI:
    """
    키움 OpenAPI+ OCX 래퍼.
    TR 요청은 중첩 QEventLoop로 동기화.
    """

    OCX_ID = "KHOPENAPI.KHOpenAPICtrl.1"

    def __init__(self):
        self.ocx = QAxWidget(self.OCX_ID)
        self._tr_data: dict  = {}
        self._loop: Optional[QEventLoop] = None

        # 이벤트 연결
        self.ocx.OnEventConnect.connect(self._on_connect)
        self.ocx.OnReceiveTrData.connect(self._on_tr_data)
        self.ocx.OnReceiveMsg.connect(self._on_msg)
        self.ocx.OnReceiveChejanData.connect(self._on_chejan)

        self._connected = False
        self._connect_loop: Optional[QEventLoop] = None

    # ── 로그인 ──────────────────────────────
    def connect(self) -> bool:
        """로그인 창 표시 후 접속 완료까지 대기"""
        self._connect_loop = QEventLoop()
        self.ocx.dynamicCall("CommConnect()")
        self._connect_loop.exec_()
        return self._connected

    def _on_connect(self, err_code: int):
        if err_code == 0:
            self._connected = True
            log.info("[Kiwoom] 로그인 성공")
        else:
            log.error("[Kiwoom] 로그인 실패 (코드=%d)", err_code)
        if self._connect_loop and self._connect_loop.isRunning():
            self._connect_loop.exit()

    # ── 계좌 정보 ────────────────────────────
    def get_login_info(self, tag: str) -> str:
        return self.ocx.dynamicCall("GetLoginInfo(QString)", tag)

    def get_account(self) -> str:
        """환경변수 설정 계좌 우선, 없으면 첫 번째 계좌 반환"""
        if ACCOUNT:
            return ACCOUNT
        accounts = self.get_login_info("ACCNO").strip().rstrip(";").split(";")
        return accounts[0] if accounts else ""

    # ── TR 요청 공통 ─────────────────────────
    def _set_input(self, key: str, value: str):
        self.ocx.dynamicCall("SetInputValue(QString, QString)", key, value)

    def _comm_rq(self, rqname: str, trcode: str, next_yn: int, screen: str):
        return self.ocx.dynamicCall(
            "CommRqData(QString, QString, int, QString)",
            rqname, trcode, next_yn, screen,
        )

    def _get_comm_data(self, trcode: str, rqname: str, idx: int, item: str) -> str:
        return self.ocx.dynamicCall(
            "GetCommData(QString, QString, int, QString)",
            trcode, rqname, idx, item,
        ).strip()

    def _get_repeat_cnt(self, trcode: str, rqname: str) -> int:
        return self.ocx.dynamicCall(
            "GetRepeatCnt(QString, QString)", trcode, rqname
        )

    def _wait_loop(self):
        self._loop = QEventLoop()
        self._loop.exec_()

    def _exit_loop(self):
        if self._loop and self._loop.isRunning():
            self._loop.exit()

    def _on_tr_data(
        self, screen_no, rqname, trcode, record_name, next_yn,
        unused1, unused2, unused3, unused4,
    ):
        """TR 데이터 수신 공통 핸들러"""
        if rqname == "계좌잔고":
            self._parse_balance(trcode, rqname)
        elif rqname == "현재가조회":
            self._parse_price(trcode, rqname)
        self._exit_loop()

    def _on_msg(self, screen_no, rqname, trcode, msg):
        log.info("[Kiwoom] TR 메시지 [%s] %s", rqname, msg)

    def _on_chejan(self, gubun, item_cnt, fid_list):
        """체결/잔고 이벤트 (주문 체결 확인용)"""
        if gubun == "0":   # 주문 접수/체결
            code   = self.ocx.dynamicCall("GetChejanData(int)", 9001).strip().lstrip("A")
            qty    = self.ocx.dynamicCall("GetChejanData(int)", 911).strip()
            status = self.ocx.dynamicCall("GetChejanData(int)", 913).strip()
            log.info("[체결] 종목=%s 수량=%s 상태=%s", code, qty, status)

    # ── 잔고/포지션 조회 (opw00018) ──────────
    def _parse_balance(self, trcode: str, rqname: str):
        cnt = self._get_repeat_cnt(trcode, rqname)
        positions: dict[str, int] = {}
        for i in range(cnt):
            code = self._get_comm_data(trcode, rqname, i, "종목번호").lstrip("A")
            qty  = self._get_comm_data(trcode, rqname, i, "보유수량")
            if code and qty:
                try:
                    positions[code] = int(qty)
                except ValueError:
                    pass

        # 예수금/총평가 (단일행 레코드)
        cash = self._get_comm_data(trcode, rqname, 0, "예수금")
        eval_total = self._get_comm_data(trcode, rqname, 0, "총평가금액")
        self._tr_data["positions"]  = positions
        self._tr_data["cash"]       = int(cash.replace(",", "") or 0)
        self._tr_data["eval_total"] = int(eval_total.replace(",", "") or 0)

    def request_balance(self, acc: str) -> dict:
        """보유 포지션 + 계좌 평가 반환 → {"positions": {code: qty}, "cash": int, "eval_total": int}"""
        self._set_input("계좌번호", acc)
        self._set_input("비밀번호", "")
        self._set_input("비밀번호입력매체구분", "00")
        self._set_input("조회구분", "01")
        self._comm_rq("계좌잔고", "opw00018", 0, SCR_BALANCE)
        self._wait_loop()
        return self._tr_data.copy()

    # ── 현재가 조회 ──────────────────────────
    def _parse_price(self, trcode: str, rqname: str):
        raw = self._get_comm_data(trcode, rqname, 0, "현재가")
        try:
            self._tr_data["current_price"] = abs(int(raw.replace(",", "")))
        except ValueError:
            self._tr_data["current_price"] = 0

    def get_current_price(self, code: str) -> int:
        """opt10001 TR로 현재가 조회"""
        self._set_input("종목코드", code)
        self._comm_rq("현재가조회", "opt10001", 0, SCR_PRICE)
        self._wait_loop()
        return self._tr_data.get("current_price", 0)

    def get_price_fast(self, code: str) -> int:
        """마스터 데이터에서 직접 조회 (TR 없이, 실시간 갱신값)"""
        raw = self.ocx.dynamicCall("GetMasterLastPrice(QString)", code).strip()
        try:
            return abs(int(raw))
        except ValueError:
            return self.get_current_price(code)  # fallback

    # ── 주문 ─────────────────────────────────
    def send_order(
        self,
        acc: str,
        code: str,
        qty: int,
        side: str,          # "BUY" | "SELL"
        screen: str,
    ) -> bool:
        """
        시장가 주문 전송.
        order_type: 1=신규매수, 2=신규매도
        hoga_gb: "03"=시장가
        """
        order_type = 1 if side == "BUY" else 2
        ret = self.ocx.dynamicCall(
            "SendOrder(QString, QString, QString, int, QString, int, int, QString, QString)",
            "주문", screen, acc, order_type, code, qty, 0, "03", "",
        )
        if ret == 0:
            log.info("[주문] %s %s %d주 → 접수", side, code, qty)
            return True
        else:
            log.error("[주문] %s %s %d주 → 실패 (ret=%d)", side, code, qty, ret)
            return False


# ─────────────────────────────────────────
# MeanRev-60d 시그널 생성
# ─────────────────────────────────────────
class MeanRevSignal:
    """
    market_data.db 기반 MeanReversion-60d 시그널.
    실행 전 data_collector.py 증분 업데이트 자동 실행.
    """

    def __init__(self, top_k: int = TOP_K, lookback: int = LOOKBACK):
        self.top_k    = top_k
        self.lookback = lookback

    def update_db(self):
        """data_collector.py 증분 업데이트 실행"""
        log.info("[Signal] DB 증분 업데이트 시작...")
        script = Path(__file__).parent / "data_collector.py"
        try:
            result = subprocess.run(
                [sys.executable, str(script), "--mode", "incremental", "--market", "KR"],
                capture_output=True, text=True, timeout=600,
            )
            if result.returncode == 0:
                log.info("[Signal] DB 업데이트 완료")
            else:
                log.warning("[Signal] DB 업데이트 경고: %s", result.stderr[-300:])
        except subprocess.TimeoutExpired:
            log.error("[Signal] DB 업데이트 타임아웃 (10분)")
        except Exception as e:
            log.error("[Signal] DB 업데이트 실패: %s", e)

    def get_target(self) -> list[str]:
        """
        MeanRev-60d 하위 top_k 종목 코드 반환.
        반환값: ["005930", "000660", ...]  (6자리 KR 코드)
        """
        today = datetime.now().strftime("%Y-%m-%d")
        # 신호 계산에 충분한 과거 데이터 확보 (lookback + 여유)
        start = "2024-01-01"

        results = strategy_mean_reversion(
            market="KR",
            lookbacks=[self.lookback],
            top_k=self.top_k,
            market_filter=False,
            vol_scale=False,
            start=start,
            end=today,
            db_path=DB_PATH,
        )
        key = f"MeanRev-{self.lookback}d"
        result = results.get(key)
        if result is None or result.trades.empty:
            log.warning("[Signal] 시그널 없음")
            return []

        # 가장 최근 리밸런싱 날의 보유 종목 추출
        # trades에서 마지막 BUY 날의 종목 목록 사용
        buys = result.trades[result.trades["action"] == "BUY"]
        if buys.empty:
            return []
        last_date = buys["date"].max()
        target = buys[buys["date"] == last_date]["ticker"].tolist()
        log.info("[Signal] 선정 종목 (%d개, 기준일=%s): %s",
                 len(target), last_date.date(), target[:5])
        return [str(t).zfill(6) for t in target]


# ─────────────────────────────────────────
# 키움 자동매매 메인
# ─────────────────────────────────────────
class KiwoomTrader:
    def __init__(self, api: KiwoomAPI, signal: MeanRevSignal, notifier: TelegramNotifier):
        self.api       = api
        self.signal    = signal
        self.notifier  = notifier
        self.acc       = api.get_account()
        self.peak_nav: float = 0.0   # 손절 기준 고점

        if not self.acc:
            raise ValueError("계좌번호를 확인하세요. (.env KIWOOM_ACCOUNT 또는 영웅문 로그인)")
        log.info("[Trader] 사용 계좌: %s", self.acc)

    # ── NAV 계산 ─────────────────────────────
    def _get_nav(self) -> float:
        info = self.api.request_balance(self.acc)
        nav  = info["eval_total"] or info["cash"]
        log.info("[NAV] 총평가: %d원 | 예수금: %d원", info["eval_total"], info["cash"])
        return float(nav)

    def _get_positions(self) -> dict[str, int]:
        info = self.api.request_balance(self.acc)
        return info["positions"]

    # ── 손절 체크 ────────────────────────────
    def _check_stop_loss(self) -> bool:
        nav = self._get_nav()
        if self.peak_nav == 0.0:
            self.peak_nav = nav
            return False

        self.peak_nav = max(self.peak_nav, nav)
        drawdown = (nav - self.peak_nav) / self.peak_nav

        if drawdown <= -STOP_LOSS_PCT:
            msg = (
                f"🚨 *손절 발동!*\n"
                f"고점: `{self.peak_nav:,.0f}원` → 현재: `{nav:,.0f}원`\n"
                f"낙폭: `{drawdown:.2%}` (기준: -{STOP_LOSS_PCT:.0%})\n"
                f"전량 현금화 진행 중..."
            )
            log.warning(msg)
            self.notifier.send(msg)

            positions = self._get_positions()
            for code, qty in positions.items():
                if self.api.send_order(self.acc, code, qty, "SELL", SCR_ORDER_SELL):
                    self.notifier.send(f"🔴 *손절 청산* `{code}` {qty}주")

            self.peak_nav = 0.0
            self.notifier.send("💵 *전량 현금 보유 — 다음 달 재진입 검토*")
            return True

        log.info("[StopLoss] 낙폭=%.2f%% (고점=%,.0f, 현재=%,.0f) — 정상",
                 drawdown * 100, self.peak_nav, nav)
        return False

    # ── 리밸런싱 ─────────────────────────────
    def rebalance(self):
        now = datetime.now()
        log.info("=" * 55)
        log.info("리밸런싱 시작: %s", now.strftime("%Y-%m-%d %H:%M KST"))
        log.info("=" * 55)

        self.notifier.send(
            f"📊 *MeanRev-60d 리밸런싱 시작*\n"
            f"📅 {now.strftime('%Y-%m-%d %H:%M KST')}\n"
            f"계좌: `{self.acc}`"
        )

        # 0. 손절 체크
        if self._check_stop_loss():
            log.warning("손절 발동 — 이번 달 리밸런싱 스킵")
            return

        # 1. DB 업데이트 + 시그널 계산
        self.signal.update_db()
        try:
            target_codes = self.signal.get_target()
        except Exception as e:
            msg = f"❌ 시그널 계산 오류: {e}"
            log.error(msg)
            self.notifier.send(msg)
            return

        if not target_codes:
            self.notifier.send("⚠️ 시그널 없음 — 리밸런싱 스킵")
            return

        self.notifier.send(
            f"🎯 *선정 종목 ({len(target_codes)}개)*\n"
            + "\n".join(f"  `{c}`" for c in target_codes)
        )

        # 2. 현재 포지션 + NAV 조회
        positions = self._get_positions()
        nav       = self._get_nav()
        target_per = nav // self.signal.top_k   # 종목당 목표 금액

        log.info("NAV=%,.0f원 | 종목당=%,.0f원", nav, target_per)

        # 3. 청산 (목표 외 종목)
        exits = [c for c in positions if c not in target_codes]
        for code in exits:
            qty = positions[code]
            log.info("[청산] %s %d주", code, qty)
            if self.api.send_order(self.acc, code, qty, "SELL", SCR_ORDER_SELL):
                self.notifier.send(f"🔴 *청산* `{code}` {qty}주")

        # 청산 체결 대기 (시장가 3초 여유)
        if exits:
            import time; time.sleep(3)
            nav       = self._get_nav()
            target_per = nav // self.signal.top_k

        # 4. 매수 (목표 종목)
        positions = self._get_positions()
        for code in target_codes:
            price = self.api.get_price_fast(code)
            if price <= 0:
                log.warning("[매수 스킵] %s 가격 조회 실패", code)
                continue

            target_qty  = int(target_per // price)
            current_qty = positions.get(code, 0)
            delta       = target_qty - current_qty

            if delta > 0:
                log.info("[매수] %s %d주 @ %d원", code, delta, price)
                if self.api.send_order(self.acc, code, delta, "BUY", SCR_ORDER_BUY):
                    self.notifier.send(f"🟢 *매수* `{code}` {delta}주 @ {price:,}원")
            elif delta < 0:
                trim = abs(delta)
                log.info("[비중축소] %s -%d주", code, trim)
                if self.api.send_order(self.acc, code, trim, "SELL", SCR_ORDER_SELL):
                    self.notifier.send(f"🟡 *비중축소* `{code}` -{trim}주")
            else:
                log.info("[유지] %s", code)

        # 5. 완료 리포트
        final_nav = self._get_nav()
        final_pos = self._get_positions()
        pos_lines = "\n".join(f"  `{c}`: {q}주" for c, q in final_pos.items()) or "  (없음)"

        summary = (
            f"✅ *리밸런싱 완료*\n"
            f"💰 총평가: `{final_nav:,.0f}원`\n"
            f"📋 *보유 포지션:*\n{pos_lines}\n"
            f"🕐 {datetime.now().strftime('%H:%M KST')}"
        )
        log.info(summary)
        self.notifier.send(summary)

    # ── 일별 손절 체크 (15:25 KST) ───────────
    def daily_check(self):
        now = datetime.now()
        if now.weekday() >= 5:
            return
        log.info("[일별 손절 체크] %s", now.strftime("%Y-%m-%d"))
        self._check_stop_loss()


# ─────────────────────────────────────────
# 스케줄러 (QTimer 기반)
# ─────────────────────────────────────────
def _is_first_monday() -> bool:
    today = date.today()
    return today.weekday() == 0 and today.day <= 7


def _is_market_open() -> bool:
    now = datetime.now()
    # 주말 제외, 장 시간 09:00~15:30
    if now.weekday() >= 5:
        return False
    t = now.hour * 100 + now.minute
    return 900 <= t <= 1530


def run_scheduler(trader: KiwoomTrader):
    """
    QTimer로 분 단위 체크:
      - 매월 첫째 월요일 09:05 → rebalance()
      - 매 거래일 15:25 → daily_check()
    """
    rebal_done_this_month: Optional[int] = None
    daily_done_today: Optional[date]     = None

    def tick():
        nonlocal rebal_done_this_month, daily_done_today
        now   = datetime.now()
        today = date.today()
        hhmm  = now.hour * 100 + now.minute

        # 리밸런싱: 첫째 월요일 09:05
        month_key = today.year * 100 + today.month
        if (
            _is_first_monday()
            and hhmm == REBAL_HOUR * 100 + REBAL_MINUTE
            and rebal_done_this_month != month_key
        ):
            rebal_done_this_month = month_key
            trader.rebalance()

        # 일별 손절 체크: 15:25
        if (
            hhmm == 1525
            and daily_done_today != today
            and _is_market_open()
        ):
            daily_done_today = today
            trader.daily_check()

    timer = QTimer()
    timer.timeout.connect(tick)
    timer.start(60_000)   # 1분마다 체크
    log.info("[Scheduler] 시작 — 매월 첫째 월요일 %02d:%02d KST 리밸런싱",
             REBAL_HOUR, REBAL_MINUTE)
    trader.notifier.send(
        f"🤖 *KiwoomTrader 시작*\n"
        f"전략: MeanRev-{LOOKBACK}d | Top-{TOP_K} | 매월 첫째 월요일 {REBAL_HOUR:02d}:{REBAL_MINUTE:02d} KST\n"
        f"손절: 고점 대비 -{STOP_LOSS_PCT:.0%}\n"
        f"계좌: `{trader.acc}`"
    )
    return timer   # GC 방지용 반환


# ─────────────────────────────────────────
# CLI
# ─────────────────────────────────────────
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="키움 OpenAPI+ 자동매매 — MeanRev-60d")
    parser.add_argument("--dry-run",  action="store_true", help="시그널만 계산 (주문 없음)")
    parser.add_argument("--run-now",  action="store_true", help="즉시 리밸런싱 실행")
    parser.add_argument("--top-k",   type=int, default=TOP_K, help=f"종목 수 (기본 {TOP_K})")
    args = parser.parse_args()

    app = QApplication(sys.argv)

    notifier = TelegramNotifier(TELEGRAM_TOKEN, TELEGRAM_CHAT_ID)
    signal   = MeanRevSignal(top_k=args.top_k)

    if args.dry_run:
        # Qt 없이 시그널만 출력
        signal.update_db()
        targets = signal.get_target()
        print(f"\n=== Dry Run: MeanRev-{LOOKBACK}d 시그널 ===")
        print(f"선정 종목 (top {args.top_k}): {targets}")
        sys.exit(0)

    # 키움 로그인
    api = KiwoomAPI()
    if not api.connect():
        log.error("로그인 실패 — 종료")
        sys.exit(1)

    trader = KiwoomTrader(api, signal, notifier)

    if args.run_now:
        trader.rebalance()
    else:
        _timer = run_scheduler(trader)   # GC 방지
        sys.exit(app.exec_())
