"""
main.py
NASDAQ Intraday Quant System — 진입점

사용법:
    python main.py backtest --start 2026-03-01 --end 2026-03-31 --top-n 5 --capital 100000
    python main.py backtest --days 14
    python main.py live
"""
import argparse
import logging
import sys
from datetime import date, timedelta
from pathlib import Path

from config import CAPITAL, TOP_N


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        level=level,
    )


def _resolve_dates(args) -> tuple[date, date]:
    """--start/--end 또는 --days 로부터 (start, end) 결정"""
    if args.start and args.end:
        return date.fromisoformat(args.start), date.fromisoformat(args.end)
    end = date.today()
    start = end - timedelta(days=args.days)
    return start, end


def cmd_backtest(args) -> None:
    from nasdaq_quant.backtest import Backtester
    from nasdaq_quant.report import Reporter

    _setup_logging(args.verbose)

    start, end = _resolve_dates(args)
    print(f"[backtest] {start} ~ {end}  top_n={args.top_n}  capital=${args.capital:,.0f}")

    bt = Backtester(capital=args.capital, top_n=args.top_n)
    trades = bt.run(start, end)

    rp = Reporter(capital=args.capital)
    rp.print_console(trades)

    if args.output:
        out = Path(args.output)
        csv_path = rp.to_csv(trades, out.with_suffix(".csv"))
        html_path = rp.to_html(trades, out.with_suffix(".html"))
        print(f"  CSV  → {csv_path}")
        print(f"  HTML → {html_path}")


def cmd_live(args) -> None:
    import os
    from pathlib import Path
    from datetime import date
    from dotenv import load_dotenv
    import requests as _req

    _setup_logging(args.verbose)
    load_dotenv(Path(__file__).parent / ".env")

    api_key    = os.getenv("ALPACA_API_KEY", "")
    secret_key = os.getenv("ALPACA_SECRET_KEY", "")
    tg_token   = os.getenv("TELEGRAM_TOKEN", "")
    tg_chat    = os.getenv("TELEGRAM_CHAT_ID", "")

    if not api_key or not secret_key:
        print("[오류] ALPACA_API_KEY / ALPACA_SECRET_KEY 환경변수 설정 필요 (.env)")
        sys.exit(1)

    # Telegram notifier (alpaca_trader.py와 동일 구조)
    class _TGNotifier:
        def __init__(self, token, chat_id):
            self.enabled = bool(token and chat_id)
            self._token  = token
            self._chat   = chat_id
        def send(self, msg: str) -> None:
            if not self.enabled:
                print(f"[알림] {msg}")
                return
            try:
                _req.post(
                    f"https://api.telegram.org/bot{self._token}/sendMessage",
                    json={"chat_id": self._chat, "text": msg, "parse_mode": "Markdown"},
                    timeout=10,
                )
            except Exception as e:
                logging.getLogger(__name__).warning("Telegram 전송 실패: %s", e)

    from nasdaq_quant.live.broker import AlpacaBroker
    from nasdaq_quant.live.runner import ORBLiveRunner
    from nasdaq_quant.live.journal import PaperJournal

    broker   = AlpacaBroker(api_key, secret_key, paper=True)
    notifier = _TGNotifier(tg_token, tg_chat)
    journal  = PaperJournal(log_dir=Path(args.log_dir))

    if not broker.is_market_open() and not args.dry_run:
        print("[경고] 현재 시장 휴장 상태입니다. --dry-run 으로 실행하세요.")

    runner = ORBLiveRunner(
        broker   = broker,
        notifier = notifier,
        capital  = args.capital,
        top_n    = args.top_n,
        dry_run  = args.dry_run,
        journal  = journal,
    )
    runner.run_day()

    # 기간 누적 리포트 (당일 단독 실행 시에도 표시)
    journal.print_period_report()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="NASDAQ Intraday Quant System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--verbose", "-v", action="store_true", help="상세 로그 출력")
    sub = parser.add_subparsers(dest="cmd")

    # ── backtest ──
    bt = sub.add_parser("backtest", help="백테스트 실행")
    bt.add_argument("--start",   type=str,   default=None,     help="시작일 YYYY-MM-DD")
    bt.add_argument("--end",     type=str,   default=None,     help="종료일 YYYY-MM-DD")
    bt.add_argument("--days",    type=int,   default=14,       help="--start/--end 미지정 시 최근 N일")
    bt.add_argument("--top-n",   type=int,   default=TOP_N,    dest="top_n",   help="상위 종목 수")
    bt.add_argument("--capital", type=float, default=CAPITAL,  help="초기 자본 ($)")
    bt.add_argument("--output",  type=str,   default=None,     help="출력 파일 경로 (확장자 무시, .csv/.html 자동)")

    # ── live ──
    lv = sub.add_parser("live", help="실전 매매 (ORB)")
    lv.add_argument("--dry-run",  action="store_true", help="시그널만 출력, 실제 주문 없음")
    lv.add_argument("--top-n",   type=int,   default=TOP_N,   dest="top_n",  help="상위 종목 수")
    lv.add_argument("--capital", type=float, default=CAPITAL, help="운용 자본 ($)")
    lv.add_argument("--log-dir", type=str,   default="logs",  dest="log_dir", help="거래 일지 저장 디렉터리")

    args = parser.parse_args()
    if args.cmd == "backtest":
        cmd_backtest(args)
    elif args.cmd == "live":
        cmd_live(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
