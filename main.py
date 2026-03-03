import argparse
import time
import schedule
from telegram_sender import send_recap


def job():
    print("=" * 40)
    print("시황 수집 및 텔레그램 발송 시작...")
    send_recap()
    print("=" * 40)


def main():
    parser = argparse.ArgumentParser(description="미국 주식 시장 일일 시황 봇")
    parser.add_argument(
        "--schedule", action="store_true",
        help="매일 08:00 KST (23:00 UTC) 자동 발송 모드",
    )
    args = parser.parse_args()

    if args.schedule:
        schedule.every().day.at("23:00").do(job)   # UTC 23:00 = KST 08:00
        print("스케줄 모드 시작 — 매일 08:00 KST 발송")
        while True:
            schedule.run_pending()
            time.sleep(30)
    else:
        job()


if __name__ == "__main__":
    main()
