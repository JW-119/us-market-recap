import time
import requests
from config import TELEGRAM_BOT_TOKEN, CHANNEL_ID
from fetcher import fetch_indices, fetch_sectors, fetch_top_movers, get_market_date


def build_market_recap_message():
    """시황 요약 HTML 메시지 리스트 반환 (3800자 분할)."""
    date = get_market_date() or "N/A"
    indices = fetch_indices()
    sectors = fetch_sectors()
    gainers, losers = fetch_top_movers(top_n=5)

    parts = []

    # ── 헤더 ──
    parts.append(f"<b>📊 미국 시장 시황 — {date}</b>\n")

    # ── 지수 요약 ──
    parts.append("<b>▎주요 지수</b>")
    for _, r in indices.iterrows():
        arrow = "🔺" if r["변동"] >= 0 else "🔻"
        sign = "+" if r["변동"] >= 0 else ""
        parts.append(
            f"  {r['이름']}: <b>{r['종가']:,.2f}</b>  "
            f"{arrow} {sign}{r['변동']:,.2f} ({sign}{r['등락률']:.2f}%)"
        )
    parts.append("")

    # ── 섹터 Leaders / Laggards ──
    if not sectors.empty:
        top3 = sectors.head(3)
        bot3 = sectors.tail(3).iloc[::-1]
        parts.append("<b>▎섹터 Leaders</b>")
        for _, r in top3.iterrows():
            sign = "+" if r["등락률"] >= 0 else ""
            parts.append(f"  {r['섹터']} ({r['티커']}): {sign}{r['등락률']:.2f}%")
        parts.append("")
        parts.append("<b>▎섹터 Laggards</b>")
        for _, r in bot3.iterrows():
            sign = "+" if r["등락률"] >= 0 else ""
            parts.append(f"  {r['섹터']} ({r['티커']}): {sign}{r['등락률']:.2f}%")
        parts.append("")

    # ── Top Gainers / Losers ──
    if not gainers.empty:
        parts.append("<b>▎상승 Top 5</b>")
        for _, r in gainers.iterrows():
            parts.append(
                f"  {r['티커']}: {r['종가']:,.2f}  (+{r['등락률']:.2f}%)"
            )
        parts.append("")

    if not losers.empty:
        parts.append("<b>▎하락 Top 5</b>")
        for _, r in losers.iterrows():
            parts.append(
                f"  {r['티커']}: {r['종가']:,.2f}  ({r['등락률']:.2f}%)"
            )

    full_text = "\n".join(parts)

    # 3800자 분할
    if len(full_text) <= 3800:
        return [full_text]

    messages = []
    while full_text:
        if len(full_text) <= 3800:
            messages.append(full_text)
            break
        cut = full_text[:3800].rfind("\n")
        if cut == -1:
            cut = 3800
        messages.append(full_text[:cut])
        full_text = full_text[cut:].lstrip("\n")
    return messages


def send_recap():
    """시황 메시지를 빌드하고 텔레그램으로 발송."""
    if not TELEGRAM_BOT_TOKEN or not CHANNEL_ID:
        print("[오류] TELEGRAM_BOT_TOKEN 또는 CHANNEL_ID가 설정되지 않았습니다.")
        return False

    messages = build_market_recap_message()
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    for msg in messages:
        resp = requests.post(url, json={
            "chat_id": CHANNEL_ID,
            "text": msg,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }, timeout=30)

        if resp.status_code != 200:
            print(f"[오류] 텔레그램 전송 실패: {resp.text}")
            return False

        if len(messages) > 1:
            time.sleep(1)

    print(f"[완료] 시황 메시지 {len(messages)}건 발송 완료")
    return True
