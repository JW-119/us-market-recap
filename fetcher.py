import logging
import threading
import time
from collections import deque
from difflib import SequenceMatcher
from urllib.parse import quote, unquote

import feedparser
import yfinance as yf
import pandas as pd
import requests
from datetime import datetime, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor
from config import (
    INDICES, SECTOR_ETFS, MAJOR_STOCKS,
    BARCHART_PAGE_URL, BARCHART_API_URL, BARCHART_HEADERS,
    NEW_HIGHS_TARGETS, NEW_HIGHS_FIELDS, NEW_HIGHS_MIN_MARKET_CAP,
    NEW_HIGHS_PAGE_SIZE, GROQ_API_KEY, GROQ_MODEL, SECTOR_NEWS_QUERIES,
)

log = logging.getLogger(__name__)


def get_market_date():
    """마지막 거래일 문자열 (YYYY-MM-DD). 최대 3회 재시도."""
    for attempt in range(3):
        try:
            sp = yf.download("^GSPC", period="5d", progress=False)
            if not sp.empty:
                return str(sp.index[-1].date())
        except Exception as e:
            log.warning("get_market_date attempt %d failed: %s", attempt + 1, e)
        if attempt < 2:
            time.sleep(2)
    return None


def fetch_fear_greed():
    """CNN Fear & Greed Index 현재 점수·등급·전일대비 변동 반환."""
    url = "https://production.dataviz.cnn.io/index/fearandgreed/graphdata"
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Referer": "https://www.cnn.com/markets/fear-and-greed",
    }
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        fg = resp.json()["fear_and_greed"]
        score = round(fg["score"])
        prev = round(fg.get("previous_close", score))
        rating_map = {
            "extreme fear": "극단적 공포",
            "fear": "공포",
            "neutral": "중립",
            "greed": "탐욕",
            "extreme greed": "극단적 탐욕",
        }
        return {
            "score": score,
            "prev": prev,
            "change": score - prev,
            "rating": rating_map.get(fg["rating"], fg["rating"]),
        }
    except Exception:
        return None


def fetch_indices():
    """주요 지수별 종가·전일대비·등락률 DataFrame 반환."""
    tickers = list(INDICES.keys())
    data = yf.download(tickers, period="5d", progress=False, group_by="ticker")

    rows = []
    for tkr, name in INDICES.items():
        try:
            if len(tickers) == 1:
                df = data
            else:
                df = data[tkr]
            df = df.dropna(subset=["Close"])
            if len(df) < 2:
                continue
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2])
            chg = close - prev
            pct = chg / prev * 100
            rows.append({
                "이름": name,
                "티커": tkr,
                "종가": round(close, 2),
                "변동": round(chg, 2),
                "등락률": round(pct, 2),
            })
        except Exception:
            continue
    return pd.DataFrame(rows)


def fetch_sectors():
    """섹터 ETF별 등락률 DataFrame 반환 (등락률 내림차순)."""
    tickers = list(SECTOR_ETFS.keys())
    data = yf.download(tickers, period="5d", progress=False, group_by="ticker")

    rows = []
    for tkr, name in SECTOR_ETFS.items():
        try:
            df = data[tkr].dropna(subset=["Close"])
            if len(df) < 2:
                continue
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2])
            pct = (close - prev) / prev * 100
            rows.append({
                "섹터": name,
                "티커": tkr,
                "종가": round(close, 2),
                "등락률": round(pct, 2),
            })
        except Exception:
            continue
    result = pd.DataFrame(rows)
    if not result.empty:
        result = result.sort_values("등락률", ascending=False).reset_index(drop=True)
    return result


def _get_stock_name(tkr):
    """yfinance에서 종목 shortName 조회. 실패 시 티커 반환."""
    try:
        info = yf.Ticker(tkr).info
        return tkr, info.get("shortName", tkr)
    except Exception:
        return tkr, tkr


def fetch_top_movers(top_n=10):
    """대형주 상승/하락 Top N → (gainers_df, losers_df) 튜플."""
    tickers = MAJOR_STOCKS
    data = yf.download(tickers, period="5d", progress=False, group_by="ticker")

    rows = []
    for tkr in tickers:
        try:
            df = data[tkr].dropna(subset=["Close"])
            if len(df) < 2:
                continue
            close = float(df["Close"].iloc[-1])
            prev = float(df["Close"].iloc[-2])
            chg = close - prev
            pct = chg / prev * 100
            rows.append({
                "티커": tkr,
                "종가": round(close, 2),
                "등락률": round(pct, 2),
            })
        except Exception:
            continue

    all_df = pd.DataFrame(rows)
    if all_df.empty:
        return pd.DataFrame(), pd.DataFrame()

    all_df = all_df.sort_values("등락률", ascending=False).reset_index(drop=True)
    gainers = all_df.head(top_n).reset_index(drop=True)
    losers = all_df.tail(top_n).sort_values("등락률").reset_index(drop=True)

    # 상위/하위 종목에 대해서만 이름 조회 (병렬)
    need_names = set(gainers["티커"]) | set(losers["티커"])
    with ThreadPoolExecutor(max_workers=10) as pool:
        name_map = dict(pool.map(_get_stock_name, need_names))

    for df in (gainers, losers):
        df.insert(0, "종목명", df["티커"].map(name_map))

    return gainers, losers


def _translate_text(text):
    """텍스트를 한글로 번역. 실패 시 원문 반환."""
    if not text:
        return text
    try:
        from deep_translator import GoogleTranslator
        return GoogleTranslator(source="en", target="ko").translate(text)
    except Exception:
        return text


_NEWS_BLOCKED_PUBLISHERS = {"MT Newswires", "Barchart", "Barrons.com", "Barron's"}


# ── Groq Rate Limiter (RPM + TPM 이중 슬라이딩 윈도우) ──

class _GroqRateLimiter:
    """RPM(28/60s) + TPM(5500/60s) 이중 슬라이딩 윈도우 rate limiter."""

    def __init__(self, max_rpm=28, max_tpm=20000, window_sec=60):
        self._max_rpm = max_rpm
        self._max_tpm = max_tpm
        self._window = window_sec
        self._call_log: deque = deque()       # (timestamp,)
        self._token_log: deque = deque()      # (timestamp, tokens)
        self._lock = threading.Lock()

    def _purge(self, now):
        while self._call_log and now - self._call_log[0] > self._window:
            self._call_log.popleft()
        while self._token_log and now - self._token_log[0][0] > self._window:
            self._token_log.popleft()

    def _current_tpm(self):
        return sum(t for _, t in self._token_log)

    def acquire(self, est_tokens=500):
        """est_tokens: 이 호출의 예상 토큰 수 (입력+출력)."""
        while True:
            with self._lock:
                now = time.monotonic()
                self._purge(now)

                rpm_ok = len(self._call_log) < self._max_rpm
                tpm_ok = self._current_tpm() + est_tokens <= self._max_tpm

                if rpm_ok and tpm_ok:
                    self._call_log.append(now)
                    self._token_log.append((now, est_tokens))
                    return

                # 대기 시간 계산
                waits = []
                if not rpm_ok and self._call_log:
                    waits.append(self._window - (now - self._call_log[0]) + 0.1)
                if not tpm_ok and self._token_log:
                    waits.append(self._window - (now - self._token_log[0][0]) + 0.1)
                sleep_time = max(waits) if waits else 1.0

            reason = "RPM" if not rpm_ok else "TPM"
            log.info("Groq %s limit reached, sleeping %.1fs", reason, sleep_time)
            time.sleep(sleep_time)


_groq_limiter = _GroqRateLimiter()


# ── Google News RSS 수집 ──

def _fetch_google_news_rss(ticker, query):
    """Google News RSS에서 검색 쿼리로 기사 수집. 최대 10개 반환."""
    encoded = quote(query)
    url = f"https://news.google.com/rss/search?q={encoded}+when:1d&hl=en-US&gl=US&ceid=US:en"
    try:
        feed = feedparser.parse(url)
        results = []
        for entry in feed.entries[:10]:
            title = entry.get("title", "")
            link = entry.get("link", "")
            source = entry.get("source", {})
            publisher = source.get("title", "") if isinstance(source, dict) else ""
            if not title or not link:
                continue
            if publisher in _NEWS_BLOCKED_PUBLISHERS:
                continue
            results.append({
                "title": title,
                "summary": "",
                "publisher": publisher,
                "url": link,
            })
        return results
    except Exception:
        return []


# ── 중복 제거 (제목 유사도) ──

def _deduplicate_articles(articles, threshold=0.6):
    """SequenceMatcher로 제목 유사도 > threshold인 중복 제거."""
    unique = []
    for article in articles:
        is_dup = False
        for kept in unique:
            ratio = SequenceMatcher(
                None, article["title"].lower(), kept["title"].lower()
            ).ratio()
            if ratio > threshold:
                is_dup = True
                break
        if not is_dup:
            unique.append(article)
    return unique


# ── 품질 점수 ──

_CAUSAL_KEYWORDS = [
    "because", "due to", "driven by", "amid", "after", "following",
    "as", "on", "surge", "plunge", "rally", "drop", "fall", "rise",
    "gain", "decline", "soar", "tumble", "jump", "slide",
]


def _score_article_quality(title, body):
    """기사 품질 점수: 본문 길이 + 인과관계 키워드 + 제목 구체성."""
    score = 0
    # 본문 길이 (최대 5점)
    score += min(len(body) / 500, 5)
    # 인과관계 키워드 (본문에서, 최대 3점)
    lower_body = body.lower()
    keyword_hits = sum(1 for kw in _CAUSAL_KEYWORDS if kw in lower_body)
    score += min(keyword_hits, 3)
    # 제목 구체성: 숫자 포함 시 +1
    import re
    if re.search(r'\d', title):
        score += 1
    return score


# ── 2단계 합성 LLM ──

def _synthesize_sector_summary(sector_name, summaries):
    """개별 기사 요약들을 합성하여 1~2문장 섹터 등락 원인 생성."""
    if not summaries:
        return ""
    bullet_list = "\n".join(f"- {s}" for s in summaries)
    est_tokens = len(bullet_list) // 4 + 200 + 200  # 입력 추정 + 시스템 + 출력
    messages = [
        {
            "role": "system",
            "content": (
                "미국 주식 섹터 뉴스 편집자.\n"
                "아래 개별 기사 요약들을 종합하여, 해당 섹터의 등락 핵심 원인을 "
                "1~2문장으로 합성하라.\n"
                "반드시 한국어만 사용. 영어 고유명사(기업명, ETF명)만 영어 허용.\n"
                "문장 끝은 반드시 명사형 간결체: ~함, ~됨, ~전망, ~때문, ~영향, ~기여 등.\n"
                "중복 내용은 통합하고, 가장 중요한 원인 위주로 압축할 것."
            ),
        },
        {
            "role": "user",
            "content": f"섹터: {sector_name}\n\n개별 요약:\n{bullet_list}",
        },
    ]
    for attempt in range(3):
        try:
            from groq import Groq
            _groq_limiter.acquire(est_tokens)
            client = Groq(api_key=GROQ_API_KEY)
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=messages,
                temperature=0.2,
                max_tokens=200,
            )
            result = response.choices[0].message.content.strip()
            return _clean_llm_output(result)
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                wait = 2 ** attempt * 5
                log.warning("Synthesis 429 for %s, retry %d after %ds", sector_name, attempt + 1, wait)
                time.sleep(wait)
            else:
                break
    return " / ".join(summaries)


def _extract_article_body(url):
    """trafilatura로 기사 본문 추출. 실패 시 빈 문자열 반환."""
    try:
        import trafilatura
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            return ""
        text = trafilatura.extract(downloaded)
        return text or ""
    except Exception:
        return ""


def _is_quality_article(body):
    """본문 200자 미만이면 영상/비기사로 판단하여 제거."""
    return len(body) >= 200


def _clean_llm_output(text):
    """LLM 출력 후처리: 프롬프트 에코 제거 + 줄바꿈 정리 + 비한국어 문자 제거 + 말투 교정."""
    import re
    # 프롬프트 에코 제거 (모델이 입력을 되풀이하는 경우)
    text = re.sub(r'^.*?개별\s*(?:기사\s*)?요약\s*(?:종합)?[:：]\s*', '', text, flags=re.DOTALL)
    text = re.sub(r'^섹터[:：]\s*\S+\s*', '', text)
    # 리터럴 \n 및 실제 줄바꿈 → 공백
    text = text.replace('\\n', ' ').replace('\n', ' ')
    text = re.sub(r'\s{2,}', ' ', text)
    # 비한국어/비ASCII 스크립트 제거 (한글, ASCII, 숫자, 기본 구두점만 허용)
    text = re.sub(r'[^\uAC00-\uD7A3\u3131-\u3163\u1100-\u11FF'
                  r'a-zA-Z0-9\s\.,;:!\?\-\+\%\(\)\/\'\"~$&@#]', '', text)
    # 말투 교정: 문장 끝 패턴 치환
    replacements = [
        (r'하고\s*있습니다', '하는 중'),
        (r'되고\s*있습니다', '되는 중'),
        (r'하고\s*있다', '하는 중'),
        (r'되고\s*있다', '되는 중'),
        (r'것으로\s*보입니다', '것으로 판단됨'),
        (r'것으로\s*보인다', '것으로 판단됨'),
        (r'수\s*있습니다', '수 있음'),
        (r'수\s*있다', '수 있음'),
        (r'때문입니다', '때문'),
        (r'때문이다', '때문'),
        (r'되었습니다', '됨'),
        (r'되었다', '됨'),
        (r'했습니다', '함'),
        (r'했다', '함'),
        (r'됩니다', '됨'),
        (r'된다', '됨'),
        (r'합니다', '함'),
        (r'한다', '함'),
        (r'입니다', '임'),
        (r'이다', '임'),
        (r'있습니다', '있음'),
        (r'봅니다', '봄'),
        (r'보인다', '보임'),
        (r'겠습니다', '전망'),
    ]
    for pattern, repl in replacements:
        text = re.sub(pattern, repl, text)
    return text.strip()


def _summarize_with_llm(sector_name, title, body):
    """Groq LLM으로 '왜?' 요약 생성. SKIP 응답이면 None 반환."""
    truncated_body = body[:2300]
    est_tokens = len(truncated_body) // 4 + 200 + 150  # 본문 추정 + 시스템 + 출력
    messages = [
        {
            "role": "system",
            "content": (
                "미국 주식 시장 섹터 뉴스 분석가.\n"
                "기사를 읽고 해당 섹터의 등락 원인을 1문장 한국어로 요약.\n"
                "반드시 한국어만 사용. 영어 고유명사(기업명, ETF명)만 영어 허용.\n"
                "문장 끝은 반드시 명사형 간결체: ~함, ~됨, ~전망, ~때문, ~영향, ~기여 등.\n\n"
                "예시:\n"
                "- 대법원의 관세 무효화 판결로 수입 비용 부담 완화, 소비재 섹터 반등에 기여함\n"
                "- AI 투자 과열 우려 확산으로 기술주 전반 매도세 발생, 섹터 하락 요인으로 작용함\n"
                "- 금리 인하 기대감에 부동산 관련주 상승, XLRE 0.5% 상승함\n\n"
                "인과관계 없는 기사(종목 소개, 광고, 영상)면 SKIP 이라고만 답할 것."
            ),
        },
        {
            "role": "user",
            "content": (
                f"섹터: {sector_name}\n"
                f"제목: {title}\n"
                f"본문:\n{truncated_body}"
            ),
        },
    ]
    for attempt in range(3):
        try:
            from groq import Groq
            _groq_limiter.acquire(est_tokens)
            client = Groq(api_key=GROQ_API_KEY)
            response = client.chat.completions.create(
                model=GROQ_MODEL,
                messages=messages,
                temperature=0.2,
                max_tokens=150,
            )
            result = response.choices[0].message.content.strip()
            if result.upper() == "SKIP":
                return None
            return _clean_llm_output(result)
        except Exception as e:
            if "429" in str(e) or "rate" in str(e).lower():
                wait = 2 ** attempt * 5
                log.warning("Summarize 429 for %s, retry %d after %ds", sector_name, attempt + 1, wait)
                time.sleep(wait)
            else:
                break
    return None


def fetch_sector_news():
    """섹터 ETF별 뉴스 수집 → {섹터명: {synthesis, articles}}.

    LLM 모드: yfinance + Google News RSS → 중복 제거 → 본문 추출 → 품질 필터/점수
    → 상위 5개 선택 → 1단계 개별 요약 → 2단계 합성 요약
    Non-LLM 모드: yfinance만 사용, 번역 후 반환 (synthesis 빈 문자열)
    """
    sector_map = {tkr: name for tkr, name in SECTOR_ETFS.items()}
    use_llm = bool(GROQ_API_KEY)

    max_yf = 8 if use_llm else 10

    def _get_yf_news(tkr):
        try:
            items = yf.Ticker(tkr).news or []
            results = []
            for item in items[:max_yf]:
                content = item.get("content", {})
                title = content.get("title", "")
                summary = content.get("summary", "")
                provider = content.get("provider", {})
                publisher = provider.get("displayName", "") if isinstance(provider, dict) else ""
                click_through = content.get("clickThroughUrl", {})
                url = click_through.get("url", "") if isinstance(click_through, dict) else ""
                if not title or not url:
                    continue
                if publisher in _NEWS_BLOCKED_PUBLISHERS:
                    continue
                results.append({
                    "title": title,
                    "summary": summary,
                    "publisher": publisher,
                    "url": url,
                })
            return tkr, results
        except Exception:
            return tkr, []

    def _get_google_news(tkr):
        query = SECTOR_NEWS_QUERIES.get(tkr, "")
        if not query:
            return tkr, []
        return tkr, _fetch_google_news_rss(tkr, query)

    # 1) 뉴스 수집: yfinance + Google News RSS (병렬)
    with ThreadPoolExecutor(max_workers=11) as pool:
        yf_futures = {pool.submit(_get_yf_news, tkr): tkr for tkr in sector_map}
        gn_futures = {pool.submit(_get_google_news, tkr): tkr for tkr in sector_map}

        yf_results = {}
        for future in yf_futures:
            tkr, news_list = future.result()
            yf_results[tkr] = news_list

        gn_results = {}
        for future in gn_futures:
            tkr, news_list = future.result()
            gn_results[tkr] = news_list

    # 소스 병합 + 중복 제거
    raw_output = {}
    for tkr, sector_name in sector_map.items():
        combined = yf_results.get(tkr, []) + gn_results.get(tkr, [])
        deduped = _deduplicate_articles(combined)
        raw_output[sector_name] = deduped
        log.info("%s: yf=%d, gn=%d, dedup=%d",
                 sector_name, len(yf_results.get(tkr, [])),
                 len(gn_results.get(tkr, [])), len(deduped))

    if not use_llm:
        # ── Non-LLM: yfinance summary + deep_translator 번역, 새 구조 반환 ──
        output = {}
        for sector, news_list in raw_output.items():
            output[sector] = {"synthesis": "", "articles": news_list[:3]}

        all_texts = []
        text_map = []
        for sector, data in output.items():
            for i, n in enumerate(data["articles"]):
                if n["title"]:
                    all_texts.append(n["title"])
                    text_map.append((sector, i, "title"))
                if n["summary"]:
                    all_texts.append(n["summary"])
                    text_map.append((sector, i, "summary"))

        if all_texts:
            with ThreadPoolExecutor(max_workers=10) as pool:
                translated = list(pool.map(_translate_text, all_texts))
            for (sector, i, field), translated_text in zip(text_map, translated):
                output[sector]["articles"][i][field] = translated_text

        return output

    # ── LLM 모드: 본문 추출 → 품질 필터/점수 → 상위 5개 → 2단계 합성 ──

    # 2) 모든 기사 URL에서 본문 추출 (병렬)
    all_articles = []  # (sector, article_dict)
    for sector, news_list in raw_output.items():
        for article in news_list:
            all_articles.append((sector, article))

    urls = [a[1]["url"] for a in all_articles]
    with ThreadPoolExecutor(max_workers=10) as pool:
        bodies = list(pool.map(_extract_article_body, urls))

    # 3) 품질 필터 (200자+) + 품질 점수 정렬 → 섹터당 상위 5개
    sector_candidates = {name: [] for name in sector_map.values()}
    for (sector, article), body in zip(all_articles, bodies):
        if not _is_quality_article(body):
            continue
        score = _score_article_quality(article["title"], body)
        sector_candidates[sector].append((article, body, score))

    # 점수 내림차순 → 상위 5개
    top_per_sector = {}
    for sector, candidates in sector_candidates.items():
        candidates.sort(key=lambda x: x[2], reverse=True)
        top_per_sector[sector] = candidates[:5]
        log.info("%s: %d quality articles, top %d selected",
                 sector, len(candidates), len(top_per_sector[sector]))

    # 4) 1단계: 개별 LLM 요약 (병렬, 3 workers)
    summarize_tasks = []  # (sector, article, body)
    for sector, items in top_per_sector.items():
        for article, body, _ in items:
            summarize_tasks.append((sector, article, body))

    def _do_summarize(item):
        sector, article, body = item
        summary = _summarize_with_llm(sector, article["title"], body)
        return sector, article, summary

    with ThreadPoolExecutor(max_workers=3) as pool:
        summarized = list(pool.map(_do_summarize, summarize_tasks))

    # SKIP 제거 → 섹터별 그룹핑
    sector_articles = {name: [] for name in sector_map.values()}
    sector_summaries = {name: [] for name in sector_map.values()}
    for sector, article, summary in summarized:
        if summary is None:
            continue
        sector_articles[sector].append({
            "title": article["title"],
            "summary": summary,
            "publisher": article["publisher"],
            "url": article["url"],
        })
        sector_summaries[sector].append(summary)

    # 5) 2단계: 섹터별 합성 LLM (병렬, 3 workers)
    def _do_synthesis(item):
        sector_name, summaries = item
        if not summaries:
            return sector_name, ""
        synthesis = _synthesize_sector_summary(sector_name, summaries)
        return sector_name, synthesis

    synthesis_tasks = [(s, sums) for s, sums in sector_summaries.items()]
    with ThreadPoolExecutor(max_workers=3) as pool:
        synthesis_results = list(pool.map(_do_synthesis, synthesis_tasks))

    synthesis_map = dict(synthesis_results)

    # 6) 제목 번역 (summary는 LLM이 한국어로 직접 생성)
    all_titles = []
    title_map = []
    for sector, articles in sector_articles.items():
        for i, n in enumerate(articles):
            if n["title"]:
                all_titles.append(n["title"])
                title_map.append((sector, i))

    if all_titles:
        with ThreadPoolExecutor(max_workers=10) as pool:
            translated = list(pool.map(_translate_text, all_titles))
        for (sector, i), translated_title in zip(title_map, translated):
            sector_articles[sector][i]["title"] = translated_title

    # 7) 최종 반환: {섹터: {synthesis, articles}}
    output = {}
    for sector_name in sector_map.values():
        output[sector_name] = {
            "synthesis": synthesis_map.get(sector_name, ""),
            "articles": sector_articles.get(sector_name, []),
        }

    return output


def fetch_weekly_earnings():
    """MAJOR_STOCKS 중 이번 주 실적 발표 예정 종목 DataFrame 반환."""
    today = datetime.now()
    # 이번 주 월요일 ~ 일요일
    monday = today - timedelta(days=today.weekday())
    sunday = monday + timedelta(days=6)
    mon_date = monday.date()
    sun_date = sunday.date()

    def _get_earnings(tkr):
        try:
            cal = yf.Ticker(tkr).calendar
            if cal is None or not isinstance(cal, dict):
                return None
            earnings_dates = cal.get("Earnings Date", [])
            if not earnings_dates:
                return None
            for ed in earnings_dates:
                if hasattr(ed, "date"):
                    ed_date = ed.date()
                else:
                    ed_date = pd.Timestamp(ed).date()
                if mon_date <= ed_date <= sun_date:
                    eps_avg = cal.get("Earnings Average", None)
                    rev_avg = cal.get("Revenue Average", None)
                    return {
                        "티커": tkr,
                        "발표일": str(ed_date),
                        "EPS 예상": round(eps_avg, 2) if eps_avg else "-",
                        "매출 예상": f"{rev_avg / 1e9:.1f}B" if rev_avg else "-",
                    }
            return None
        except Exception:
            return None

    with ThreadPoolExecutor(max_workers=10) as pool:
        results = list(pool.map(_get_earnings, MAJOR_STOCKS))

    rows = [r for r in results if r is not None]
    df = pd.DataFrame(rows)
    if not df.empty:
        df = df.sort_values("발표일").reset_index(drop=True)
        with ThreadPoolExecutor(max_workers=10) as pool:
            name_map = dict(pool.map(_get_stock_name, df["티커"]))
        df.insert(0, "종목명", df["티커"].map(name_map))
    return df


def fetch_index_history(ticker, period="1mo"):
    """캔들차트용 OHLCV DataFrame (날짜 인덱스)."""
    df = yf.download(ticker, period=period, progress=False)
    if df.empty:
        return df
    # MultiIndex columns → flatten
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)
    return df


# ── Barchart New Highs ──

def _barchart_session() -> tuple[requests.Session, dict]:
    """Barchart 페이지에 접속하여 XSRF 토큰이 포함된 세션을 반환."""
    session = requests.Session()
    headers = dict(BARCHART_HEADERS)
    headers["Referer"] = BARCHART_PAGE_URL

    resp = session.get(BARCHART_PAGE_URL, headers=headers, timeout=30)
    resp.raise_for_status()

    xsrf_token = session.cookies.get("XSRF-TOKEN")
    if not xsrf_token:
        raise RuntimeError("XSRF-TOKEN 쿠키를 찾을 수 없습니다.")
    headers["X-XSRF-TOKEN"] = unquote(xsrf_token)
    return session, headers


def _barchart_fetch_page(session, headers, lists_value, page):
    """API에서 한 페이지의 데이터를 가져옴."""
    params = {
        "lists": lists_value,
        "fields": NEW_HIGHS_FIELDS,
        "meta": "field.shortName,field.type,field.description,lists.lastUpdate",
        "page": str(page),
        "limit": str(NEW_HIGHS_PAGE_SIZE),
        "raw": "1",
    }
    resp = session.get(BARCHART_API_URL, params=params, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.json()


def _format_market_cap(value):
    """시가총액을 읽기 좋은 형식으로 변환 (예: 2.5T, 150.3B)."""
    if pd.isna(value) or value == 0:
        return "-"
    if value >= 1e12:
        return f"{value / 1e12:.1f}T"
    if value >= 1e9:
        return f"{value / 1e9:.1f}B"
    if value >= 1e6:
        return f"{value / 1e6:.0f}M"
    return str(value)


def _fetch_single_target(session, headers, lists_value, label):
    """하나의 타겟(52주/3개월)에 대해 전체 페이지를 수집하여 DataFrame 반환."""
    all_records = []
    page = 1

    field_names = [f.strip() for f in NEW_HIGHS_FIELDS.split(",")]

    while True:
        data = _barchart_fetch_page(session, headers, lists_value, page)
        records = data.get("data", [])
        if not records:
            break

        for record in records:
            raw = record.get("raw", record)
            row = {f: raw.get(f, "") for f in field_names}
            all_records.append(row)

        total = data.get("count", 0)
        fetched = page * NEW_HIGHS_PAGE_SIZE
        if fetched >= total or len(records) < NEW_HIGHS_PAGE_SIZE:
            break
        page += 1
        time.sleep(0.5)

    if not all_records:
        return pd.DataFrame()

    df = pd.DataFrame(all_records)

    # 시총 숫자 변환 + 필터링 ($2B 이상)
    df["marketCap"] = pd.to_numeric(df["marketCap"], errors="coerce")
    df = df[df["marketCap"] >= NEW_HIGHS_MIN_MARKET_CAP].copy()

    if df.empty:
        return df

    # 시총 내림차순 정렬
    df = df.sort_values("marketCap", ascending=False).reset_index(drop=True)

    # 퍼센트 필드 ×100 변환
    pct_fields = [
        "percentChange", "percentChange1m", "percentChange3m",
        "percentChange1y", "percentChangeYtd",
    ]
    for f in pct_fields:
        if f in df.columns:
            df[f] = pd.to_numeric(df[f], errors="coerce") * 100
            df[f] = df[f].round(2)

    # 숫자 변환
    for f in ["lastPrice", "priceChange", "volume"]:
        if f in df.columns:
            df[f] = pd.to_numeric(df[f], errors="coerce")

    # 시총 포맷팅
    df["marketCap"] = df["marketCap"].apply(_format_market_cap)

    # 컬럼명 변환
    col_map = {
        "symbol": "Symbol",
        "symbolName": "Name",
        "marketCap": "Market Cap",
        "lastPrice": "Price",
        "priceChange": "Change",
        "percentChange": "Change%",
        "volume": "Volume",
        "percentChange1m": "1M%",
        "percentChange3m": "3M%",
        "percentChange1y": "52W%",
        "percentChangeYtd": "YTD%",
    }
    df = df.rename(columns=col_map)
    display_cols = [v for v in col_map.values() if v in df.columns]
    return df[display_cols].reset_index(drop=True)


def fetch_new_highs():
    """Barchart에서 52주/3개월 신고가 종목을 수집.

    Returns:
        dict: {"52주 신고가": DataFrame, "3개월 신고가": DataFrame}
    """
    try:
        session, headers = _barchart_session()
    except Exception:
        return {t["label"]: pd.DataFrame() for t in NEW_HIGHS_TARGETS}

    def _fetch(target):
        try:
            return target["label"], _fetch_single_target(
                session, headers, target["lists"], target["label"]
            )
        except Exception:
            return target["label"], pd.DataFrame()

    results = {}
    with ThreadPoolExecutor(max_workers=2) as pool:
        for label, df in pool.map(_fetch, NEW_HIGHS_TARGETS):
            results[label] = df

    return results


def fetch_overnight():
    """22:30 KST(전날) ~ 현재 사이의 선물/VIX 변동 + 주요 뉴스."""
    from zoneinfo import ZoneInfo
    _TZ_KST = ZoneInfo("Asia/Seoul")
    now = datetime.now(timezone.utc)
    now_kst = now.astimezone(_TZ_KST)

    # 기준 시각: 전날 22:30 KST
    yesterday_kst = now_kst.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
    base_time = yesterday_kst.replace(hour=22, minute=30)

    # 1) 선물 데이터 (ES=F, NQ=F) + VIX (^VIX)
    tickers = ["ES=F", "NQ=F", "^VIX"]
    result = {"futures": [], "news": []}

    try:
        data = yf.download(tickers, period="2d", interval="1h",
                           progress=False, group_by="ticker")
    except Exception:
        return None

    names = {"ES=F": "S&P 500 선물", "NQ=F": "NASDAQ 100 선물", "^VIX": "VIX"}
    for tkr in tickers:
        try:
            df = data[tkr].dropna(subset=["Close"])
            if len(df) < 2:
                continue
            # base_time에 가장 가까운 행 vs 최신 행
            base_utc = base_time.astimezone(timezone.utc)
            df_utc = df.index.tz_localize("UTC") if df.index.tz is None else df.index
            past = df_utc[df_utc <= base_utc]
            if past.empty:
                continue
            base_idx = past[-1]
            base_close = float(df.loc[df.index[df_utc == base_idx][0], "Close"])
            latest_close = float(df["Close"].iloc[-1])
            chg = latest_close - base_close
            pct = chg / base_close * 100
            result["futures"].append({
                "name": names[tkr], "ticker": tkr,
                "base": round(base_close, 2),
                "latest": round(latest_close, 2),
                "change": round(chg, 2),
                "pct": round(pct, 2),
            })
        except Exception:
            continue

    # 2) 오버나이트 뉴스 (Google News RSS, 번역만)
    query = "US stock market overnight futures"
    articles = _fetch_google_news_rss("overnight", query)
    titles = [a["title"] for a in articles[:5]]
    if titles:
        with ThreadPoolExecutor(max_workers=5) as pool:
            translated = list(pool.map(_translate_text, titles))
        for article, title_kr in zip(articles[:5], translated):
            result["news"].append({
                "title": title_kr,
                "publisher": article["publisher"],
                "url": article["url"],
            })

    return result if result["futures"] else None
