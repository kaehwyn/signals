"""Catalyst check (§5.4) using public news search (Google News RSS: free, no key).

REAL_CATALYST  a credible outside article (last 7 days) about the event/project the coin is named after.
               The article is usually about the EVENT, not the coin, and the summary says so.
HYPE_ONLY      nothing credible found, or only coin-pump / shill coverage.
None           the search itself failed -> caller keeps UNCHECKED and retries.
X search has no free API, so X is only linked in the post, not searched.
"""
import re
import time
import xml.etree.ElementTree as ET
from email.utils import parsedate_to_datetime
from typing import Optional
from urllib.parse import quote_plus

import requests

NEWS_URL = "https://news.google.com/rss/search?q={q}+when:7d&hl=en-US&gl=US&ceid=US:en"
GENERIC = {"the", "a", "an", "of", "on", "in", "and", "coin", "token", "sol", "solana", "inu", "pepe", "doge", "cat", "dog",
           "moon", "ai", "meme", "official", "real", "baby", "mini", "super", "mr", "the", "pump", "fun", "x", "of", "to"}
SHILL_DOMAINS = ("x.com", "twitter.com", "t.me", "telegram", "pump.fun", "dexscreener", "dextools", "birdeye", "geckoterminal",
                 "coinmarketcap.com/community", "binance.com/en/square", "reddit.com", "youtube.com", "tiktok.com", "medium.com")
COIN_TALK = re.compile(r"\b(meme ?coins?|tokens?|pump(ed|s)?|solana|crypto|airdrop|presale|price prediction|1000x|100x|rally|surges?)\b", re.I)


def keywords(name: str, ticker: str) -> list:
    words = [w for w in re.findall(r"[A-Za-z0-9]+", f"{name}") if len(w) >= 3 and w.lower() not in GENERIC]
    if not words and ticker and len(ticker) >= 4 and ticker.lower() not in GENERIC:
        words = [ticker]
    return words[:4]


def parse_rss(xml_text: str) -> list:
    root = ET.fromstring(xml_text)
    out = []
    for it in root.iter("item"):
        src = it.find("source")
        out.append({"title": (it.findtext("title") or "").strip(), "link": (it.findtext("link") or "").strip(),
                    "pub": it.findtext("pubDate"), "source": (src.text or "").strip() if src is not None else "",
                    "source_url": src.get("url", "") if src is not None else ""})
    return out


def judge(items: list, words: list, now: Optional[float] = None) -> tuple:
    """(tag, url, summary) from news items. Pure function for tests."""
    now = now or time.time()
    for it in items:
        title = it["title"]
        clean = re.sub(r"\s+-\s+[^-]+$", "", title)            # drop " - Source" suffix
        if not all(re.search(rf"\b{re.escape(w)}\b", clean, re.I) for w in words):
            continue
        if any(d in (it.get("source_url") or "").lower() or d in (it.get("link") or "").lower() for d in SHILL_DOMAINS):
            continue
        if COIN_TALK.search(clean):
            continue                                             # coverage of the coin pumping isn't a catalyst
        try:
            if it.get("pub") and now - parsedate_to_datetime(it["pub"]).timestamp() > 7 * 86400:
                continue
        except Exception:
            pass
        src = it.get("source") or "news"
        return "REAL_CATALYST", it["link"], f"{src}: \"{clean[:110]}\" (event coverage, not about the coin)"
    return "HYPE_ONLY", None, None


def check_catalyst(name: str, ticker: str, timeout: float = 10) -> tuple:
    words = keywords(name, ticker)
    if not words:
        return "HYPE_ONLY", None, "no specific narrative in the name"
    try:
        r = requests.get(NEWS_URL.format(q=quote_plus(" ".join(f'"{w}"' for w in words))), timeout=timeout,
                         headers={"User-Agent": "Mozilla/5.0 (spidey-bot catalyst check)"})
        if r.status_code != 200:
            return None, None, None
        return judge(parse_rss(r.text), words)
    except Exception:
        return None, None, None
