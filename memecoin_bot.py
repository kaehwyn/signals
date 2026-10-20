"""
Solana memecoin radar (PAPER mode): finds and CLASSIFIES opportunities, quality over quantity.

Every monitored token is classified from ALL available data (price action, volume, liquidity, buyers/
sellers, holders, wallet concentration, insiders/clusters, narrative + current meta) into one of:
    QUICK SCALP | NARRATIVE META | EARLY GEM | MOMENTUM | AVOID   (or NO PLAY)
Each category has its own gates, so a token is never forced into a play because one metric looks good.
Alerts (compact Discord embed, CA at the bottom) are sent only for qualified plays. If nothing qualifies,
the bot stays quiet and (once per idle_notice_minutes) says: NO QUALITY PLAYS RIGHT NOW.

Scanning is continuous: new pairs, FINAL STRETCH (pump.fun bonding curve nearly full) and MIGRATED lanes
are polled every 20-60s; every monitored token is refreshed at least once a minute (strong ~10s,
new ~15s, watch ~30s, weak 60s). Classifications are recomputed on every refresh and can change; if an
alerted token changes category, turns AVOID or breaks its invalidation level you get a short UPDATE.

Paper take-profit / stop-loss / timeout / daily-loss-limit risk management is unchanged. No wallet, no
keys, no real trades.

Setup:   pip install requests
Env:     DISCORD_WEBHOOK_URL   channel webhook                              [unchanged]
         SOLANA_RPC_URL        optional Helius/QuickNode endpoint            [unchanged]
         DEBUG_EMBED=1         add scores/fits/risk detail to embeds
         CA_TEXT=1             ALSO post the CA as plain message text (easier to copy on some phones)
         JUPITER_QUOTE_URL / JUPITER_API_KEY   optional, for the sell-route check
Run:     python memecoin_bot.py                  runs forever
         python memecoin_bot.py --max-minutes N  run N minutes, save state, exit (GitHub Actions)
         python memecoin_bot.py --test           send 4 fake sample embeds (one per play type)
         python memecoin_bot.py --report         outcomes by category + what the bot has learned
         python memecoin_bot.py --probe MINT     show what is read from RugCheck/DexScreener for a token
Optional: themes.json  ({"Theme": ["keyword", ...]}) replaces the built-in narrative themes.
"""
import csv
import json
import math
import os
import re
import signal as os_signal
import statistics
import sys
import threading
import time
import traceback
from collections import Counter, defaultdict, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests

RPC_URL = os.getenv("SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com"
DEXSCREENER = "https://api.dexscreener.com"
GECKO = "https://api.geckoterminal.com/api/v2"
GECKO_HEADERS = {"Accept": "application/json;version=20230302"}
RUGCHECK = "https://api.rugcheck.xyz/v1"  # third-party public endpoints: can change or rate-limit
JUPITER_QUOTE = os.getenv("JUPITER_QUOTE_URL") or "https://lite-api.jup.ag/swap/v1/quote"
JUPITER_KEY = os.getenv("JUPITER_API_KEY")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")  # keep secret
DEBUG_EMBED = os.getenv("DEBUG_EMBED") == "1" or "--debug" in sys.argv
CA_TEXT = os.getenv("CA_TEXT") == "1"

SOL_MINT = "So11111111111111111111111111111111111111112"
STABLES = {SOL_MINT, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
POSTED_SECONDS = 30 * 86400      # never re-signal a posted token for 30 days
TIER_RANK = {"strong": 0, "new": 1, "watch": 2, "weak": 3}
CATS = ("QUICK SCALP", "MOMENTUM", "NARRATIVE META", "EARLY GEM")
CAT_STYLE = {"QUICK SCALP": ("⚡", 0xF39C12), "MOMENTUM": ("🚀", 0x2ECC71), "NARRATIVE META": ("🧠", 0x9B59B6),
             "EARLY GEM": ("💎", 0x1ABC9C), "AVOID": ("⛔", 0xE74C3C)}
HORIZON_CP = {"QUICK SCALP": 60, "MOMENTUM": 60, "NARRATIVE META": 240, "EARLY GEM": 240}  # minutes judged for learning
RED = 0xE74C3C


@dataclass
class Config:
    # ---- candidate pool / floors
    gate_liquidity_usd: float = 8_000       # below this a token is not worth enriching (API budget)
    gate_volume_h1_usd: float = 3_000
    min_pair_age_min: float = 8
    max_pair_age_min: float = 48 * 60       # existing tokens are monitored too, up to 48h old
    dead_liquidity_usd: float = 3_000
    max_candidates: int = 400
    # ---- monitoring cadence (seconds). Every token is refreshed at least once a minute.
    interval_strong: float = 10
    interval_new: float = 15
    interval_watch: float = 30
    interval_weak: float = 60
    discovery_gecko_new_s: float = 20       # NEW PAIRS lane
    discovery_gecko_trending_s: float = 90
    discovery_dex_profiles_s: float = 30
    discovery_dex_boosts_s: float = 60
    discovery_final_s: float = 45           # FINAL STRETCH lane (pump.fun bonding pools)
    discovery_migrated_s: float = 60        # MIGRATED lane (PumpSwap pools)
    gecko_refresh_s: float = 90             # unique buyers/sellers refresh for promising tokens
    tick_seconds: float = 1.0
    max_batches_per_tick: int = 3
    workers: int = 4                        # background threads for RugCheck / RPC / Jupiter
    # ---- lanes
    graduation_mcap_usd: float = 69_000     # ASSUMPTION (historical pump.fun migration size); replaced by the
    final_stretch_pct: float = 0.80         # median of migrations the bot actually observes (>= 3)
    migrated_window_min: float = 180
    # ---- enrichment
    rugcheck_ttl_strong: float = 90
    rugcheck_ttl_watch: float = 240
    authority_recheck_s: float = 600
    use_sell_check: bool = True
    sell_check_usd: float = 20
    # ---- alert rules
    signal_threshold: float = 68            # base Mind Score; moves with market conditions
    thr_offset: dict = field(default_factory=lambda: {"QUICK SCALP": 0, "MOMENTUM": 0, "NARRATIVE META": -4, "EARLY GEM": -6})
    min_observations: int = 3
    min_observe_seconds: float = 120
    max_risk: float = 55
    min_data_coverage: float = 0.6
    max_alerts_per_token: int = 3           # first alert + up to 2 updates
    update_cooldown_s: float = 600
    idle_notice_minutes: float = 60         # post "NO QUALITY PLAYS RIGHT NOW." at most this often (0 = never)
    # ---- hard vetoes (safety overrides score)
    veto_top10_pct: float = 65
    veto_insider_pct: float = 40
    veto_liq_drop_10m: float = 0.50
    veto_liq_mcap: float = 0.008
    # ---- paper trading / risk management (unchanged behaviour)
    position_size_usd: float = 25
    take_profit: float = 0.50
    stop_loss: float = 0.20
    max_hold_min: float = 360
    max_open_positions: int = 10            # caps PAPER positions only; it never blocks alerts
    max_daily_loss_usd: float = 50
    no_price_exit_min: float = 10
    # ---- learning
    learn_min_samples: int = 30
    # ---- files
    log_file: str = "paper_trades.csv"
    signals_file: str = "signals.jsonl"
    state_file: str = "state.json"


# =====================================================================================
# HTTP with per-host rate limiting (respects API limits; never blocks the main loop long)
# =====================================================================================
class Limiter:
    def __init__(self, min_interval: float):
        self.min_interval = min_interval
        self.lock = threading.Lock()
        self.next_ok = 0.0
        self.blocked_until = 0.0
        self.calls = deque()

    def acquire(self, max_wait: float = 0.5) -> bool:
        with self.lock:
            now = time.time()
            if now < self.blocked_until:
                return False
            start = max(now, self.next_ok)
            if start - now > max_wait:
                return False
            self.next_ok = start + self.min_interval
        if start > now:
            time.sleep(start - now)
        self.calls.append(time.time())
        return True

    def penalize(self, seconds: float):
        self.blocked_until = max(self.blocked_until, time.time() + seconds)

    def per_min(self) -> int:
        cutoff = time.time() - 60
        while self.calls and self.calls[0] < cutoff:
            self.calls.popleft()
        return len(self.calls)


LIMITERS = {  # conservative: well under each provider's documented/observed limits
    "dex": Limiter(0.35),        # DexScreener pairs: 300/min documented
    "dexfeed": Limiter(1.2),     # DexScreener profiles/boosts: 60/min documented
    "gecko": Limiter(4.0),       # GeckoTerminal free tier: ~30/min (some docs say ~10/min); we use ~7/min
    "rugcheck": Limiter(1.0),
    "rpc": Limiter(0.4),
    "jupiter": Limiter(1.0),
}


def http_json(host, url, params=None, method="GET", body=None, headers=None, max_wait=0.5, timeout=10):
    """Returns (parsed_json_or_None, status). status 0 = rate-limiter said wait, -1 = network error."""
    lim = LIMITERS[host]
    if not lim.acquire(max_wait):
        return None, 0
    try:
        r = requests.request(method, url, params=params, json=body, headers=headers, timeout=timeout)
    except Exception as e:
        print(f"[warn] {host} request failed: {e}")
        return None, -1
    if r.status_code == 429:
        try:
            ra = float(r.headers.get("Retry-After", 30))
        except ValueError:
            ra = 30.0
        lim.penalize(max(5.0, min(ra, 120.0)))
        print(f"[warn] {host} rate limited; backing off")
        return None, 429
    try:
        data = r.json()
    except ValueError:
        data = None
    return data, r.status_code


def get_json(url, host="dex", **kw):
    data, status = http_json(host, url, **kw)
    return data if status == 200 else None


# =====================================================================================
# Small helpers
# =====================================================================================
def _num(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def usd(n):
    n = _num(n)
    if n is None:
        return "n/a"
    for div, suffix in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= div:
            return f"${n / div:.2f}{suffix}"
    return f"${n:,.2f}"


def pct(n):
    n = _num(n)
    return "n/a" if n is None else f"{n:+.0f}%" if abs(n) >= 10 else f"{n:+.1f}%"


def price_str(p):
    return "$" + f"{float(p):.10f}".rstrip("0").rstrip(".")


def short_addr(a):
    return f"{a[:4]}…{a[-4:]}" if a and len(a) > 10 else (a or "n/a")


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def ramp(x, lo, hi):
    """Map x onto 0..100 (lo -> 0, hi -> 100). lo > hi gives an inverse ramp. None stays None (unavailable)."""
    if x is None:
        return None
    if lo == hi:
        return 100.0 if x >= hi else 0.0
    return 100.0 * clamp((x - lo) / (hi - lo), 0.0, 1.0)


def wavg(parts):
    parts = [(v, w) for v, w in parts if v is not None]
    if not parts:
        return None
    return sum(v * w for v, w in parts) / sum(w for _, w in parts)


def parse_iso_ms(s):
    if not s:
        return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp() * 1000
    except ValueError:
        return None


# =====================================================================================
# Discord
# =====================================================================================
def notify(embed: dict, content: str = ""):
    """Post one embed (plus optional plain message text) via webhook. Never breaks the bot."""
    if not DISCORD_WEBHOOK:
        print("[warn] DISCORD_WEBHOOK_URL not set; skipping alert")
        return
    payload = {"embeds": [embed]}
    if content:
        payload["content"] = content
    try:
        for _ in range(2):  # one retry if rate limited
            r = requests.post(DISCORD_WEBHOOK, json=payload, timeout=10)
            if r.status_code == 429:
                time.sleep(float(r.json().get("retry_after", 1)))
                continue
            if r.status_code >= 400:
                print(f"[warn] discord returned {r.status_code}: {r.text[:200]}")
            break
    except Exception as e:
        print(f"[warn] discord notify failed: {e}")


# =====================================================================================
# Data models
# =====================================================================================
@dataclass
class Snap:
    t: float
    price: float
    liq: Optional[float] = None
    mcap: Optional[float] = None
    vol_m5: Optional[float] = None
    vol_h1: Optional[float] = None
    vol_h24: Optional[float] = None
    buys_m5: Optional[int] = None
    sells_m5: Optional[int] = None
    buys_h1: Optional[int] = None
    sells_h1: Optional[int] = None
    chg_m5: Optional[float] = None
    chg_h1: Optional[float] = None
    chg_h24: Optional[float] = None
    dex: str = ""


@dataclass
class Candidate:
    mint: str
    first_seen: float
    source: str = ""
    symbol: str = "?"
    name: str = ""
    description: str = ""
    links: list = field(default_factory=list)
    icon: Optional[str] = None
    pair_addr: Optional[str] = None
    url: str = ""
    created_ms: Optional[float] = None
    info: dict = field(default_factory=dict)
    snaps: deque = field(default_factory=lambda: deque(maxlen=120))
    first_snap: Optional[Snap] = None
    peak_price: float = 0.0
    first_dex: str = ""
    dex: str = ""
    migrated_t: Optional[float] = None
    tier: str = "new"
    next_check: float = 0.0
    hot_until: float = 0.0
    no_pair_since: Optional[float] = None
    dirty: bool = True
    dead_reason: str = ""
    signaled: bool = False
    # enrichment
    authority: Optional[bool] = None
    authority_t: float = 0.0
    decimals: Optional[int] = None
    rc: dict = field(default_factory=dict)
    rc_t: float = 0.0
    rc_ok_t: float = 0.0
    sell_ok: Optional[bool] = None
    sell_fail: int = 0
    sell_t: float = 0.0
    holders_hist: list = field(default_factory=list)
    top10_first: Optional[float] = None
    buyers_h1: Optional[int] = None
    sellers_h1: Optional[int] = None
    theme_hits: dict = field(default_factory=dict)
    # analysis
    f: dict = field(default_factory=dict)
    subs: dict = field(default_factory=dict)
    risk: float = 0.0
    risk_notes: list = field(default_factory=list)
    vetoes: list = field(default_factory=list)
    score: Optional[float] = None
    cheap: Optional[float] = None
    coverage: float = 0.0
    narr: dict = field(default_factory=dict)
    play: dict = field(default_factory=dict)
    blockers: list = field(default_factory=list)
    # alert state
    alert_cat: str = ""
    alert_t: float = 0.0
    alert_n: int = 0
    invalid_price: Optional[float] = None
    invalidated: bool = False


@dataclass
class Signal:
    mint: str
    symbol: str
    entry_price: float
    t0: float
    url: str
    size_usd: float
    score: float
    subs: dict
    risk: float
    entry: dict
    paper: bool = True
    learn_done: bool = False
    checkpoints: dict = field(default_factory=dict)
    max_up: float = 0.0
    max_dd: float = 0.0
    last_price: float = 0.0
    category: str = ""
    theme: str = ""
    horizon: int = 60
    invalid_price: Optional[float] = None


# =====================================================================================
# Data sources (every metric is either read from a source or left as None = unavailable)
# =====================================================================================
def snap_from_dex(pair: dict, t: float) -> Optional[Snap]:
    price = _num(pair.get("priceUsd"))
    if not price or price <= 0:
        return None
    vol, chg, tx = pair.get("volume") or {}, pair.get("priceChange") or {}, pair.get("txns") or {}
    m5, h1 = tx.get("m5") or {}, tx.get("h1") or {}
    return Snap(t=t, price=price, liq=_num((pair.get("liquidity") or {}).get("usd")),
                mcap=_num(pair.get("marketCap") or pair.get("fdv")),
                vol_m5=_num(vol.get("m5")), vol_h1=_num(vol.get("h1")), vol_h24=_num(vol.get("h24")),
                buys_m5=m5.get("buys"), sells_m5=m5.get("sells"), buys_h1=h1.get("buys"), sells_h1=h1.get("sells"),
                chg_m5=_num(chg.get("m5")), chg_h1=_num(chg.get("h1")), chg_h24=_num(chg.get("h24")),
                dex=pair.get("dexId") or "")


def snap_from_gecko(a: dict, t: float, dex: str = "") -> Optional[Snap]:
    price = _num(a.get("base_token_price_usd"))
    if not price or price <= 0:
        return None
    vol, chg, tx = a.get("volume_usd") or {}, a.get("price_change_percentage") or {}, a.get("transactions") or {}
    m5, h1 = tx.get("m5") or {}, tx.get("h1") or {}
    return Snap(t=t, price=price, liq=_num(a.get("reserve_in_usd")), mcap=_num(a.get("market_cap_usd") or a.get("fdv_usd")),
                vol_m5=_num(vol.get("m5")), vol_h1=_num(vol.get("h1")), vol_h24=_num(vol.get("h24")),
                buys_m5=m5.get("buys"), sells_m5=m5.get("sells"), buys_h1=h1.get("buys"), sells_h1=h1.get("sells"),
                chg_m5=_num(chg.get("m5")), chg_h1=_num(chg.get("h1")), chg_h24=_num(chg.get("h24")), dex=dex)


def fetch_dex_feed(path: str):
    """DexScreener profile/boost feeds -> list of {mint, description, links, icon}; None if unavailable."""
    data, st = http_json("dexfeed", f"{DEXSCREENER}{path}")
    if st != 200 or not isinstance(data, list):
        return None
    return [{"mint": d["tokenAddress"], "description": d.get("description") or "", "links": d.get("links") or [],
             "icon": d.get("icon")}
            for d in data if isinstance(d, dict) and d.get("chainId") == "solana" and d.get("tokenAddress")]


def fetch_dex_pairs(mints: list):
    """Up to 30 tokens per call -> {mint: best pair (by liquidity) where the token is the base}; None if unavailable."""
    if not mints:
        return {}
    data, st = http_json("dex", f"{DEXSCREENER}/tokens/v1/solana/{','.join(mints)}", max_wait=1.0)
    if st != 200 or data is None:
        return None
    pairs = data if isinstance(data, list) else (data.get("pairs") or [])
    want, best = set(mints), {}
    for p in pairs:
        if not isinstance(p, dict) or p.get("chainId") != "solana":
            continue
        m = (p.get("baseToken") or {}).get("address")
        if m not in want:
            continue
        liq = _num((p.get("liquidity") or {}).get("usd")) or 0
        if m not in best or liq > (_num((best[m].get("liquidity") or {}).get("usd")) or 0):
            best[m] = p
    return best


def _parse_gecko_pools(data) -> list:
    included = {i.get("id"): (i.get("attributes") or {}) for i in (data.get("included") or []) if isinstance(i, dict)}
    out, t = [], time.time()
    for pool in data.get("data") or []:
        a, rel = pool.get("attributes") or {}, pool.get("relationships") or {}
        base_id = ((rel.get("base_token") or {}).get("data") or {}).get("id") or ""
        quote_id = ((rel.get("quote_token") or {}).get("data") or {}).get("id") or ""
        base, quote = base_id.split("_", 1)[-1], quote_id.split("_", 1)[-1]
        dex = (((rel.get("dex") or {}).get("data") or {}).get("id")) or ""
        if not base:
            continue
        mint, usable = (base, True) if base not in STABLES else (quote, False)  # inverted pools: skip price data
        if not mint or mint in STABLES:
            continue
        tok = included.get(f"solana_{mint}") or {}
        symbol = tok.get("symbol") or (a.get("name") or "?").split(" / ")[0].strip()
        h1 = (a.get("transactions") or {}).get("h1") or {}
        item = {"mint": mint, "symbol": symbol, "name": tok.get("name") or symbol, "pair": a.get("address"),
                "created_ms": parse_iso_ms(a.get("pool_created_at")), "icon": tok.get("image_url"),
                "buyers_h1": h1.get("buyers"), "sellers_h1": h1.get("sellers"), "dex": dex}
        if usable:
            item["snap"] = snap_from_gecko(a, t, dex)
        out.append(item)
    return out


def fetch_gecko_pools(kind: str):
    """GeckoTerminal new_pools / trending_pools for Solana -> list of discovery items; None if unavailable."""
    data, st = http_json("gecko", f"{GECKO}/networks/solana/{kind}", params={"include": "base_token"}, headers=GECKO_HEADERS)
    return _parse_gecko_pools(data) if (st == 200 and isinstance(data, dict)) else None


def fetch_gecko_dex_pools(dex_id: str, page: int = 1):
    data, st = http_json("gecko", f"{GECKO}/networks/solana/dexes/{dex_id}/pools",
                         params={"include": "base_token", "page": page}, headers=GECKO_HEADERS)
    return _parse_gecko_pools(data) if (st == 200 and isinstance(data, dict)) else None


def fetch_gecko_dexes():
    """List Solana DEX ids on GeckoTerminal (runs once in a worker thread) -> [(id, name)]."""
    rows = []
    for page in (1, 2, 3):
        data, st = http_json("gecko", f"{GECKO}/networks/solana/dexes", params={"page": page}, headers=GECKO_HEADERS, max_wait=30)
        if st != 200 or not isinstance(data, dict):
            break
        page_rows = [r for r in (data.get("data") or []) if isinstance(r, dict)]
        rows += [(r.get("id") or "", (r.get("attributes") or {}).get("name") or "") for r in page_rows]
        if len(page_rows) < 20:
            break
    return rows


def pick_pump_dexes(rows):
    """(bonding-curve dex ids, PumpSwap-like dex ids) detected from GeckoTerminal's own list, not hard-coded."""
    bonding, swap = [], []
    for i, name in rows:
        key = f"{i} {name}".lower()
        if "pump" in key:
            (swap if "swap" in key else bonding).append(i)
    return bonding, swap


def fetch_gecko_buyers(pair_addrs: list):
    """Unique buyers/sellers (1h) for up to 30 pools in one call -> {pool address: (buyers, sellers)}; None if unavailable."""
    if not pair_addrs:
        return {}
    data, st = http_json("gecko", f"{GECKO}/networks/solana/pools/multi/{','.join(pair_addrs)}", headers=GECKO_HEADERS)
    if st != 200 or not isinstance(data, dict):
        return None
    out = {}
    for pool in data.get("data") or []:
        a = pool.get("attributes") or {}
        h1 = (a.get("transactions") or {}).get("h1") or {}
        if a.get("address") and h1.get("buyers") is not None:
            out[a["address"]] = (h1.get("buyers"), h1.get("sellers"))
    return out


def mint_info(mint):
    """(authorities_revoked, decimals); (None, None) if the RPC call failed."""
    data, _ = http_json("rpc", RPC_URL, method="POST", max_wait=30,
                        body={"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
                              "params": [mint, {"encoding": "jsonParsed"}]})
    try:
        info = data["result"]["value"]["data"]["parsed"]["info"]
        return (info.get("mintAuthority") is None and info.get("freezeAuthority") is None), info.get("decimals")
    except Exception:
        return None, None


def mint_authorities_revoked(mint):  # kept for compatibility
    return mint_info(mint)[0]


def parse_rugcheck(rep) -> dict:
    """Defensive parse of a RugCheck report. Anything missing stays None ('unavailable')."""
    rc = {"ok": False, "holders": None, "top10_pct": None, "top1_pct": None, "insider_pct": None,
          "insider_networks": None, "insider_wallets": None, "lp_locked_pct": None, "rugged": None,
          "danger_risks": None, "risk_names": [], "deployer": None, "deployer_pct": None}
    if not isinstance(rep, dict):
        return rc
    rc["ok"] = True
    th = _num(rep.get("totalHolders"))
    rc["holders"] = int(th) if th is not None else None
    known = rep.get("knownAccounts") or {}

    def is_pool(h):
        for k in (h.get("address"), h.get("owner")):
            ka = known.get(k) if k else None
            if isinstance(ka, dict) and str(ka.get("type", "")).upper() in ("AMM", "LP", "POOL"):
                return True
        return False

    holders = [h for h in (rep.get("topHolders") or []) if isinstance(h, dict) and not is_pool(h)]
    pcts = sorted((p for p in (_num(h.get("pct")) for h in holders) if p is not None), reverse=True)
    if pcts:
        rc["top10_pct"], rc["top1_pct"] = sum(pcts[:10]), pcts[0]
    if any("insider" in h for h in holders):
        rc["insider_pct"] = sum((_num(h.get("pct")) or 0.0) for h in holders if h.get("insider"))
    nets = rep.get("insiderNetworks")
    if isinstance(nets, list):
        rc["insider_networks"] = len(nets)
        wallets = sum((_num(n.get("size")) or _num(n.get("activeAccounts")) or 0) for n in nets if isinstance(n, dict))
        rc["insider_wallets"] = int(wallets) if wallets else None
    lp = [v for v in (_num(((m or {}).get("lp") or {}).get("lpLockedPct")) for m in (rep.get("markets") or [])) if v is not None]
    if lp:
        rc["lp_locked_pct"] = max(lp)
    if rep.get("rugged") is not None:
        rc["rugged"] = bool(rep.get("rugged"))
    danger = [r.get("name") for r in (rep.get("risks") or []) if isinstance(r, dict) and str(r.get("level", "")).lower() == "danger"]
    rc["danger_risks"], rc["risk_names"] = len(danger), danger
    creator = rep.get("creator")
    if isinstance(creator, str) and creator:
        rc["deployer"] = creator
        for h in holders:
            if creator in (h.get("address"), h.get("owner")):
                rc["deployer_pct"] = _num(h.get("pct"))
                break
    return rc


def fetch_rugcheck(mint) -> dict:
    return parse_rugcheck(get_json(f"{RUGCHECK}/tokens/{mint}/report", host="rugcheck", max_wait=30))


def sell_route_check(mint, price_usd, decimals, cfg) -> Optional[bool]:
    """Honeypot proxy. True: Jupiter can route a small sell to SOL. False: Jupiter says no route.
    None: unavailable (API down, unknown decimals, ...). Not a guarantee either way."""
    if not price_usd or decimals is None:
        return None
    raw = int((cfg.sell_check_usd / price_usd) * (10 ** int(decimals)))
    if raw <= 0:
        return None
    headers = {"x-api-key": JUPITER_KEY} if JUPITER_KEY else None
    data, st = http_json("jupiter", JUPITER_QUOTE, headers=headers, max_wait=30,
                         params={"inputMint": mint, "outputMint": SOL_MINT, "amount": raw, "slippageBps": 1500})
    if st == 200 and isinstance(data, dict):
        out = _num(data.get("outAmount"))
        return True if (out and out > 0) else None
    if st in (400, 404) and data is not None:
        body = json.dumps(data).lower()
        if "route" in body or "not_tradable" in body or "not tradable" in body:
            return False
    return None


def do_enrich(mint, price, decimals, need_auth, need_rc, need_sell, cfg) -> dict:
    """Runs in a worker thread; returns results only (the main thread applies them)."""
    out = {}
    if need_auth:
        rev, dec = mint_info(mint)
        out["authority"], out["decimals"] = rev, dec
        if dec is not None:
            decimals = dec
    if need_rc:
        out["rc"] = fetch_rugcheck(mint)
    if need_sell:
        out["sell"] = sell_route_check(mint, price, decimals, cfg)
    return out


# =====================================================================================
# Features, category scores, risk (rug/failure) and the general Mind Score
# =====================================================================================
WEIGHTS = {"momentum": .14, "volume": .14, "buy_sell": .10, "holders": .08, "liquidity": .12,
           "distribution": .10, "insiders": .08, "organic": .08, "narrative": .06, "continuation": .10}
CHEAP_KEYS = ("momentum", "volume", "buy_sell", "liquidity", "continuation")  # need no slow lookups
MCAP_TIERS = (30e3, 50e3, 100e3, 250e3, 500e3, 1e6, 5e6)
SEVERE_RE = re.compile(r"rug|honeypot|freeze|mint auth|blacklist|bundl|copycat|correlat|can.?t sell|cannot sell|creator.*(histor|rug)", re.I)


def snap_ago(c: Candidate, now: float, seconds: float) -> Optional[Snap]:
    """Latest snapshot taken at least `seconds` ago (None if our history is shorter than that)."""
    target, best = now - seconds, None
    for s in c.snaps:
        if s.t <= target:
            best = s
        else:
            break
    return best


def dex_stage(dex: str):
    d = (dex or "").lower()
    if not d:
        return None
    if "pumpswap" in d:
        return "graduated"
    return "bonding" if "pump" in d else "graduated"


def features(c: Candidate, now: float, cfg: Config) -> dict:
    s, s0, rc = c.snaps[-1], (c.first_snap or c.snaps[0]), (c.rc or {})
    tb, ts, mb, ms = s.buys_h1 or 0, s.sells_h1 or 0, s.buys_m5 or 0, s.sells_m5 or 0
    f = {"price": s.price, "liq": s.liq, "mcap": s.mcap, "vol_h1": s.vol_h1, "vol_m5": s.vol_m5,
         "chg_m5": s.chg_m5, "chg_h1": s.chg_h1, "chg_h24": s.chg_h24, "buys_h1": tb, "sells_h1": ts,
         "buys_m5": mb, "sells_m5": ms, "tx_h1": tb + ts,
         "br_h1": tb / (tb + ts) if tb + ts >= 20 else None,
         "br_m5": mb / (mb + ms) if mb + ms >= 6 else None,
         "obs": len(c.snaps), "span_s": s.t - s0.t}
    f["age_min"] = (now * 1000 - c.created_ms) / 60000 if c.created_ms else (now - c.first_seen) / 60
    f["vol_accel"] = (s.vol_m5 * 12) / s.vol_h1 if (s.vol_m5 is not None and s.vol_h1 and s.vol_h1 >= 1000) else None
    f["turnover"] = s.vol_h1 / s.liq if (s.vol_h1 is not None and s.liq) else None
    f["liq_mcap"] = s.liq / s.mcap if (s.liq and s.mcap) else None
    p5 = snap_ago(c, now, 300)
    f["px_chg_5m"] = s.price / p5.price - 1 if p5 else None
    recent = list(c.snaps)[-7:]
    f["rising_frac"] = (sum(1 for a, b in zip(recent, recent[1:]) if b.price >= a.price * 0.999) / (len(recent) - 1)) if len(recent) >= 4 else None
    peak = max((x.price for x in c.snaps if x.t >= now - 1800), default=s.price)
    f["dd_from_peak"] = s.price / peak - 1 if peak > 0 else None
    l10 = snap_ago(c, now, 600)
    f["liq_chg_10m"] = s.liq / l10.liq - 1 if (l10 and l10.liq and s.liq is not None) else None
    f["liq_chg_first"] = s.liq / s0.liq - 1 if (s0.liq and s.liq is not None) else None
    f["mcap_chg_first"] = s.mcap / s0.mcap - 1 if (s0.mcap and s.mcap is not None) else None
    tier_idx = lambda m: sum(1 for t in MCAP_TIERS if m >= t)
    f["tier_up"] = (tier_idx(s.mcap) - tier_idx(s0.mcap)) if (s.mcap and s0.mcap) else None
    f["dex_stage"] = dex_stage(s.dex)
    f["graduated_recent"] = bool(c.migrated_t is not None or (dex_stage(c.first_dex) == "bonding" and dex_stage(s.dex) == "graduated"))
    hh = c.holders_hist
    f["holders"] = hh[-1][1] if hh else rc.get("holders")
    growth = None
    if len(hh) >= 2:
        t_now, h_now = hh[-1]
        old = [x for x in hh if now - 3600 <= x[0] <= t_now - 180]
        if old and old[0][1] > 0:
            growth = ((h_now / old[0][1]) - 1) / ((t_now - old[0][0]) / 3600) * 100
    f["holders_growth_hr"] = growth
    f.update({"top10": rc.get("top10_pct"), "top1": rc.get("top1_pct"), "insider": rc.get("insider_pct"),
              "nets": rc.get("insider_networks"), "lp_locked": rc.get("lp_locked_pct")})
    f["buyers_h1"], f["sellers_h1"] = c.buyers_h1, c.sellers_h1
    f["buys_per_buyer"] = tb / c.buyers_h1 if (c.buyers_h1 and c.buyers_h1 >= 5) else None
    link_types = {str(x.get("type", "")).lower() for x in c.links if isinstance(x, dict)}
    has_site = bool(c.info.get("websites")) or "website" in link_types
    has_social = bool(c.info.get("socials")) or bool(link_types & {"twitter", "telegram", "discord"})
    f["social_score"] = 100.0 if (has_site and has_social) else (50.0 if (has_site or has_social) else 0.0)
    return f


def category_scores(f: dict, narr: dict) -> dict:
    s = {}
    m = wavg([(ramp(f["chg_m5"], -3, 6), .35), (ramp(f["chg_h1"], -10, 40), .25),
              (ramp(f["rising_frac"], .3, .8), .25), (ramp(f["dd_from_peak"], -.35, 0), .15)])
    if m is not None and f["chg_h1"] is not None:  # overextended pumps are not "momentum"
        m *= .35 if f["chg_h1"] > 800 else (.6 if f["chg_h1"] > 300 else 1.0)
    s["momentum"] = m
    s["volume"] = wavg([(ramp(f["vol_h1"], 5_000, 80_000), .4), (ramp(f["vol_accel"], .7, 1.8), .3),
                        (ramp(f["turnover"], 60, 15), .3)])
    bs = wavg([(ramp(f["br_h1"], .42, .62), .4), (ramp(f["br_m5"], .42, .66), .35),
               (ramp(None if (f["br_m5"] is None or f["br_h1"] is None) else f["br_m5"] - f["br_h1"], -.05, .05), .25)])
    s["buy_sell"] = bs * .7 if (bs is not None and f["tx_h1"] < 30) else bs
    s["holders"] = wavg([(ramp(f["holders"], 100, 1000), .4), (ramp(f["holders_growth_hr"], 0, 20), .6)])
    s["liquidity"] = wavg([(ramp(f["liq"], 8_000, 60_000), .35), (ramp(f["liq_mcap"], .015, .08), .25),
                           (ramp(f["liq_chg_10m"], -.25, .10), .25), (ramp(f["lp_locked"], 30, 100), .15)])
    s["distribution"] = wavg([(ramp(f["top10"], 55, 15), .7), (ramp(f["top1"], 20, 3), .3)])
    s["insiders"] = wavg([(ramp(f["insider"], 30, 3), .7), (ramp(f["nets"], 3, 0), .3)])
    s["organic"] = wavg([(ramp(f["buyers_h1"], 20, 150), .35), (ramp(f["buys_per_buyer"], 4, 1.5), .25),
                         (ramp(f["tx_h1"], 40, 400), .2), (f["social_score"], .2)])
    ns = (narr or {}).get("strength")
    s["narrative"] = ns if ns is not None else 30.0   # unclear narrative = low-neutral, never invented
    s["continuation"] = wavg([(ramp(f["span_s"] / 60, 2, 12), .2), (ramp(f["rising_frac"], .4, .8), .2),
                              (ramp(f["mcap_chg_first"], 0, .5), .2),
                              (100.0 if (f["graduated_recent"] or (f["tier_up"] or 0) >= 1) else 40.0, .15),
                              (ramp(f["liq_chg_first"], -.1, .3), .15), (ramp(f["holders_growth_hr"], 0, 20), .1)])
    return s


def risk_assessment(c: Candidate, f: dict, cfg: Config):
    """Returns (risk 0-100, notes, vetoes). Missing data adds no risk points (it is not guessed)."""
    pts, notes, vetoes = 0.0, [], []

    def add(p, note):
        nonlocal pts
        pts += p
        notes.append(note)

    rc = c.rc or {}
    if c.authority is False:
        vetoes.append("mint/freeze authority not revoked")
    if rc.get("rugged"):
        vetoes.append("RugCheck: rugged")
    names, danger = rc.get("risk_names") or [], rc.get("danger_risks") or 0
    severe = [n for n in names if SEVERE_RE.search(str(n))]
    if severe:
        vetoes.append(f"RugCheck danger: {severe[0]}")
    elif danger >= 2:
        vetoes.append(f"RugCheck: {danger} danger flags")
    elif danger == 1:
        add(25, f"RugCheck danger: {names[0] if names else '1 flag'}")
    t10, ins, nets, lp = f["top10"], f["insider"], f["nets"], f["lp_locked"]
    if t10 is not None:
        if t10 >= cfg.veto_top10_pct:
            vetoes.append(f"top-10 holders {t10:.0f}%")
        elif t10 > 40:
            add(ramp(t10, 40, cfg.veto_top10_pct) * .25, f"top-10 holders {t10:.0f}%")
    if t10 is not None and c.top10_first is not None and t10 - c.top10_first >= 10:
        add(10, "holder concentration rising")
    if ins is not None:
        if ins >= cfg.veto_insider_pct:
            vetoes.append(f"insiders hold {ins:.0f}%")
        elif ins > 10:
            add(ramp(ins, 10, cfg.veto_insider_pct) * .25, f"insiders {ins:.0f}%")
    if nets and nets >= 2:
        add(10, f"{nets} connected-wallet clusters")
    if lp is not None:
        if lp < 10:
            add(20, "LP not locked/burned")
        elif lp < 50:
            add(10, "LP mostly unlocked")
    r = f["liq_mcap"]
    if r is not None:
        if r < cfg.veto_liq_mcap:
            vetoes.append("liquidity under 0.8% of market cap")
        elif r < .03:
            add(ramp(r, .03, cfg.veto_liq_mcap) * .2, "thin liquidity vs market cap")
    lc = f["liq_chg_10m"]
    if lc is not None and lc < 0:
        if -lc >= cfg.veto_liq_drop_10m:
            vetoes.append("liquidity collapsing")
        elif -lc >= .15:
            add(ramp(-lc, .15, .5) * .35, f"liquidity down {-lc:.0%} in 10m")
    dd = f["dd_from_peak"]
    if dd is not None:
        if dd <= -.6:
            add(40, "price down 60%+ from recent peak")
        elif dd <= -.4:
            add(25, "dumping from recent peak")
    if (f["buys_m5"] + f["sells_m5"]) >= 10 and f["sells_m5"] >= 2 * f["buys_m5"]:
        add(15, "sell pressure surge (5m)")
    if f["buys_h1"] >= 40 and f["sells_h1"] <= 2:
        vetoes.append("buys but almost no sells (honeypot-like)")
    if c.sell_ok is False and c.sell_fail >= 2:
        vetoes.append("no sell route found")
    if f["turnover"] is not None and f["turnover"] > 60:
        add(15, "artificial-looking volume churn")
    if f["buys_per_buyer"] is not None and f["buys_per_buyer"] > 6:
        add(10, "same wallets buying repeatedly")
    return min(100.0, pts), notes, vetoes


def risk_factor(risk: float) -> float:
    return max(0.0, 1.0 - 1.15 * (risk / 100.0) ** 1.3)


def compute_score(subs: dict, risk: float, weights: dict):
    """General Mind Score 0-100 = weighted category average, cut by rug risk and by missing data."""
    avail = {k: v for k, v in subs.items() if k in weights and v is not None}
    if not avail:
        return None, 0.0
    cov_w, tot_w = sum(weights[k] for k in avail), sum(weights.values())
    base = sum(weights[k] * v for k, v in avail.items()) / cov_w
    coverage = cov_w / tot_w
    return base * risk_factor(risk) * (0.6 + 0.4 * coverage), coverage


# =====================================================================================
# Narrative / meta (evidence-based; says "unclear" when it cannot tell)
# =====================================================================================
DEFAULT_THEMES = {
    "Dogs": ["dog", "doge", "shib", "inu", "puppy", "pup", "woof", "bark", "bonk", "wif", "floki", "corgi", "husky", "hound", "pug"],
    "Cats": ["cat", "kitty", "kitten", "meow", "neko", "popcat", "purr", "mog", "tabby"],
    "Frogs/Pepe": ["pepe", "frog", "kek", "toad", "ribbit", "wojak"],
    "AI/agents": ["ai", "gpt", "agent", "agents", "agi", "llm", "neural", "robot", "bot", "sentient", "grok", "claude", "openai", "deepseek"],
    "Politics": ["trump", "maga", "biden", "harris", "election", "vote", "president", "senate", "congress", "republican", "democrat", "potus"],
    "Celebrity/influencer": ["elon", "musk", "kanye", "drake", "snoop", "mrbeast", "tate"],
    "Animals": ["bear", "monkey", "ape", "penguin", "hippo", "duck", "goat", "cow", "pig", "bird", "fish", "whale", "turtle", "capybara", "hamster", "squirrel", "fox", "wolf", "lion", "tiger", "panda", "shark", "owl", "bunny", "rabbit"],
    "Food": ["pizza", "burger", "taco", "cheese", "banana", "coffee", "sushi", "ramen", "donut", "fries", "bacon", "cookie"],
    "Space/sci-fi": ["moon", "mars", "rocket", "alien", "ufo", "space", "galaxy", "cosmic", "astro"],
    "Meme formats": ["chad", "gigachad", "sigma", "based", "npc", "skibidi", "rizz", "doomer", "meme", "memes", "lore"],
    "Gaming/anime": ["waifu", "anime", "manga", "otaku", "gamer", "gaming", "kawaii", "senpai"],
    "Degen culture": ["degen", "wagmi", "ngmi", "hodl", "fomo", "jeet"],
}
STOPWORDS = {"the", "and", "for", "coin", "token", "sol", "solana", "meme", "memecoin", "official", "new", "fun", "pump", "pumpfun", "inu", "of", "to", "in", "on"}
GENERIC = {"coin", "token", "moon", "pump", "sol", "solana", "meme", "memecoin", "new", "test"}


def load_themes():
    try:
        if os.path.exists("themes.json"):
            with open("themes.json") as fh:
                d = json.load(fh)
            if isinstance(d, dict) and d:
                return {k: [str(x).lower() for x in v] for k, v in d.items()}
    except Exception as e:
        print(f"[warn] themes.json ignored: {e}")
    return DEFAULT_THEMES


def _split_words(text: str):
    ws = {w.lower() for w in re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|\d+", text)}
    return ws | set(re.findall(r"[a-z0-9]+", text.lower()))


class Narrative:
    """Finds what a token is about from its name/ticker/description, then measures the CURRENT meta from the
    tokens the bot is monitoring: how many related tokens are active, how they are moving, whether group
    volume is rising, who leads. Meta stage: EARLY / DEVELOPING / TRENDING / OVERCROWDED (+ FADING)."""

    def __init__(self, cfg: Config, themes: dict):
        self.cfg, self.themes = cfg, themes
        self.groups = {}
        self.vol_hist = defaultdict(lambda: deque(maxlen=60))
        self.names = defaultdict(dict)  # normalized symbol/name -> {mint: first time seen}

    def note_name(self, c: Candidate, now: float):
        for raw in (c.symbol, c.name):
            key = re.sub(r"[^a-z0-9]", "", (raw or "").lower())
            if len(key) >= 2:
                self.names[key].setdefault(c.mint, now)

    def copycats(self, c: Candidate, now: float) -> int:
        n = set()
        for raw in (c.symbol, c.name):
            key = re.sub(r"[^a-z0-9]", "", (raw or "").lower())
            n |= {m for m, t in self.names.get(key, {}).items() if m != c.mint and now - t < 43200}
        return len(n)

    def match_themes(self, c: Candidate):
        text = f"{c.symbol} {c.name}"
        words, joined = _split_words(text), re.sub(r"[^a-z0-9]", "", text.lower())
        hits = {}
        for theme, kws in self.themes.items():
            m = [k for k in kws if k in words or (len(k) >= 4 and k in joined)]
            if m:
                hits[theme] = m
        return hits

    def desc_hits(self, c: Candidate, skip):
        if not c.description:
            return {}
        words, out = _split_words(c.description[:400]), {}
        for theme, kws in self.themes.items():
            if theme in skip:
                continue
            m = [k for k in kws if k in words]
            if len(m) >= 2:
                out[theme] = m
        return out

    def sig_words(self, c: Candidate):
        lex = {k for kws in self.themes.values() for k in kws}
        ws = {w for w in re.findall(r"[a-z]+", f"{c.symbol} {c.name}".lower()) if len(w) >= 3}
        return {w for w in ws if w not in STOPWORDS and w not in lex}

    def group_stats(self, key, members, now):
        cfg = self.cfg
        act = [m for m in members if m.snaps and (m.snaps[-1].liq or 0) >= cfg.gate_liquidity_usd and (m.snaps[-1].vol_h1 or 0) >= cfg.gate_volume_h1_usd]
        h1s = [m.snaps[-1].chg_h1 for m in act if m.snaps[-1].chg_h1 is not None]
        vol_sum = sum(m.snaps[-1].vol_h1 or 0 for m in act)
        hist = self.vol_hist[key]
        if not hist or now - hist[-1][0] >= 60:
            hist.append((now, vol_sum))
        old = [x for x in hist if x[0] <= now - 600]
        trend = (vol_sum / old[-1][1] - 1) if (old and old[-1][1] > 0) else None
        med = statistics.median(h1s) if h1s else None
        pos = (sum(1 for x in h1s if x > 0) / len(h1s)) if h1s else None
        leaders = sorted(((m.snaps[-1].chg_h1, m) for m in act if m.snaps[-1].chg_h1 is not None), key=lambda x: -x[0])
        heat = wavg([(ramp(med, -10, 40), .35), (ramp(pos, .3, .8), .25), (ramp(trend, -.2, .5), .25), (ramp(len(act), 1, 6), .15)])
        return {"n_tokens": len(members), "n_active": len(act), "med_h1": med, "pos_share": pos, "vol_sum": vol_sum,
                "vol_trend": trend, "heat": heat,
                "leaders": [(m.mint, m.symbol, chg, m.snaps[-1].vol_h1) for chg, m in leaders[:3]]}

    def refresh(self, cands, now):
        uni = [c for c in cands if c.snaps and (c.snaps[-1].liq or 0) >= self.cfg.dead_liquidity_usd and not c.dead_reason]
        by_theme, by_word = defaultdict(list), defaultdict(list)
        for c in uni:
            c.theme_hits = self.match_themes(c)
            for t in c.theme_hits:
                by_theme[t].append(c)
            for w in self.sig_words(c):
                by_word[w].append(c)
        groups = {("theme", t): self.group_stats(("theme", t), ms, now) for t, ms in by_theme.items()}
        for w, ms in by_word.items():
            if len(ms) >= 3:
                groups[("word", w)] = self.group_stats(("word", w), ms, now)
        self.groups = groups

    def board(self, top=3):
        """Hottest groups right now (for logs / idle notice)."""
        rows = [(g["heat"], k[1], g) for k, g in self.groups.items() if g["n_active"] >= 2 and g["heat"] is not None]
        return sorted(rows, key=lambda r: -r[0])[:top]

    def analyze(self, c: Candidate, now: float) -> dict:
        hits = c.theme_hits or self.match_themes(c)
        opts = [("theme", t, self.groups.get(("theme", t)), False) for t in hits]
        opts += [("cluster", w, self.groups.get(("word", w)), False) for w in self.sig_words(c)
                 if (self.groups.get(("word", w)) or {}).get("n_tokens", 0) >= 3]
        opts += [("theme", t, self.groups.get(("theme", t)), True) for t in self.desc_hits(c, set(hits))]
        if not opts:
            return {"clear": False, "label": "unclear", "strength": None}
        kind, name, g, from_desc = max(opts, key=lambda o: ((o[2] or {}).get("n_active", 0), (o[2] or {}).get("pos_share") or 0))
        g = g or {"n_tokens": 1, "n_active": 0, "med_h1": None, "pos_share": None, "vol_trend": None, "heat": None, "leaders": []}
        n_act, med, trend, pos = g["n_active"], g["med_h1"], g["vol_trend"], g["pos_share"]
        if kind == "cluster" and n_act < 2:
            return {"clear": False, "label": "unclear", "strength": None}
        copies = self.copycats(c, now)
        if trend is not None and trend <= -.25 and (med is None or med < 0):
            meta = "FADING"
        elif n_act >= 15 or copies >= 4:
            meta = "OVERCROWDED"
        elif n_act >= 5 and (pos or 0) >= .6 and ((trend or 0) >= .10 or (med or 0) >= 15):
            meta = "TRENDING"
        elif n_act >= 3:
            meta = "DEVELOPING"
        else:
            meta = "EARLY"
        confirmed = n_act >= 2
        words = {w for w in re.findall(r"[a-z]+", f"{c.symbol} {c.name}".lower())}
        generic = bool(words) and words <= GENERIC
        s = c.snaps[-1] if c.snaps else None
        rel = (s.chg_h1 - med) if (s and s.chg_h1 is not None and med is not None) else None
        base = {"EARLY": 66, "DEVELOPING": 66, "TRENDING": 60, "OVERCROWDED": 25, "FADING": 12}[meta]
        if not confirmed:
            base = 35                      # keyword match only, no sector confirmation
        if pos is not None:
            base += (pos - .5) * 40
        if rel is not None:
            base += clamp(rel / 50, -1, 1) * 10
        base -= min(30, max(0, copies - 1) * 10)
        if c.description or c.links:
            base += 5
        if generic:
            base *= .6
        if from_desc:
            base *= .8
        core = name if kind == "theme" else f'"{name}" cluster'
        label = f"{core} — {meta}" + (f" ({n_act} active related)" if n_act else " (no sector confirmation yet)")
        if copies >= 2:
            label += f", {copies} copycats"
        if generic:
            label += ", generic name"
        leader = next(((m, sym, chg, v) for m, sym, chg, v in g["leaders"] if m != c.mint), None)
        return {"clear": True, "label": label, "label_core": core, "kind": kind, "theme": name, "meta": meta,
                "confirmed": confirmed, "strength": clamp(base, 0, 100), "heat": g["heat"], "n_active": n_act,
                "med_h1": med, "pos_share": pos, "vol_trend": trend, "rel_perf": rel, "leader": leader,
                "copycats": copies, "generic": generic, "from_description": from_desc}


# =====================================================================================
# Opportunity classification (independent judgment, one set of gates per play type)
# =====================================================================================
def momentum_state(f):
    m5, rf, acc, dd, br = f["chg_m5"], f["rising_frac"], f["vol_accel"], f["dd_from_peak"], f["br_m5"]
    if m5 is None and rf is None:
        return None
    fading = (m5 is not None and m5 < -2) or (dd is not None and dd <= -.2) or (acc is not None and acc < .7 and (m5 or 0) <= 0)
    rising = (m5 is not None and m5 > 1) and (rf is None or rf >= .6) and ((acc or 0) >= 1.1 or (br or 0) >= .52)
    return "FADING" if fading else ("INCREASING" if rising else "STABLE")


def organic_label(f, subs):
    bad = []
    if f["turnover"] is not None and f["turnover"] > 60:
        bad.append("volume churn")
    if f["buys_per_buyer"] is not None and f["buys_per_buyer"] > 6:
        bad.append("same wallets buying repeatedly")
    if f["insider"] is not None and f["insider"] >= 25:
        bad.append("insider-heavy supply")
    if f["nets"] and f["nets"] >= 2:
        bad.append("connected wallet clusters")
    if bad:
        return "MANUFACTURED-LOOKING", bad
    good = (subs.get("organic") or 0) >= 55 and (f["insider"] is None or f["insider"] <= 12)
    return ("ORGANIC" if good else "MIXED"), []


def play_score(fit, risk, coverage):
    return None if fit is None else fit * risk_factor(risk) * (0.6 + 0.4 * coverage)


def lane_note(f):
    lanes = f.get("lanes") or []
    if "FINAL STRETCH" in lanes and f.get("bond_progress") is not None:
        return (f"It is in the final stretch (~{min(f['bond_progress'], 1.0):.0%} of the estimated migration size); "
                "migration can add liquidity and visibility, but can also trigger sell-the-news dumps.")
    if "MIGRATED" in lanes and f.get("mig_min") is not None:
        return f"It migrated ~{f['mig_min']:.0f} min ago, so liquidity and holder behavior after migration matter most."
    if "NEW PAIR" in lanes:
        return f"New pair, {f['age_min']:.0f} min old, so its history is short."
    return ""


def risk_text(c, f, narr, mom):
    if c.risk >= 15 and c.risk_notes:
        return c.risk_notes[0][0].upper() + c.risk_notes[0][1:] + "."
    if mom == "FADING":
        return "Momentum is fading; the setup fails quickly if buyers do not return."
    if "FINAL STRETCH" in (f.get("lanes") or []):
        return "Migration can trigger a sell-the-news dump as early holders exit."
    if narr.get("meta") in ("OVERCROWDED", "FADING"):
        return f"The {narr.get('theme')} theme is {narr['meta'].lower()}; copycats dilute attention."
    gaps = [k for k, v in (("holder data", f["holders"]), ("top-10 data", f["top10"]), ("unique buyers", f["buyers_h1"])) if v is None]
    if gaps:
        return "Some evidence is missing (" + ", ".join(gaps) + "), so confidence is limited."
    return "No major red flag found; the main risk is a normal memecoin reversal when buyers stop."


def compose_play(cat, c, f, narr, mom, org, track, gem_hits):
    s = c.snaps[-1]
    price, liq, vol1 = s.price, s.liq, s.vol_h1
    m5, h1, acc = f["chg_m5"], f["chg_h1"], f["vol_accel"]
    br = f["br_m5"] if f["br_m5"] is not None else f["br_h1"]
    note = lane_note(f)
    if cat == "QUICK SCALP":
        thesis = (f"Volume is running {acc:.1f}× its hourly pace and {br:.0%} of recent trades are buys, with price {pct(m5)} in 5m "
                  f"on {usd(liq)} liquidity: a short-term breakout setup, not a hold.")
        why = "Buyers are absorbing sells while volume accelerates. If that pressure holds, continuation over the next minutes is plausible. " + note
        inv_mult, horizon = 0.90, "MINUTES"
        inv = f"Price below {price_str(price * inv_mult)} (-10%), 5m buys under 45%, volume back under its hourly pace, or liquidity down 25% from {usd(liq)}."
    elif cat == "MOMENTUM":
        tot = f["buys_h1"] + f["sells_h1"]
        sells = f["sells_h1"] / tot if tot else None
        who = (f"{f['buyers_h1']} unique buyers at {f['buys_per_buyer']:.1f} buys each" if (f["buyers_h1"] and f["buys_per_buyer"])
               else "wallet-level data is limited")
        thesis = (f"Already {pct(h1)} in 1h on {usd(vol1)} volume with momentum {mom.lower()}. The move looks {org.lower()} ({who}) "
                  f"and price sits {abs(f['dd_from_peak'] or 0):.0%} below its 30-min high.")
        why = ("Demand is already visible with no heavy distribution yet" + (f" (sells are {sells:.0%} of 1h trades)" if sells is not None else "")
               + ". Continuation depends on volume staying at or above its hourly pace. " + note)
        inv_mult, horizon = 0.85, "HOURS"
        inv = f"Price below {price_str(price * inv_mult)} (-15%), volume under half its hourly pace, 5m buys under 45%, or liquidity down 25% from {usd(liq)}."
    elif cat == "NARRATIVE META":
        core, n, med, tr, rel = narr["label_core"], narr["n_active"], narr["med_h1"], narr["vol_trend"], narr.get("rel_perf")
        sector = f"{core} group: {n} related tokens active" + (f", median {pct(med)} 1h" if med is not None else "") + \
                 (f", group volume {tr:+.0%} over 10m" if tr is not None else "")
        where = (f"this token is only {pct(h1)}, lagging the group" if (rel is not None and rel <= -10)
                 else f"this token is {pct(h1)}" if h1 is not None else "this token has not moved yet")
        thesis = f"{sector}; {where}. Meta stage: {narr['meta']}."
        lead = narr.get("leader")
        comp = f" Comparable: {lead[1]} is {pct(lead[2])} in 1h." if lead else ""
        rec = f" The bot's past {narr['theme']} signals: {track[0]}, median {track[1]:+.0%} at their horizon." if track else ""
        absorb = f"{usd(liq)} liquidity" + (f" and {f['holders']:,} holders" if f["holders"] else "")
        why = (f"If attention keeps rotating into {core}, tokens that have not moved yet tend to catch bids; this one has {absorb} to take it.{comp}{rec} "
               "This is a hypothesis from market data; social attention is not measured. " + note)
        horizon = "DAYS" if (narr["meta"] in ("DEVELOPING", "TRENDING") and (liq or 0) >= 50_000 and f["age_min"] >= 360) else "HOURS"
        inv_mult = 0.85
        inv = (f"{core} group volume turns clearly negative" + (f" (now {tr:+.0%})" if tr is not None else "")
               + (f", the leader {lead[1]} loses 30%+" if lead else "") + f", price below {price_str(price * inv_mult)} (-15%), or liquidity down 25% from {usd(liq)}.")
    else:  # EARLY GEM
        bits = []
        if (f["holders_growth_hr"] or 0) >= 3:
            bits.append(f"holders {f['holders_growth_hr']:+.0f}%/h")
        if f["buyers_h1"]:
            bits.append(f"{f['buyers_h1']} unique buyers")
        if f["liq_chg_first"] is not None:
            bits.append(f"liquidity {f['liq_chg_first']:+.0%} since first seen")
        if acc:
            bits.append(f"volume at {acc:.1f}× its hourly pace")
        if narr.get("clear") and narr.get("meta") in ("EARLY", "DEVELOPING"):
            bits.append(f"{narr['label_core']} narrative {narr['meta'].lower()}")
        thesis = f"Not pumping yet ({pct(h1)} 1h) but early strength is building: " + ", ".join(bits) + "."
        why = ("Organic growth in holders and buyers with steady liquidity is the pattern that usually comes before visibility. "
               "No catalyst is visible yet, so this is a watch, not a chase. " + note)
        inv_mult, horizon = 0.80, "WATCHLIST"
        inv = f"Holders stop growing for 30+ min, liquidity falls below {usd((liq or 0) * 0.8)}, top-10 rises above 40%, or price below {price_str(price * inv_mult)} (-20%)."
    return {"thesis": thesis, "why": why.strip(), "risk": risk_text(c, f, narr, mom), "invalidation": inv,
            "horizon": horizon, "invalid_price": price * inv_mult}


def confidence_level(c, f, score, thr_cat, learner_n, cfg):
    pts = sum([c.coverage >= .85, score >= thr_cat + 8, f["span_s"] >= 480, c.risk <= 25,
               f["holders"] is not None and f["buyers_h1"] is not None])
    level = "HIGH" if pts >= 5 else ("MEDIUM" if pts >= 3 else "LOW")
    if level == "HIGH" and (f["holders"] is None or f["top10"] is None or learner_n < cfg.learn_min_samples):
        level = "MEDIUM"   # scoring is not yet calibrated against enough tracked outcomes
    return level


def classify_play(c: Candidate, now: float, cfg: Config, thr: float, learner) -> dict:
    f, subs, narr = c.f, c.subs, (c.narr or {})
    mom, (org, org_bad) = momentum_state(f), organic_label(f, subs)
    out = {"category": "NO PLAY", "score": None, "momentum": mom, "organic": org, "fits": {}, "scores": {}, "near": None,
           "avoid": [], "confidence": "LOW", "thesis": "", "why": "", "risk": "", "invalidation": "", "horizon": "",
           "invalid_price": None}
    avoid = list(c.vetoes)
    if c.risk >= 60:
        avoid.append(f"risk score {c.risk:.0f}/100" + (f" ({c.risk_notes[0]})" if c.risk_notes else ""))
    if org == "MANUFACTURED-LOOKING" and c.risk >= 35:
        avoid.append("manufactured-looking activity: " + ", ".join(org_bad))
    if (f["chg_h1"] or 0) > 400 and mom == "FADING":
        avoid.append("blow-off top is fading")
    if avoid:
        out.update(category="AVOID", avoid=avoid)
        return out
    g = subs.get
    m5, h1, acc = f["chg_m5"], f["chg_h1"], f["vol_accel"]
    br = f["br_m5"] if f["br_m5"] is not None else f["br_h1"]
    liq_ok = g("liquidity") is None or g("liquidity") >= 45
    not_fake = org != "MANUFACTURED-LOOKING"
    elig, fits = {}, {}
    # ---- QUICK SCALP: short-term breakout with accelerating volume and buy pressure
    elig["QUICK SCALP"] = bool(mom == "INCREASING" and (acc or 0) >= 1.3 and (br or 0) >= .53 and (m5 or 0) >= .5
                               and (h1 is None or h1 <= 120) and liq_ok and not_fake)
    fits["QUICK SCALP"] = wavg([(g("volume"), .25), (g("buy_sell"), .25), (g("momentum"), .25), (g("liquidity"), .15), (ramp(acc, 1.0, 2.2), .10)])
    # ---- MOMENTUM: already moving strongly and still healthy
    elig["MOMENTUM"] = bool(((h1 or 0) >= 25 or (m5 or 0) >= 6) and (h1 is None or h1 <= 400) and mom in ("INCREASING", "STABLE")
                            and (f["vol_h1"] or 0) >= 15_000 and (br is None or br >= .5)
                            and (f["dd_from_peak"] is None or f["dd_from_peak"] >= -.2) and not_fake and liq_ok)
    fits["MOMENTUM"] = wavg([(g("momentum"), .30), (g("volume"), .25), (g("buy_sell"), .20), (g("organic"), .15), (g("liquidity"), .10)])
    # ---- NARRATIVE META: confirmed, early-enough theme with a token that has not run yet
    track = learner.theme_record(narr.get("theme")) if narr.get("clear") else None
    lag = ramp(None if narr.get("rel_perf") is None else -narr["rel_perf"], -30, 30)
    narr_play = wavg([(narr.get("strength"), .45), (narr.get("heat"), .35), (lag, .20)])
    elig["NARRATIVE META"] = bool(narr.get("clear") and narr.get("confirmed") and narr.get("meta") in ("EARLY", "DEVELOPING", "TRENDING")
                                  and (narr.get("heat") or 0) >= 50 and (h1 is None or h1 <= 60) and mom != "FADING"
                                  and c.risk <= 40 and liq_ok and not_fake)
    fits["NARRATIVE META"] = wavg([(narr_play, .35), (g("liquidity"), .12), (g("distribution"), .10), (g("insiders"), .10),
                                   (g("buy_sell"), .10), (g("organic"), .08), (g("holders"), .10), (g("continuation"), .05)])
    # ---- EARLY GEM: not pumping, but growth signals are building
    gem_hits = []
    if (f["holders_growth_hr"] or 0) >= 3:
        gem_hits.append("holders growing")
    if (f["buyers_h1"] or 0) >= 40 and (f["buys_per_buyer"] is None or f["buys_per_buyer"] <= 3):
        gem_hits.append("organic unique buyers")
    if f["liq_chg_first"] is not None and f["liq_chg_first"] >= -0.05:
        gem_hits.append("liquidity stable or improving")
    if (acc or 0) >= 1.0:
        gem_hits.append("volume rising")
    if narr.get("clear") and narr.get("meta") in ("EARLY", "DEVELOPING"):
        gem_hits.append("developing narrative")
    elig["EARLY GEM"] = bool(len(gem_hits) >= 3 and (h1 is None or -15 <= h1 <= 45) and mom != "FADING" and f["span_s"] >= 300
                             and (g("distribution") is None or g("distribution") >= 50) and (g("insiders") is None or g("insiders") >= 55)
                             and c.risk <= 45 and liq_ok and not_fake)
    fits["EARLY GEM"] = wavg([(g("holders"), .22), (g("organic"), .20), (g("continuation"), .14), (g("liquidity"), .14),
                              (g("distribution"), .10), (g("insiders"), .10), (g("volume"), .10)])
    scores = {k: play_score(fits[k], c.risk, c.coverage) for k in CATS if elig[k] and fits[k] is not None}
    margins = {k: s - (thr + cfg.thr_offset.get(k, 0)) for k, s in scores.items()}
    out.update(fits={k: (None if v is None else round(v, 1)) for k, v in fits.items()}, scores={k: round(v, 1) for k, v in scores.items()},
               near=max(margins.values()) if margins else None)
    best = max(margins, key=lambda k: (margins[k], -CATS.index(k))) if margins else None
    if best is None or margins[best] < 0:
        return out
    parts = compose_play(best, c, f, narr, mom, org, track, gem_hits)
    out.update(category=best, score=scores[best], **parts,
               confidence=confidence_level(c, f, scores[best], thr + cfg.thr_offset.get(best, 0), len(learner.outcomes), cfg))
    return out


# =====================================================================================
# Learning: transparent, bounded, needs real sample size. Not a trained model.
# =====================================================================================
class Learner:
    """After every alert we record what happened (return at the play's horizon, max up/drawdown, died/rugged...).
    Once there are >= learn_min_samples outcomes, for each score category we compare outcomes of signals where
    that category scored high vs low (split at the median). If the difference is statistically visible
    (|z| >= 1.5) the category's weight is nudged by at most +-30%, shrunk toward 0 when the sample is small.
    Weights are re-derived from scratch every time (no drift). The recent median return also moves the alert
    threshold by -2..+4 points. It also keeps a per-theme track record used as 'comparable' evidence."""

    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.outcomes = []
        self.mult = {k: 1.0 for k in WEIGHTS}
        self.info = {}
        self.perf_adj = 0.0

    def add(self, rec: dict):
        self.outcomes = (self.outcomes + [rec])[-500:]
        self.refresh()

    @staticmethod
    def ret(o):
        return o.get("ret_h", o.get("ret_1h", 0.0))

    def theme_record(self, theme):
        rs = [self.ret(o) for o in self.outcomes if theme and o.get("theme") == theme]
        return (len(rs), statistics.median(rs)) if len(rs) >= 3 else None

    def refresh(self):
        n = len(self.outcomes)
        self.mult, self.info, self.perf_adj = {k: 1.0 for k in WEIGHTS}, {}, 0.0
        if n < self.cfg.learn_min_samples:
            return
        shrink = n / (n + 40.0)
        ys = [clamp(self.ret(o), -1.0, 2.0) for o in self.outcomes]
        for k in WEIGHTS:
            pairs = [((o.get("subs") or {}).get(k), y) for o, y in zip(self.outcomes, ys)]
            pairs = [(v, y) for v, y in pairs if v is not None]
            if len(pairs) < 20:
                self.info[k] = {"n": len(pairs), "note": "too few samples"}
                continue
            med = statistics.median(v for v, _ in pairs)
            hi, lo = [y for v, y in pairs if v >= med], [y for v, y in pairs if v < med]
            if len(hi) < 8 or len(lo) < 8:
                self.info[k] = {"n": len(pairs), "note": "uneven split"}
                continue
            eff = statistics.fmean(hi) - statistics.fmean(lo)
            se = math.sqrt(statistics.variance(hi) / len(hi) + statistics.variance(lo) / len(lo))
            z = eff / se if se > 0 else 0.0
            adj = clamp(z / 4.0, -1, 1) * 0.30 * shrink if abs(z) >= 1.5 else 0.0
            self.mult[k] = 1.0 + adj
            self.info[k] = {"n": len(pairs), "effect": eff, "z": z, "mult": 1.0 + adj}
        if n >= 20:
            m = statistics.median(ys[-20:])
            self.perf_adj = 4.0 if m < -0.10 else (-2.0 if m > 0.15 else 0.0)


# =====================================================================================
# Discord embeds: neat and organized; CA is the last field (bottom of the embed)
# =====================================================================================
def on_chain_text(c: Candidate) -> str:
    f, rc = c.f, c.rc or {}
    p = lambda v: "n/a" if v is None else f"{v:.0f}%"
    l1 = f"Top 10 {p(f['top10'])} • Largest {p(f['top1'])} • Insiders {p(f['insider'])}"
    bits = []
    if f["lp_locked"] is not None:
        bits.append(f"LP {f['lp_locked']:.0f}% locked/burned")
    bits.append("authorities revoked" if c.authority else "authorities unverified")
    if c.sell_ok is True:
        bits.append("sell route ✓")
    l2 = " • ".join(bits)
    parts = []
    if f["nets"] is not None:
        parts.append(f"{f['nets']} connected-wallet cluster{'s' if f['nets'] != 1 else ''}"
                     + (f" ({rc['insider_wallets']} wallets)" if rc.get("insider_wallets") else ""))
    if f["buys_per_buyer"] is not None:
        parts.append(f"{f['buys_per_buyer']:.1f} buys per buyer (repeat-wallet check)")
    l3 = " • ".join(parts) or "clusters / repeat wallets: n/a"
    dep = rc.get("deployer")
    l4 = (f"Deployer {short_addr(dep)}" + (f" holds {rc['deployer_pct']:.1f}%" if rc.get("deployer_pct") is not None else "")) if dep else "Deployer: n/a"
    return "\n".join([l1, l2, l3, l4, "n/a (no data source): funding links, bundled buys"])[:1000]


def narrative_text(c: Candidate, track) -> str:
    n = c.narr or {}
    if not n.get("clear"):
        return "Narrative: unclear\nMeta: not tied to a detected theme\nSocial attention: not measured"
    lines = [f"**{n['label_core']}**" + (" (from description)" if n.get("from_description") else "")]
    if n.get("confirmed"):
        bits = [f"{n['n_active']} active related"]
        if n.get("med_h1") is not None:
            bits.append(f"median {pct(n['med_h1'])} 1h")
        if n.get("vol_trend") is not None:
            bits.append(f"volume {n['vol_trend']:+.0%}/10m")
        lines.append(f"Meta: **{n['meta']}** — " + ", ".join(bits))
    else:
        lines.append("Meta: no sector confirmation yet (keyword match only)")
    if n.get("leader"):
        lines.append(f"Comparable: {n['leader'][1]} {pct(n['leader'][2])} (1h)")
    if track:
        lines.append(f"Bot's past {n['theme']} signals: {track[0]}, median {track[1]:+.0%}")
    lines.append("Social attention: not measured")
    return "\n".join(lines)[:1000]


def build_play_embed(c: Candidate, cfg: Config, thr: float, learner, debug: bool = False) -> dict:
    p, s, f = c.play, c.snaps[-1], c.f
    cat = p["category"]
    emoji, color = CAT_STYLE.get(cat, ("•", 0x95A5A6))
    acc = f.get("vol_accel")
    arrow = " ↑" if (acc is not None and acc >= 1.2) else (" ↓" if (acc is not None and acc <= .8) else "")
    br = f.get("br_h1")
    buys = f"{f['buys_h1']} / {f['sells_h1']}" + (f" ({br:.0%})" if br is not None else "")
    ub = f"{f['buyers_h1']} / {f['sellers_h1']}" if f["buyers_h1"] is not None else "n/a"
    holders = "n/a" if f["holders"] is None else f"{f['holders']:,}" + (f" ({f['holders_growth_hr']:+.0f}%/h)" if f["holders_growth_hr"] is not None else "")
    top = "n/a" if f["top10"] is None else f"{f['top10']:.0f}% / " + ("n/a" if f["top1"] is None else f"{f['top1']:.0f}%")
    ins = "n/a" if f["insider"] is None else f"{f['insider']:.0f}%"
    links = [f"[Chart]({c.url})"] if c.url else []
    links.append(f"[RugCheck](https://rugcheck.xyz/tokens/{c.mint})")
    for w in (c.info.get("websites") or []):
        if w.get("url"):
            links.append(f"[{w.get('label') or 'Website'}]({w['url']})")
    for x in (c.info.get("socials") or []):
        if x.get("url"):
            links.append(f"[{(x.get('type') or 'link').title()}]({x['url']})")
    track = learner.theme_record((c.narr or {}).get("theme")) if (c.narr or {}).get("clear") else None
    lane = f.get("lane_text")
    fields = [
        {"name": "Mind Score", "value": f"**{p['score']:.0f}/100**", "inline": True},
        {"name": "Confidence", "value": p["confidence"], "inline": True},
        {"name": "Horizon", "value": p["horizon"], "inline": True},
        {"name": "Momentum", "value": p["momentum"] or "n/a", "inline": True},
        {"name": "Market cap", "value": usd(s.mcap), "inline": True},
        {"name": "Liquidity", "value": usd(s.liq), "inline": True},
        {"name": "1h volume", "value": f"{usd(s.vol_h1)}{arrow}", "inline": True},
        {"name": "Buys / sells (1h)", "value": buys, "inline": True},
        {"name": "Unique buyers / sellers", "value": ub, "inline": True},
        {"name": "Holders", "value": holders, "inline": True},
        {"name": "Top 10 / largest", "value": top, "inline": True},
        {"name": "Insiders", "value": ins, "inline": True},
        {"name": "Narrative / meta", "value": narrative_text(c, track), "inline": False},
        {"name": "Why it could move", "value": p["why"][:1000], "inline": False},
        {"name": "On-chain", "value": on_chain_text(c), "inline": False},
        {"name": "Risk", "value": p["risk"][:500], "inline": False},
        {"name": "Invalidation", "value": p["invalidation"][:500], "inline": False},
        {"name": "Links", "value": " • ".join(links)[:1000], "inline": False},
    ]
    if debug:
        order = sorted(c.subs.items(), key=lambda kv: -(kv[1] if kv[1] is not None else -1))
        fields.append({"name": "Debug", "value": (
            "fits: " + ", ".join(f"{k} {v}" for k, v in p["fits"].items() if v is not None)
            + f"\nscores: {p['scores']}\nrisk {c.risk:.0f} ({'; '.join(c.risk_notes) or 'no flags'}) • coverage {c.coverage:.0%} • thr {thr:.0f}"
            + "\n" + ", ".join(f"{k} {'n/a' if v is None else f'{v:.0f}'}" for k, v in order))[:1000], "inline": False})
    fields.append({"name": "CA", "value": f"```{c.mint}```", "inline": False})   # bottom of the embed
    embed = {
        "title": f"{emoji} {cat} — {c.symbol}"[:256],
        "description": ((f"*{lane}*\n" if lane else "") + f"**Thesis:** {p['thesis']}")[:1500],
        "color": color,
        "fields": fields,
        "footer": {"text": f"Paper TP +{cfg.take_profit:.0%} / SL -{cfg.stop_loss:.0%} • Not financial advice"},
        "timestamp": now_iso(),
    }
    if c.url:
        embed["url"] = c.url
    if c.icon:
        embed["thumbnail"] = {"url": c.icon}
    return embed


def build_update_embed(c: Candidate, cfg: Config, kind: str, text: str, entry_price: float) -> dict:
    s, p = c.snaps[-1], c.play
    emoji, color = CAT_STYLE.get(kind, ("🔄", 0x95A5A6))
    since = (s.price / entry_price - 1) * 100 if entry_price else None
    fields = [
        {"name": "Since alert", "value": pct(since), "inline": True},
        {"name": "Momentum", "value": p.get("momentum") or "n/a", "inline": True},
        {"name": "Liquidity", "value": usd(s.liq), "inline": True},
    ]
    if p.get("score") is not None:
        fields.insert(0, {"name": "Mind Score", "value": f"**{p['score']:.0f}/100**", "inline": True})
    fields.append({"name": "CA", "value": f"```{c.mint}```", "inline": False})
    return {"title": f"{emoji} UPDATE — {c.symbol}: {kind}", "description": text[:900], "color": color, "fields": fields,
            "footer": {"text": "Classification changed with new data • Not financial advice"}, "timestamp": now_iso()}


def build_exit_embed(sig: Signal, price: float, pnl: float, reason: str) -> dict:
    change = price / sig.entry_price - 1 if sig.entry_price else 0
    return {
        "title": f"EXIT: {sig.symbol} ({reason.replace('_', ' ')})",
        "color": 0x2ECC71 if pnl >= 0 else RED,
        "fields": [
            {"name": "Entry", "value": price_str(sig.entry_price), "inline": True},
            {"name": "Now", "value": price_str(price) if price else "no price (possible rug)", "inline": True},
            {"name": "Change", "value": f"{change:+.1%}", "inline": True},
            {"name": "Paper PnL", "value": f"${pnl:.2f} on ${sig.size_usd:.0f} (ignores fees/slippage)", "inline": False},
            {"name": "CA", "value": f"```{sig.mint}```", "inline": False},
        ],
        "footer": {"text": "Paper result. Real trades do worse after fees and slippage."},
        "timestamp": now_iso(),
    }


# =====================================================================================
# The scanner
# =====================================================================================
class Bot:
    def __init__(self, cfg: Config, clock=time.time):
        self.cfg, self.clock = cfg, clock
        self.cands: dict = {}
        self.signals: dict = {}
        self.seen: dict = {}              # mint -> do not re-add before this time
        self.learner = Learner(cfg)
        self.narr = Narrative(cfg, load_themes())
        self.sent_times = deque()
        self.realized_pnl_today = 0.0
        self.day = time.strftime("%F")
        self.limit_notified_day = None
        self.sched = defaultdict(float)
        self._narr_next = self._regime_next = self._status_next = self._save_next = 0.0
        self.pool = ThreadPoolExecutor(cfg.workers) if cfg.workers > 0 else None
        self.inflight: dict = {}
        self.regime = {"heat": 0.5, "threshold": cfg.signal_threshold, "budget": 5, "known": False}
        self.grad_obs: list = []          # market caps at which we actually saw pump.fun tokens migrate
        self.pump_dexes = None            # (bonding ids, swap ids) detected from GeckoTerminal
        self.pump_dexes_t = 0.0
        self.dex_future = None
        self._lane_i = 0
        self.started = clock()
        self.last_alert_t = 0.0
        self.last_idle_t = 0.0
        self.stop = False
        self.stats = Counter()

    # ---------- files / logging ----------
    def init_files(self):
        if not os.path.exists(self.cfg.log_file):
            with open(self.cfg.log_file, "w", newline="") as fh:
                csv.writer(fh).writerow(["time", "action", "symbol", "mint", "price", "pnl_usd", "reason"])
        if not os.path.exists(self.cfg.signals_file):
            open(self.cfg.signals_file, "w").close()

    def log_csv(self, action, symbol, mint, price, pnl=0.0, reason=""):
        with open(self.cfg.log_file, "a", newline="") as fh:
            csv.writer(fh).writerow([time.strftime("%F %T"), action, symbol, mint, price, round(pnl, 2), reason])
        print(f"[{action}] {symbol} @ {price} pnl={pnl:.2f} {reason}")

    def jlog(self, rec: dict):
        with open(self.cfg.signals_file, "a") as fh:
            fh.write(json.dumps(rec, default=str) + "\n")

    def roll_day(self, now):
        today = time.strftime("%F", time.gmtime(now))
        if today != self.day:
            self.day, self.realized_pnl_today = today, 0.0

    def submit(self, fn, *a):
        if self.pool:
            return self.pool.submit(fn, *a)
        fut = Future()
        try:
            fut.set_result(fn(*a))
        except Exception as e:
            fut.set_exception(e)
        return fut

    # ---------- lanes ----------
    def grad_mcap(self) -> float:
        return statistics.median(self.grad_obs) if len(self.grad_obs) >= 3 else self.cfg.graduation_mcap_usd

    def lane_info(self, c: Candidate, f: dict, now: float) -> dict:
        cfg, lanes, prog, mig = self.cfg, [], None, None
        if f["dex_stage"] == "bonding" and f["mcap"]:
            prog = f["mcap"] / self.grad_mcap()
            if prog >= cfg.final_stretch_pct:
                lanes.append("FINAL STRETCH")
        if c.migrated_t is not None:
            mig = (now - c.migrated_t) / 60
        elif f["dex_stage"] == "graduated" and "pumpswap" in (c.dex or "").lower() and f["age_min"] <= cfg.migrated_window_min:
            mig = f["age_min"]   # PumpSwap pools are normally created at migration, so pool age ~ time since migration
        if mig is not None and mig <= cfg.migrated_window_min:
            lanes.append("MIGRATED")
        if f["age_min"] <= 60:
            lanes.append("NEW PAIR")
        text = None
        if "FINAL STRETCH" in lanes:
            text = f"FINAL STRETCH · ~{min(prog, 1.0):.0%} to migration (est.)"
        elif "MIGRATED" in lanes:
            text = f"MIGRATED · ~{mig:.0f} min ago"
        elif "NEW PAIR" in lanes:
            text = f"NEW PAIR · {f['age_min']:.0f} min old"
        return {"lanes": lanes, "lane_text": text, "bond_progress": prog, "mig_min": mig}

    def lane_pools(self, kind: str):
        """FINAL STRETCH = pump.fun bonding-curve pools; MIGRATED = PumpSwap pools. DEX ids come from GeckoTerminal's list."""
        now = time.time()
        if self.pump_dexes is not None and not (self.pump_dexes[0] or self.pump_dexes[1]) and now - self.pump_dexes_t > 600:
            self.pump_dexes, self.dex_future = None, None     # nothing found last time: look again every 10 min
        if self.pump_dexes is None:
            if self.dex_future is None:
                self.dex_future = self.submit(fetch_gecko_dexes)
            if not self.dex_future.done():
                return None
            try:
                self.pump_dexes = pick_pump_dexes(self.dex_future.result())
            except Exception:
                self.pump_dexes = ([], [])
            self.pump_dexes_t = now
            print(f"[lanes] GeckoTerminal pump dex ids: bonding={self.pump_dexes[0]} swap={self.pump_dexes[1]}")
        ids = self.pump_dexes[0] if kind == "final" else self.pump_dexes[1]
        if not ids:
            return []
        self._lane_i += 1
        return fetch_gecko_dex_pools(ids[self._lane_i % len(ids)], 1 + (self._lane_i // len(ids)) % 2)

    # ---------- discovery ----------
    def discover(self, now):
        c = self.cfg
        jobs = (("gecko_new", c.discovery_gecko_new_s, lambda: fetch_gecko_pools("new_pools")),
                ("gecko_trend", c.discovery_gecko_trending_s, lambda: fetch_gecko_pools("trending_pools")),
                ("lane_final", c.discovery_final_s, lambda: self.lane_pools("final")),
                ("lane_migrated", c.discovery_migrated_s, lambda: self.lane_pools("migrated")),
                ("dex_profiles", c.discovery_dex_profiles_s, lambda: fetch_dex_feed("/token-profiles/latest/v1")),
                ("dex_boosts", c.discovery_dex_boosts_s, lambda: fetch_dex_feed("/token-boosts/latest/v1")))
        for name, every, fn in jobs:
            if now < self.sched[name]:
                continue
            items = fn()
            self.sched[name] = now + (every if items is not None else 5)  # retry soon if rate-limited/failed
            for it in items or []:
                self.ingest(it, now, name)
        if now >= self.sched["gecko_refresh"]:
            self.sched["gecko_refresh"] = now + c.gecko_refresh_s
            self.refresh_buyers(now)

    def ingest(self, it: dict, now: float, source: str):
        mint = it["mint"]
        c = self.cands.get(mint)
        if c is None:
            if (self.seen.get(mint) or 0) > now:
                return
            if len(self.cands) >= self.cfg.max_candidates and not self.evict_weakest(now):
                return
            c = Candidate(mint=mint, first_seen=now, source=source)
            self.cands[mint] = c
            self.stats["discovered"] += 1
            self.stats[f"found_{source}"] += 1
        for k_src, k_dst in (("symbol", "symbol"), ("name", "name"), ("description", "description"), ("icon", "icon"),
                             ("pair", "pair_addr"), ("created_ms", "created_ms")):
            val, cur = it.get(k_src), getattr(c, k_dst, None)
            if val and (not cur or cur == "?"):
                setattr(c, k_dst, val)
        if it.get("links") and not c.links:
            c.links = it["links"]
        if it.get("buyers_h1") is not None:
            c.buyers_h1, c.sellers_h1 = it.get("buyers_h1"), it.get("sellers_h1")
        sn = it.get("snap")
        if sn is not None and (not c.snaps or sn.t - c.snaps[-1].t >= 5):
            self.add_snap(c, sn, now)
        self.narr.note_name(c, now)

    def evict_weakest(self, now) -> bool:
        weak = [c for c in self.cands.values() if c.tier == "weak" and not c.signaled]
        if not weak:
            return False
        w = min(weak, key=lambda c: (c.cheap or 0, -c.first_seen))
        del self.cands[w.mint]
        return True

    def refresh_buyers(self, now):
        pick = sorted((c for c in self.cands.values() if c.pair_addr and (c.tier in ("strong", "watch") or c.signaled)),
                      key=lambda c: -(c.cheap or 0))[:30]
        res = fetch_gecko_buyers([c.pair_addr for c in pick])
        for c in pick:
            if res and c.pair_addr in res:
                c.buyers_h1, c.sellers_h1 = res[c.pair_addr]
                c.dirty = True

    # ---------- refresh (batched, priority/tier driven) ----------
    def interval_for(self, c: Candidate) -> float:
        cfg = self.cfg
        return {"strong": cfg.interval_strong, "new": cfg.interval_new, "watch": cfg.interval_watch}.get(c.tier, cfg.interval_weak)

    def add_snap(self, c: Candidate, s: Snap, now: float):
        prev = c.snaps[-1] if c.snaps else None
        if prev and dex_stage(prev.dex) == "bonding" and dex_stage(s.dex) == "graduated" and c.migrated_t is None:
            c.migrated_t = now                                   # we watched this token migrate
            if s.mcap:
                self.grad_obs = (self.grad_obs + [s.mcap])[-50:]  # learn the real migration size
            c.hot_until = now + 300
        c.snaps.append(s)
        if c.first_snap is None:
            c.first_snap, c.first_dex = s, s.dex
        c.peak_price = max(c.peak_price, s.price)
        c.dex = s.dex or c.dex
        if prev and prev.price > 0:   # event triggers: important data changed -> re-check soon, re-verify safety
            if abs(s.price / prev.price - 1) >= 0.08:
                c.hot_until = now + 120
            if prev.liq and s.liq and abs(s.liq / prev.liq - 1) >= 0.20:
                c.hot_until, c.rc_t = now + 120, 0.0
            if prev.vol_m5 and s.vol_m5 and prev.vol_m5 > 0 and s.vol_m5 >= 2.5 * prev.vol_m5 and s.vol_m5 >= 1000:
                c.hot_until = now + 120
        c.dirty = True

    def apply_pair(self, c: Candidate, pair: dict, now: float):
        s = snap_from_dex(pair, now)
        if s is None:
            c.no_pair_since = c.no_pair_since or now
            return
        c.no_pair_since = None
        bt = pair.get("baseToken") or {}
        c.symbol, c.name = bt.get("symbol") or c.symbol, bt.get("name") or c.name
        c.url, c.pair_addr = pair.get("url") or c.url, pair.get("pairAddress") or c.pair_addr
        c.created_ms = pair.get("pairCreatedAt") or c.created_ms
        info = pair.get("info") or {}
        if info:
            c.info = info
            c.icon = info.get("imageUrl") or c.icon
        self.add_snap(c, s, now)

    def refresh_due(self, now):
        due = [c for c in self.cands.values() if c.next_check <= now]
        if not due:
            return
        due.sort(key=lambda c: (TIER_RANK.get(c.tier, 9), c.next_check))
        for i in range(0, min(len(due), 30 * self.cfg.max_batches_per_tick), 30):
            batch = due[i:i + 30]
            pairs = fetch_dex_pairs([c.mint for c in batch])
            if pairs is None:
                break  # rate-limited or network error: try again next tick
            self.stats["dex_calls"] += 1
            for c in batch:
                p = pairs.get(c.mint)
                if p:
                    self.apply_pair(c, p, now)
                else:
                    c.no_pair_since = c.no_pair_since or now
                    c.dirty = True
                c.next_check = now + (4 if now < c.hot_until else self.interval_for(c))

    # ---------- enrichment (slow lookups in worker threads; only for promising tokens) ----------
    def schedule_enrichment(self, now):
        cfg = self.cfg
        free = (cfg.workers if cfg.workers > 0 else 4) - len(self.inflight)
        if free <= 0:
            return
        want = []
        for c in self.cands.values():
            if c.dead_reason or c.mint in self.inflight or not c.snaps:
                continue
            s = c.snaps[-1]
            if not c.signaled:
                if (s.liq or 0) < cfg.gate_liquidity_usd or (s.vol_h1 or 0) < cfg.gate_volume_h1_usd:
                    continue
                if c.f.get("age_min", 0) < cfg.min_pair_age_min or len(c.snaps) < 2 or (c.cheap or 0) < 35:
                    continue
            ttl = cfg.rugcheck_ttl_strong if (c.tier == "strong" or c.signaled) else cfg.rugcheck_ttl_watch
            need_rc = now - c.rc_t >= ttl
            need_auth = c.authority is not True and now - c.authority_t >= cfg.authority_recheck_s
            need_sell = (cfg.use_sell_check and c.sell_ok is not True and c.sell_fail < 3 and bool(c.rc.get("ok"))
                         and (c.cheap or 0) >= 50 and now - c.sell_t >= 120)
            if need_rc or need_auth or need_sell:
                want.append((c, need_auth, need_rc, need_sell))
        want.sort(key=lambda x: -((x[0].cheap or 0) + (100 if x[0].signaled else 0)))
        for c, na, nr, ns in want[:free]:
            s = c.snaps[-1]
            self.inflight[c.mint] = self.submit(do_enrich, c.mint, s.price, c.decimals, na, nr, ns, cfg)
            self.stats["enrich_calls"] += 1
            if nr:
                c.rc_t = now
            if na:
                c.authority_t = now
            if ns:
                c.sell_t = now

    def harvest(self, now):
        cfg = self.cfg
        for mint, fut in list(self.inflight.items()):
            if not fut.done():
                continue
            del self.inflight[mint]
            c = self.cands.get(mint)
            try:
                res = fut.result()
            except Exception as e:
                print(f"[warn] enrichment failed for {mint}: {e}")
                continue
            if c is None:
                continue
            if "authority" in res:
                if res["authority"] is not None:
                    c.authority = res["authority"]
                else:
                    c.authority_t = now - cfg.authority_recheck_s + 20   # RPC failed: retry soon
                if res.get("decimals") is not None:
                    c.decimals = res["decimals"]
            if "rc" in res:
                rc = res["rc"]
                if rc.get("ok"):
                    c.rc, c.rc_ok_t = rc, now
                    if c.top10_first is None and rc.get("top10_pct") is not None:
                        c.top10_first = rc["top10_pct"]
                    h = rc.get("holders")
                    if h is not None and (not c.holders_hist or now - c.holders_hist[-1][0] >= 60):
                        c.holders_hist = (c.holders_hist + [(now, h)])[-60:]
                else:  # RugCheck failed or was rate-limited: retry in ~30s instead of waiting a full TTL
                    ttl = cfg.rugcheck_ttl_strong if (c.tier == "strong" or c.signaled) else cfg.rugcheck_ttl_watch
                    c.rc_t = now - ttl + 30
            if "sell" in res:
                if res["sell"] is True:
                    c.sell_ok, c.sell_fail = True, 0
                elif res["sell"] is False:
                    c.sell_ok, c.sell_fail = False, c.sell_fail + 1
            c.dirty = True

    # ---------- evaluation ----------
    def weights(self):
        return {k: WEIGHTS[k] * self.learner.mult.get(k, 1.0) for k in WEIGHTS}

    def dead_reason(self, c: Candidate, f: dict, now: float) -> str:
        cfg = self.cfg
        if c.no_pair_since and now - c.no_pair_since > 600:
            return "no trading pair for 10 min"
        s = c.snaps[-1]
        if f["age_min"] > cfg.max_pair_age_min:
            return "too old"
        if s.liq is not None and s.liq < cfg.dead_liquidity_usd and f["obs"] >= 3:
            return "liquidity gone"
        if f["dd_from_peak"] is not None and f["dd_from_peak"] <= -0.85:
            return "price down 85% from peak"
        if f["age_min"] > 45 and (s.vol_h1 or 0) < 300:
            return "volume dried up"
        if c.tier == "weak" and now - c.first_seen > 7200 and (c.cheap or 0) < 35:
            return "stale weak candidate"
        return ""

    def classify(self, c: Candidate, now: float) -> str:
        if c.signaled:
            return "strong" if now - (self.signals[c.mint].t0 if c.mint in self.signals else now) < 3600 else "watch"
        if len(c.snaps) < self.cfg.min_observations or now - c.first_seen < 90:
            return "new"
        near = (c.play or {}).get("near")
        if now < c.hot_until or (near is not None and near >= -12) or (c.cheap or 0) >= 60:
            return "strong"
        return "watch" if (c.cheap or 0) >= 40 else "weak"

    def evaluate(self, c: Candidate, now: float):
        f = features(c, now, self.cfg)
        f.update(self.lane_info(c, f, now))
        c.f = f
        c.narr = self.narr.analyze(c, now)
        c.subs = category_scores(f, c.narr)
        c.risk, c.risk_notes, c.vetoes = risk_assessment(c, f, self.cfg)
        w = self.weights()
        c.score, c.coverage = compute_score(c.subs, c.risk, w)
        c.cheap = wavg([(c.subs.get(k), w[k]) for k in CHEAP_KEYS])
        reason = self.dead_reason(c, f, now)
        if not reason and c.vetoes:
            reason = "veto: " + c.vetoes[0]
        if reason:
            c.dead_reason = reason
            if not c.signaled:
                self.drop(c, now, reason)
                return False
        c.play = classify_play(c, now, self.cfg, self.regime["threshold"], self.learner)
        c.tier = self.classify(c, now)
        return True

    def drop(self, c: Candidate, now: float, reason: str):
        self.cands.pop(c.mint, None)
        self.seen[c.mint] = now + (86400 if reason.startswith("veto") else 6 * 3600 if reason == "too old" else 3600)
        self.stats["dropped"] += 1
        if reason.startswith("veto"):
            self.stats["vetoed"] += 1
        print(f"[drop] {c.symbol} ({c.mint[:6]}…): {reason}")

    def process_dirty(self, now):
        for c in list(self.cands.values()):
            if c.dirty and c.snaps:
                c.dirty = False
                if self.evaluate(c, now):
                    self.consider_signal(c, now)

    # ---------- alerts ----------
    def consider_signal(self, c: Candidate, now: float):
        cfg, f, p, reg, b = self.cfg, c.f, c.play, self.regime, []
        if c.signaled:
            return self.maybe_update(c, now)
        cat = p["category"]
        if cat in ("AVOID", "NO PLAY"):
            c.blockers = [("avoid: " + "; ".join(p["avoid"][:1])) if cat == "AVOID" else
                          (f"no play qualified" + (f" (closest {p['near']:+.0f} vs threshold)" if p.get("near") is not None else ""))]
            return
        if self.realized_pnl_today <= -cfg.max_daily_loss_usd:
            b.append("daily paper-loss limit")
            if self.limit_notified_day != self.day:
                self.limit_notified_day = self.day
                notify({"title": "Daily paper-loss limit reached",
                        "description": "Alerts are paused until UTC midnight (risk management).", "color": RED})
        if len(c.snaps) < cfg.min_observations or now - c.first_seen < cfg.min_observe_seconds:
            b.append("still observing")
        if f["age_min"] < cfg.min_pair_age_min:
            b.append("too new")
        if c.authority is not True:
            b.append("authorities unverified")
        if not c.rc.get("ok") or now - c.rc_ok_t > 600:
            b.append("RugCheck pending")
        if c.risk > cfg.max_risk:
            b.append(f"risk {c.risk:.0f}")
        if c.coverage < cfg.min_data_coverage:
            b.append(f"data coverage {c.coverage:.0%}")
        while self.sent_times and self.sent_times[0] < now - 3600:
            self.sent_times.popleft()
        thr_c = reg["threshold"] + cfg.thr_offset.get(cat, 0)
        if not b and len(self.sent_times) >= reg["budget"] and p["score"] < thr_c + 8:
            b.append("hourly alert budget (only standout plays)")
        c.blockers = b
        if not b:
            self.fire_signal(c, now)

    def fire_signal(self, c: Candidate, now: float):
        cfg, s, f, p = self.cfg, c.snaps[-1], c.f, c.play
        paper = sum(1 for x in self.signals.values() if x.paper) < cfg.max_open_positions
        theme = (c.narr or {}).get("theme") if (c.narr or {}).get("clear") else ""
        sig = Signal(mint=c.mint, symbol=c.symbol, entry_price=s.price, t0=now, url=c.url, size_usd=cfg.position_size_usd,
                     score=p["score"], subs=dict(c.subs), risk=c.risk, paper=paper, category=p["category"], theme=theme or "",
                     horizon=HORIZON_CP[p["category"]], invalid_price=p["invalid_price"],
                     entry={"liq": s.liq, "vol_h1": s.vol_h1, "mcap": s.mcap, "holders": f["holders"],
                            "top10": f["top10"], "insider": f["insider"]})
        self.signals[c.mint] = sig
        c.signaled, c.tier = True, "strong"
        c.alert_cat, c.alert_t, c.alert_n, c.invalid_price = p["category"], now, 1, p["invalid_price"]
        self.seen[c.mint] = now + POSTED_SECONDS
        self.sent_times.append(now)
        self.last_alert_t = now
        self.stats["signals"] += 1
        self.log_csv("SIGNAL", c.symbol, c.mint, s.price, reason=p["category"])
        self.jlog({"type": "signal", "t": now_iso(), "mint": c.mint, "symbol": c.symbol, "category": p["category"],
                   "price": s.price, "score": round(p["score"], 2), "confidence": p["confidence"], "horizon": p["horizon"],
                   "coverage": round(c.coverage, 3), "risk": round(c.risk, 1), "risk_notes": c.risk_notes, "subs": c.subs,
                   "fits": p["fits"], "play_scores": p["scores"], "features": c.f, "narrative": c.narr,
                   "thesis": p["thesis"], "why": p["why"], "risk_text": p["risk"], "invalidation": p["invalidation"],
                   "threshold": self.regime["threshold"], "heat": self.regime["heat"], "paper": paper})
        notify(build_play_embed(c, cfg, self.regime["threshold"], self.learner, DEBUG_EMBED), content=f"`{c.mint}`" if CA_TEXT else "")

    def maybe_update(self, c: Candidate, now: float):
        """An alerted token is still being judged: tell the user if the classification materially changes."""
        cfg, p, s = self.cfg, c.play, c.snaps[-1]
        sig = self.signals.get(c.mint)
        if not sig or c.alert_n >= cfg.max_alerts_per_token:
            return
        cat, msg = p["category"], None
        if cat == "AVOID" and c.alert_cat != "AVOID":
            msg = ("AVOID", "Classification changed to AVOID: " + "; ".join(p["avoid"][:2]) + ".")
        elif not c.invalidated and c.invalid_price and s.price < c.invalid_price:
            c.invalidated = True
            msg = ("INVALIDATED", f"Price {price_str(s.price)} broke the invalidation level {price_str(c.invalid_price)}; the setup no longer holds.")
        elif cat in CATS and cat != c.alert_cat and now - c.alert_t >= cfg.update_cooldown_s:
            msg = (cat, f"Evidence changed: {c.alert_cat} → {cat}. {p['thesis']}")
            c.invalid_price, sig.category, sig.invalid_price = p["invalid_price"], cat, p["invalid_price"]
        if msg:
            c.alert_cat = msg[0] if msg[0] in CATS or msg[0] == "AVOID" else c.alert_cat
            c.alert_t, c.alert_n, self.last_alert_t = now, c.alert_n + 1, now
            self.jlog({"type": "update", "t": now_iso(), "mint": c.mint, "symbol": c.symbol, "kind": msg[0], "text": msg[1]})
            notify(build_update_embed(c, cfg, msg[0], msg[1], sig.entry_price), content=f"`{c.mint}`" if CA_TEXT else "")

    def maybe_idle_notice(self, now):
        m = self.cfg.idle_notice_minutes
        if m <= 0 or now - self.started < m * 60 or now - max(self.last_alert_t, self.last_idle_t) < m * 60:
            return
        self.last_idle_t = now
        notify(self.build_idle_embed(now))

    def lane_counts(self):
        return Counter(l for c in self.cands.values() for l in (c.f.get("lanes") or []))

    def near_misses(self, n=3):
        rows = [(c.play.get("near"), c) for c in self.cands.values() if c.play and c.play.get("near") is not None and not c.signaled]
        rows += [(None, c) for c in sorted((c for c in self.cands.values() if c.score is not None and not c.signaled and
                                            not (c.play or {}).get("near") is not None), key=lambda c: -c.score)[:n]]
        rows.sort(key=lambda r: (r[0] is None, -(r[0] or 0)))
        return [c for _, c in rows[:n]]

    def build_idle_embed(self, now) -> dict:
        lanes, r = self.lane_counts(), self.regime
        scanned = (f"Monitoring {len(self.cands)} tokens • new pairs {lanes['NEW PAIR']} • final stretch {lanes['FINAL STRETCH']} • "
                   f"migrated {lanes['MIGRATED']}\nDiscovered this run: {self.stats['discovered']} • rejected: {self.stats['vetoed']} unsafe")
        watch = []
        for c in self.near_misses(3):
            p = c.play or {}
            watch.append(f"{c.symbol}: {p.get('category', 'n/a')} • " + ("; ".join(c.blockers[:2]) or "building history"))
        board = ", ".join(f"{name} ({heat:.0f})" for heat, name, _ in self.narr.board(3)) or "no confirmed theme groups yet"
        fields = [{"name": "Scanning", "value": scanned[:1000], "inline": False},
                  {"name": "Closest candidates", "value": ("\n".join(watch) or "none worth watching")[:1000], "inline": False},
                  {"name": "Current meta (hottest groups)", "value": board[:500], "inline": False}]
        return {"title": "NO QUALITY PLAYS RIGHT NOW.", "description": "Scanning continues every minute. Nothing met the quality bar.",
                "color": 0x95A5A6, "fields": fields,
                "footer": {"text": f"Market heat {r['heat']:.2f}{'' if r['known'] else ' (not enough data yet)'} • alert threshold {r['threshold']:.0f}"},
                "timestamp": now_iso()}

    # ---------- paper trades, tracking and learning ----------
    def manage_signals(self, now):
        cfg = self.cfg
        for mint, sig in list(self.signals.items()):
            c = self.cands.get(mint)
            s = c.snaps[-1] if (c and c.snaps) else None
            age_min = (now - sig.t0) / 60
            price = s.price if (s and now - s.t < 180) else None
            died, rugged = False, False
            if price:
                sig.last_price = price
                chg = price / sig.entry_price - 1
                sig.max_up, sig.max_dd = max(sig.max_up, chg), min(sig.max_dd, chg)
                for cp in (5, 15, 30, 60, 240):
                    if age_min >= cp and str(cp) not in sig.checkpoints:
                        sig.checkpoints[str(cp)] = {"price": price, "liq": s.liq, "vol_h1": s.vol_h1, "mcap": s.mcap,
                                                    "holders": (c.holders_hist[-1][1] if c and c.holders_hist else None)}
                if sig.entry.get("liq") and s.liq is not None and s.liq < 0.3 * sig.entry["liq"]:
                    died = True
                if chg <= -0.8:
                    died = True
            if c and c.no_pair_since and now - c.no_pair_since >= cfg.no_price_exit_min * 60:
                died, rugged, price = True, True, 0.0
            if c and (c.rc or {}).get("rugged"):
                died = rugged = True
            # ---- paper TP / SL / timeout (unchanged risk management)
            if sig.paper and price is not None:
                change = price / sig.entry_price - 1
                reason = ("no_price" if (price == 0.0 and rugged) else "take_profit" if change >= cfg.take_profit
                          else "stop_loss" if change <= -cfg.stop_loss else "timeout" if age_min >= cfg.max_hold_min else None)
                if reason:
                    pnl = sig.size_usd * change
                    self.realized_pnl_today += pnl
                    sig.paper = False
                    self.log_csv("EXIT", sig.symbol, mint, price, pnl, reason)
                    self.jlog({"type": "outcome", "t": now_iso(), "mint": mint, "symbol": sig.symbol, "category": sig.category,
                               "pnl": round(pnl, 2), "reason": reason, "win": pnl > 0, "score": round(sig.score, 1)})
                    notify(build_exit_embed(sig, price, pnl, reason), content=f"`{mint}`" if CA_TEXT else "")
            # ---- learning outcome at the play's own horizon (1h for scalps/momentum, 4h for narrative/gems)
            if not sig.learn_done and (str(sig.horizon or 60) in sig.checkpoints or died):
                self.finalize_learning(sig, died, rugged, now)
            if not sig.paper and sig.learn_done:
                del self.signals[mint]
                if c:
                    self.cands.pop(mint, None)

    def finalize_learning(self, sig: Signal, died: bool, rugged: bool, now: float):
        cps, hz = sig.checkpoints, str(sig.horizon or 60)

        def ret(cp):
            d = cps.get(str(cp))
            return d["price"] / sig.entry_price - 1 if d else None

        r = ret(hz)
        if r is None:
            r = (sig.last_price / sig.entry_price - 1) if sig.last_price else -1.0
        if died:
            r = min(r, -0.8) if sig.last_price else -1.0
        dh, e = cps.get(hz) or {}, sig.entry
        rel = lambda a, b: (a / b - 1) if (a is not None and b) else None
        rec = {"t": now_iso(), "mint": sig.mint, "symbol": sig.symbol, "category": sig.category, "theme": sig.theme,
               "horizon": int(hz), "score": round(sig.score, 2), "risk": round(sig.risk, 1), "subs": sig.subs,
               "ret_5m": ret(5), "ret_15m": ret(15), "ret_30m": ret(30), "ret_1h": ret(60), "ret_4h": ret(240), "ret_h": r,
               "max_up": sig.max_up, "max_dd": sig.max_dd, "liq_chg": rel(dh.get("liq"), e.get("liq")),
               "vol_chg": rel(dh.get("vol_h1"), e.get("vol_h1")), "holders_chg": rel(dh.get("holders"), e.get("holders")),
               "died": died, "rugged": rugged,
               "continued": bool(r >= 0.10 and not died and (rel(dh.get("vol_h1"), e.get("vol_h1")) or 0) > -0.5)}
        sig.learn_done = True
        self.learner.add(rec)
        self.jlog(dict(rec, type="learn_outcome"))

    # ---------- market regime (dynamic selectivity) ----------
    def update_regime(self, now):
        if now < self._regime_next:
            return
        cfg, self._regime_next = self.cfg, now + 20
        act = [c for c in self.cands.values() if c.snaps and not c.dead_reason and (c.snaps[-1].liq or 0) >= cfg.gate_liquidity_usd
               and (c.snaps[-1].vol_h1 or 0) >= cfg.gate_volume_h1_usd]
        if len(act) < 8:
            heat, known = 0.5, False
        else:
            pos = sum(1 for c in act if (c.snaps[-1].chg_h1 or 0) > 0) / len(act)
            brs = [c.f["br_h1"] for c in act if c.f.get("br_h1") is not None]
            med_br = statistics.median(brs) if brs else None
            strong = sum(1 for c in act if c.tier == "strong") / len(act)
            heat = 0.5 * pos + 0.3 * ((ramp(med_br, .45, .58) / 100.0) if med_br is not None else 0.5) + 0.2 * min(1.0, strong * 5)
            known = True
        self.regime = {"heat": heat, "known": known,
                       "threshold": clamp(cfg.signal_threshold + (0.5 - heat) * 12 + self.learner.perf_adj, 62, 88),
                       "budget": int(round(2 + 8 * heat))}

    # ---------- persistence ----------
    def save_state(self):
        st = {"day": self.day, "realized_pnl_today": self.realized_pnl_today,
              "seen": dict(list(self.seen.items())[-3000:]), "signals": [vars(s) for s in self.signals.values()],
              "learner": {"outcomes": self.learner.outcomes}, "sent_times": list(self.sent_times),
              "limit_notified_day": self.limit_notified_day, "grad_obs": self.grad_obs,
              "last_alert_t": self.last_alert_t, "last_idle_t": self.last_idle_t}
        tmp = self.cfg.state_file + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(st, fh, default=str)
        os.replace(tmp, self.cfg.state_file)

    def load_state(self, now=None):
        if not os.path.exists(self.cfg.state_file):
            return
        now = now or self.clock()
        try:
            with open(self.cfg.state_file) as fh:
                s = json.load(fh)
            self.day = s.get("day", self.day)
            self.realized_pnl_today = s.get("realized_pnl_today", 0.0)
            seen = s.get("seen", {})
            seen = seen if isinstance(seen, dict) else dict.fromkeys(seen, now + 1800)
            self.seen = {k: (v if isinstance(v, (int, float)) else now + 1800) for k, v in seen.items()}
            self.sent_times = deque(s.get("sent_times", []))
            self.limit_notified_day = s.get("limit_notified_day")
            self.grad_obs = s.get("grad_obs", [])
            self.last_alert_t, self.last_idle_t = s.get("last_alert_t", 0.0), s.get("last_idle_t", 0.0)
            self.learner.outcomes = (s.get("learner") or {}).get("outcomes", [])
            self.learner.refresh()
            for d in s.get("signals", []):
                try:
                    self.signals[d["mint"]] = Signal(**d)
                except TypeError:
                    pass
            for p in s.get("positions", []):  # migrate open paper positions from older versions
                try:
                    self.signals[p["mint"]] = Signal(mint=p["mint"], symbol=p.get("symbol", "?"), entry_price=p["entry_price"],
                                                     t0=p.get("opened_at", now), url=p.get("url", ""), size_usd=p.get("size_usd", self.cfg.position_size_usd),
                                                     score=0.0, subs={}, risk=0.0, entry={}, paper=True, learn_done=True)
                except Exception:
                    pass
            for mint, sig in self.signals.items():  # keep tracking open signals after a restart
                c = Candidate(mint=mint, first_seen=now, source="restored", symbol=sig.symbol, url=sig.url, signaled=True,
                              tier="strong", alert_cat=sig.category, alert_n=1, alert_t=sig.t0, invalid_price=sig.invalid_price)
                self.cands[mint] = c
        except Exception as e:
            print(f"[warn] could not load state, starting fresh: {e}")

    # ---------- main loop ----------
    def tick(self, now):
        self.roll_day(now)
        self.discover(now)
        self.harvest(now)
        self.refresh_due(now)
        if now >= self._narr_next:
            self._narr_next = now + 30
            self.narr.refresh(list(self.cands.values()), now)
        self.update_regime(now)
        self.process_dirty(now)
        self.schedule_enrichment(now)
        self.harvest(now)
        self.manage_signals(now)
        self.maybe_idle_notice(now)

    def status(self, now):
        tiers, lanes = Counter(c.tier for c in self.cands.values()), self.lane_counts()
        top = sorted((c for c in self.cands.values() if c.score is not None), key=lambda c: -c.score)[:5]
        r = self.regime
        print(f"[status] {now_iso()} monitored={len(self.cands)} {dict(tiers)} | lanes: new={lanes['NEW PAIR']} "
              f"final-stretch={lanes['FINAL STRETCH']} migrated={lanes['MIGRATED']} | open signals={len(self.signals)} sent={self.stats['signals']} | "
              f"heat={r['heat']:.2f}{'' if r['known'] else '(unknown)'} thr={r['threshold']:.0f} budget={r['budget']}/h | "
              f"api/min dex={LIMITERS['dex'].per_min()} feed={LIMITERS['dexfeed'].per_min()} gecko={LIMITERS['gecko'].per_min()} "
              f"rugcheck={LIMITERS['rugcheck'].per_min()} | vetoed={self.stats['vetoed']} learned={len(self.learner.outcomes)}")
        if not any((c.play or {}).get("category") in CATS and not c.signaled for c in self.cands.values()):
            print("   NO QUALITY PLAYS RIGHT NOW.")
        for c in top:
            p = c.play or {}
            print(f"   {c.symbol:<10} mind={c.score:5.1f} risk={c.risk:4.0f} play={p.get('category', '-'):<14} tier={c.tier:<6} "
                  f"lane={(c.f.get('lanes') or ['-'])[0]:<13} blockers={'; '.join(c.blockers) or '-'}")

    def run(self, max_minutes=None):
        self.init_files()
        self.load_state()
        for sg in (os_signal.SIGINT, os_signal.SIGTERM):
            try:
                os_signal.signal(sg, lambda *_: setattr(self, "stop", True))
            except ValueError:
                pass
        print(f"Radar started (paper mode). Ctrl+C to stop. Webhook {'set' if DISCORD_WEBHOOK else 'NOT set'}.")
        end = time.time() + max_minutes * 60 if max_minutes else None
        while not self.stop:
            now = self.clock()
            try:
                self.tick(now)
            except Exception:
                traceback.print_exc()
            if now >= self._status_next:
                self._status_next = now + 60
                self.status(now)
            if now >= self._save_next:
                self._save_next = now + 60
                try:
                    self.save_state()
                except Exception as e:
                    print(f"[warn] save failed: {e}")
            if end and time.time() >= end:
                break
            time.sleep(self.cfg.tick_seconds)
        try:
            self.save_state()
        finally:
            if self.pool:
                self.pool.shutdown(wait=False, cancel_futures=True)
        print("Radar stopped; state saved.")


# =====================================================================================
# Tools
# =====================================================================================
def report(cfg: Config):
    bot = Bot(cfg, clock=time.time)
    bot.load_state()
    outs, paper = bot.learner.outcomes, []
    if os.path.exists(cfg.signals_file):
        with open(cfg.signals_file) as fh:
            for line in fh:
                try:
                    r = json.loads(line)
                except ValueError:
                    continue
                if r.get("type") == "outcome":
                    paper.append(r)
    print(f"Paper trades closed: {len(paper)}" + (f" | win rate {sum(1 for p in paper if p['win']) / len(paper):.0%} | PnL ${sum(p['pnl'] for p in paper):.2f}" if paper else ""))
    print(f"Alerts with a full outcome (1h for scalp/momentum, 4h for narrative/gem): {len(outs)}")
    for cat in CATS:
        b = [o for o in outs if o.get("category") == cat]
        if b:
            rs = [Learner.ret(o) for o in b]
            print(f"  {cat:<15} n={len(b):<3} median {statistics.median(rs):+.0%} | continued {sum(1 for o in b if o.get('continued')) / len(b):.0%} | died/rugged {sum(1 for o in b if o.get('died')) / len(b):.0%}")
    if outs:
        for lo, hi, name in ((0, 70, "score < 70"), (70, 80, "score 70-80"), (80, 101, "score 80+")):
            b = [o for o in outs if lo <= o["score"] < hi]
            if b:
                print(f"  {name}: n={len(b)}, median return {statistics.median(Learner.ret(o) for o in b):+.0%}")
    print(f"\nLearning: {'ACTIVE' if len(outs) >= cfg.learn_min_samples else f'waiting for {cfg.learn_min_samples} outcomes (have {len(outs)}); weights are the defaults'}")
    print(f"Alert threshold adjustment from recent results: {bot.learner.perf_adj:+.0f}")
    print(f"Estimated migration size: ${bot.grad_mcap():,.0f} ({'observed' if len(bot.grad_obs) >= 3 else 'assumed default'}; {len(bot.grad_obs)} migrations seen)")
    print("Category weight multipliers (1.00 = default; bounded 0.70-1.30):")
    for k in WEIGHTS:
        i = bot.learner.info.get(k, {})
        extra = f"n={i['n']}, effect={i['effect']:+.2f}, z={i['z']:+.1f}" if "effect" in i else i.get("note", "")
        print(f"  {k:13s} x{bot.learner.mult[k]:.2f}  {extra}")


def probe(mint: str):
    rep = get_json(f"{RUGCHECK}/tokens/{mint}/report", host="rugcheck", max_wait=30)
    if not rep:
        print("No data (RugCheck unreachable, rate-limited, or unknown token).")
    else:
        print("top-level keys:", sorted(rep.keys()))
        print("totalHolders:", rep.get("totalHolders"), "| creator:", rep.get("creator"))
        print("topHolders[:3]:", json.dumps((rep.get("topHolders") or [])[:3], indent=1)[:1500])
        print("insiderNetworks:", json.dumps(rep.get("insiderNetworks"))[:600])
        m0 = (rep.get("markets") or [{}])[0] or {}
        print("markets[0].lp:", json.dumps(m0.get("lp"))[:600])
        print("risks:", [(r.get("name"), r.get("level")) for r in rep.get("risks") or [] if isinstance(r, dict)])
        print("\nWhat the bot extracted:", parse_rugcheck(rep))
    pairs = fetch_dex_pairs([mint])
    p = (pairs or {}).get(mint)
    print("\nDexScreener snapshot:", snap_from_dex(p, time.time()) if p else "unavailable")


def make_sample(kind: str, bot: Bot, now: float) -> Candidate:
    """Fake tokens that run through the REAL scoring/classification code, for previewing embeds."""
    P = {  # name, symbol, price0, price growth over 11 min, liq, mcap, vol_m5, vol_h1, chg_m5, chg_h1, buys_m5, sells_m5, buys_h1, sells_h1, age_min, holders growth
        "scalp": ("Zap Token", "ZAP", .0009, .06, 38_000, 900_000, 9_000, 42_000, 3.0, 14.0, 60, 30, 520, 380, 40, (410, 480)),
        "momentum": ("Run Coin", "RUN", .0021, .10, 120_000, 2_400_000, 14_000, 160_000, 2.5, 85.0, 90, 70, 900, 700, 300, (1500, 1700)),
        "gem": ("Seed Finance", "SEED", .00031, .02, 52_000, 1_100_000, 3_800, 41_000, 0.8, 4.0, 40, 30, 380, 300, 500, (700, 980)),
        "narrative": ("Doggo Inu", "DOGGO", .00012, .01, 45_000, 800_000, 2_600, 30_000, 0.4, 3.0, 35, 30, 320, 290, 90, (500, 560)),
    }[kind]
    name, sym, p0, grow, liq, mcap, v5, v1, m5, h1, b5, s5, b1, s1, age, (h_a, h_b) = P
    c = Candidate(mint=f"Sample{sym}1111111111111111111111111111111111", first_seen=now - 900, symbol=sym, name=name,
                  url="https://dexscreener.com/solana", created_ms=(now - age * 60) * 1000, authority=True, sell_ok=True,
                  info={"websites": [{"url": "https://example.com", "label": "Website"}], "socials": [{"url": "https://x.com/example", "type": "twitter"}]},
                  rc={"ok": True, "holders": h_b, "top10_pct": 19.0, "top1_pct": 4.5, "insider_pct": 4.0, "insider_networks": 1,
                      "insider_wallets": 3, "lp_locked_pct": 100.0, "rugged": False, "danger_risks": 0, "risk_names": [],
                      "deployer": "9xQeWvG816bUx9EPjHmaT23yvVM2ZWbrrpZb9PusVFin", "deployer_pct": 0.8},
                  holders_hist=[(now - 1500, h_a), (now, h_b)], buyers_h1=210, sellers_h1=150, dex="raydium")
    for i in range(12):
        k = i / 11
        c.snaps.append(Snap(t=now - 660 + i * 60, price=p0 * (1 + grow * k), liq=liq * (0.9 + 0.1 * k), mcap=mcap, vol_m5=v5, vol_h1=v1,
                            vol_h24=v1 * 8, buys_m5=b5, sells_m5=s5, buys_h1=b1, sells_h1=s1, chg_m5=m5, chg_h1=h1, chg_h24=h1 * 2, dex="raydium"))
    c.first_snap = c.snaps[0]
    if kind == "narrative":   # sibling dog tokens that are moving, so the meta is measured, not assumed
        sibs = []
        for j, (sy, ch, vol) in enumerate((("BONKY", 48, 90_000), ("WOOF", 31, 60_000), ("PUPPER", 22, 44_000))):
            x = Candidate(mint=f"Sib{j}", first_seen=now - 3000, symbol=sy, name=sy.title())
            x.snaps.append(Snap(t=now - 60, price=1, liq=40_000, mcap=700_000, vol_h1=vol, chg_h1=ch))
            sibs.append(x)
        bot.narr.vol_hist[("theme", "Dogs")].append((now - 700, 110_000))
        bot.narr.refresh(sibs + [c], now)
    return c


def send_test():
    """Four fake sample embeds (one per play type) so you can check the webhook and the layout on your phone."""
    cfg, now = Config(), time.time()
    for kind in ("scalp", "momentum", "narrative", "gem"):
        bot = Bot(cfg)
        c = make_sample(kind, bot, now)
        f = features(c, now, cfg)
        f.update(bot.lane_info(c, f, now))
        c.f, c.narr = f, bot.narr.analyze(c, now)
        c.subs = category_scores(f, c.narr)
        c.risk, c.risk_notes, c.vetoes = risk_assessment(c, f, cfg)
        c.score, c.coverage = compute_score(c.subs, c.risk, bot.weights())
        c.play = classify_play(c, now, cfg, cfg.signal_threshold, bot.learner)
        if c.play["category"] not in CATS:
            print(f"[test] sample '{kind}' did not qualify (category {c.play['category']}, near {c.play['near']}); skipped")
            continue
        notify(build_play_embed(c, cfg, cfg.signal_threshold, bot.learner, DEBUG_EMBED), content=f"`{c.mint}`" if CA_TEXT else "")
        print(f"[test] sent sample: {c.play['category']} (Mind Score {c.play['score']:.0f})")
        time.sleep(1.2)
    print("Done. These are FAKE tokens; check your Discord channel.")


if __name__ == "__main__":
    args = sys.argv
    if "--test" in args:
        send_test()
    elif "--report" in args:
        report(Config())
    elif "--probe" in args:
        i = args.index("--probe")
        probe(args[i + 1]) if len(args) > i + 1 else print("usage: --probe MINT")
    else:
        mins = None
        if "--max-minutes" in args:
            mins = float(args[args.index("--max-minutes") + 1])
        elif "--once" in args:   # compatibility with the old 5-minute cron workflow
            mins = 4.5
        Bot(Config()).run(max_minutes=mins)
