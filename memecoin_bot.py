"""
Solana memecoin radar for Discord: scans, filters out rugs, posts compact trader cards, tracks every call.
No trading, no wallet, no keys.

Channels (each is a Discord webhook; a feed is off if its variable is not set):
  DISCORD_WEBHOOK_URL            #signals    best picks: QUICK SCALP / MOMENTUM / NARRATIVE META / EARLY GEM
  DISCORD_RADAR_WEBHOOK_URL      #radar      Axiom-Pulse-style columns: NEW PAIR / FINAL STRETCH / MIGRATED
  DISCORD_HEARTBEAT_WEBHOOK_URL  #heartbeat  status every 30 min (coins scanned, safety blocks, calls tracked)
  DISCORD_TRACKER_WEBHOOK_URL    #tracker    (optional) 2x/3x/5x/10x... pings, death notes, hourly call board
Optional: BUBBLEMAPS_API_KEY (adds Bubblemaps cluster/bundle checks), SOLANA_RPC_URL, CA_POSITION=top,
          PUMPPORTAL_API_KEY (not needed for the free launch/migration streams)

Where coins come from: a LIVE pump.fun feed (PumpPortal websocket: every new launch and every migration the moment
it happens), plus GeckoTerminal new/trending pools, pump.fun bonding-curve pools (final stretch), PumpSwap pools
(migrated) and DexScreener profiles/boosts. New launches are checked at 3, 8, 15 and 30 min old; the ones that get
real liquidity and volume join the watch list (up to 1,500 coins, each re-checked at least once a minute).

Nothing is posted unless it passes the SAFETY GATE (unknown = fail): mint/freeze revoked, clean RugCheck,
LP >= 90% locked/burned (DEX pools), top 10 <= 30%, largest holder <= 10%, insiders <= 10%, <= 1 wallet cluster,
dev <= 5%, >= 150 holders, liquidity >= $20K and >= 3% of MC, >= 20 min old, watched >= 5 min with no liquidity
drop, real sellers, no wash trading, a confirmed Jupiter sell route, deployer not blacklisted, and (with a key)
Bubblemaps clusters/bundles under the limits. A deployer whose called coin rugs is blacklisted.
No filter can guarantee a coin is legit.

Run:   pip install requests websocket-client
       python memecoin_bot.py                  runs forever
       python memecoin_bot.py --max-minutes N  run N minutes then save and exit (GitHub Actions runs ~5h40m at a time)
       python memecoin_bot.py --test           send sample posts (fake coins) to each configured channel
       python memecoin_bot.py --probe MINT     show what the bot reads for one token
"""
import csv
import json
import os
import queue
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

VERSION = "RADAR-4.2"
RPC_URL = os.getenv("SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com"
DEXSCREENER = "https://api.dexscreener.com"
GECKO = "https://api.geckoterminal.com/api/v2"
GECKO_HEADERS = {"Accept": "application/json;version=20230302"}
RUGCHECK = "https://api.rugcheck.xyz/v1"
BUBBLEMAPS = "https://api.bubblemaps.io/v0"
BUBBLEMAPS_KEY = os.getenv("BUBBLEMAPS_API_KEY")
JUPITER_QUOTE = os.getenv("JUPITER_QUOTE_URL") or "https://lite-api.jup.ag/swap/v1/quote"
JUPITER_KEY = os.getenv("JUPITER_API_KEY")
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
RADAR_WEBHOOK = os.getenv("DISCORD_RADAR_WEBHOOK_URL")
HEARTBEAT_WEBHOOK = os.getenv("DISCORD_HEARTBEAT_WEBHOOK_URL")
TRACKER_WEBHOOK = os.getenv("DISCORD_TRACKER_WEBHOOK_URL")
CA_POSITION = (os.getenv("CA_POSITION") or "bottom").lower()
PUMPPORTAL_URL = os.getenv("PUMPPORTAL_URL") or "wss://pumpportal.fun/api/data"
PUMPPORTAL_KEY = os.getenv("PUMPPORTAL_API_KEY")

SOL_MINT = "So11111111111111111111111111111111111111112"
STABLES = {SOL_MINT, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
CATS = ("QUICK SCALP", "MOMENTUM", "NARRATIVE META", "EARLY GEM")
STYLE = {"QUICK SCALP": ("⚡", 0xF39C12), "MOMENTUM": ("🚀", 0x2ECC71), "NARRATIVE META": ("🧠", 0x9B59B6),
         "EARLY GEM": ("💎", 0x1ABC9C), "NEW PAIR": ("🆕", 0x3498DB), "FINAL STRETCH": ("🏁", 0xF1C40F),
         "MIGRATED": ("🎓", 0x2ECC71)}
TIER_RANK = {"strong": 0, "new": 1, "watch": 2, "weak": 3}


@dataclass
class Config:
    # candidate pool
    gate_liquidity_usd: float = 8_000
    gate_volume_h1_usd: float = 3_000
    min_pair_age_min: float = 8
    max_pair_age_min: float = 48 * 60
    dead_liquidity_usd: float = 3_000
    max_candidates: int = 1500
    # live launch feed (PumpPortal): every new pump.fun coin + migration
    launch_feed: bool = True
    launch_checks_s: tuple = (180, 480, 900, 1800)   # check a new launch at 3, 8, 15, 30 min old; drop if still dead
    max_pending_launches: int = 5000
    # refresh cadence (seconds) - every coin at least once a minute
    interval_strong: float = 10
    interval_new: float = 15
    interval_watch: float = 30
    interval_weak: float = 60
    discovery_new_s: float = 20
    discovery_trending_s: float = 90
    discovery_final_s: float = 45
    discovery_migrated_s: float = 60
    discovery_profiles_s: float = 30
    discovery_boosts_s: float = 60
    buyers_refresh_s: float = 90
    tick_seconds: float = 1.0
    workers: int = 4
    # lanes
    graduation_mcap_usd: float = 69_000   # assumed pump.fun migration size until >= 3 migrations are observed
    final_stretch_pct: float = 0.80
    migrated_window_min: float = 180
    new_pair_max_age_min: float = 60
    # enrichment
    rugcheck_ttl_strong: float = 90
    rugcheck_ttl_watch: float = 240
    authority_recheck_s: float = 600
    sell_check_usd: float = 20
    # picks (#signals)
    signal_threshold: float = 68
    thr_offset: dict = field(default_factory=lambda: {"QUICK SCALP": 0, "MOMENTUM": 0, "NARRATIVE META": -4, "EARLY GEM": -6})
    min_observations: int = 3
    min_observe_seconds: float = 120
    max_risk: float = 45
    min_data_coverage: float = 0.6
    # radar (#radar): posts per hour per column, and no chasing pumps / dumps
    radar_per_hour: dict = field(default_factory=lambda: {"NEW PAIR": 12, "FINAL STRETCH": 8, "MIGRATED": 8})
    radar_max_h1_gain: float = 200
    radar_max_drawdown: float = 0.25
    # vetoes (dropped from monitoring)
    veto_top10_pct: float = 50
    veto_insider_pct: float = 25
    veto_liq_drop_10m: float = 0.50
    veto_liq_mcap: float = 0.008
    # SAFETY GATE (required for every post; unknown = fail)
    safe_min_age_min: float = 20
    safe_min_observe_s: float = 300
    safe_min_liquidity_usd: float = 20_000
    safe_min_liq_mcap: float = 0.03
    safe_max_top10_pct: float = 30
    safe_max_top1_pct: float = 10
    safe_max_insider_pct: float = 10
    safe_max_insider_networks: int = 1
    safe_min_lp_locked_pct: float = 90
    safe_max_deployer_pct: float = 5
    safe_min_holders: int = 150
    safe_min_sell_share: float = 0.15
    safe_min_trades_h1: int = 30
    safe_max_buys_per_buyer: float = 5
    safe_max_turnover: float = 40
    safe_max_liq_drop: float = 0.15
    safe_max_cluster_share: float = 0.20   # Bubblemaps (only with BUBBLEMAPS_API_KEY)
    safe_max_bundle_share: float = 0.15
    # tracking
    track_hours: float = 24
    track_milestones: tuple = (2, 3, 5, 10, 20, 50, 100)
    board_minutes: float = 60
    heartbeat_minutes: float = 30
    calls_keep_days: float = 7
    dead_no_price_min: float = 10
    # files
    log_file: str = "radar_log.csv"
    signals_file: str = "signals.jsonl"
    state_file: str = "state.json"


# ======================================================================= HTTP (rate limited per host)
class Limiter:
    def __init__(self, min_interval: float):
        self.min_interval, self.lock = min_interval, threading.Lock()
        self.next_ok = self.blocked_until = 0.0
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
        while self.calls and self.calls[0] < time.time() - 60:
            self.calls.popleft()
        return len(self.calls)


LIMITERS = {"dex": Limiter(0.35), "dexfeed": Limiter(1.2), "gecko": Limiter(4.0), "rugcheck": Limiter(1.0),
            "rpc": Limiter(0.4), "jupiter": Limiter(1.0), "bubblemaps": Limiter(2.0)}


def http_json(host, url, params=None, method="GET", body=None, headers=None, max_wait=0.5, timeout=10):
    """(json or None, status). status 0 = limiter said wait, -1 = network error."""
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
        return r.json(), r.status_code
    except ValueError:
        return None, r.status_code


# ======================================================================= helpers
def _num(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def usd(n):
    n = _num(n)
    if n is None:
        return "n/a"
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= div:
            return f"${n / div:.1f}{suf}" if n / div < 100 else f"${n / div:.0f}{suf}"
    return f"${n:,.0f}"


def pct(n):
    n = _num(n)
    return "n/a" if n is None else (f"{n:+.0f}%" if abs(n) >= 10 else f"{n:+.1f}%")


def short_addr(a):
    return f"{a[:4]}…{a[-4:]}" if a and len(a) > 10 else (a or "n/a")


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


def ramp(x, lo, hi):
    """x onto 0..100 (lo -> 0, hi -> 100; lo > hi inverts). None stays None (unavailable)."""
    if x is None:
        return None
    if lo == hi:
        return 100.0 if x >= hi else 0.0
    return 100.0 * clamp((x - lo) / (hi - lo), 0.0, 1.0)


def wavg(parts):
    parts = [(v, w) for v, w in parts if v is not None]
    return sum(v * w for v, w in parts) / sum(w for _, w in parts) if parts else None


def ago(seconds: float) -> str:
    m = max(0, int(seconds // 60))
    return f"{m}m" if m < 60 else (f"{m // 60}h {m % 60}m" if m < 1440 else f"{m // 1440}d")


def parse_iso_ms(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp() * 1000 if s else None
    except ValueError:
        return None


# ======================================================================= Discord
class CA(str):
    """A contract address: posted as its own message in inline code, so Discord mobile copies it on tap."""


def _post(url: str, payload: dict) -> bool:
    try:
        for _ in range(3):
            r = requests.post(url, json=payload, timeout=10)
            if r.status_code == 429:
                try:
                    wait = float(r.json().get("retry_after", 1))
                except Exception:
                    wait = 1.0
                time.sleep(min(wait, 10))
                continue
            if r.status_code >= 400:
                print(f"[warn] discord returned {r.status_code}: {r.text[:200]}")
                return False
            return True
    except Exception as e:
        print(f"[warn] discord post failed: {e}")
    return False


def notify(embed: dict, ca: Optional[str] = None, hook=None) -> bool:
    url = hook or DISCORD_WEBHOOK
    if not url:
        return False
    if not ca:
        return _post(url, {"embeds": [embed]})
    ca_msg = {"content": f"`{ca}`", "allowed_mentions": {"parse": []}}
    if CA_POSITION == "top":
        return _post(url, ca_msg) and _post(url, {"embeds": [embed]})
    ok = _post(url, {"embeds": [embed]})
    if ok:
        _post(url, ca_msg)
    return ok


# ======================================================================= data models
@dataclass
class Snap:
    t: float
    price: float
    liq: Optional[float] = None
    mcap: Optional[float] = None
    vol_m5: Optional[float] = None
    vol_h1: Optional[float] = None
    buys_m5: Optional[int] = None
    sells_m5: Optional[int] = None
    buys_h1: Optional[int] = None
    sells_h1: Optional[int] = None
    chg_m5: Optional[float] = None
    chg_h1: Optional[float] = None
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
    first_dex: str = ""
    dex: str = ""
    migrated_t: Optional[float] = None
    trend_rank: Optional[int] = None
    tier: str = "new"
    next_check: float = 0.0
    hot_until: float = 0.0
    no_pair_since: Optional[float] = None
    dirty: bool = True
    dead_reason: str = ""
    # checks
    authority: Optional[bool] = None
    authority_t: float = 0.0
    decimals: Optional[int] = None
    rc: dict = field(default_factory=dict)
    rc_t: float = 0.0
    rc_ok_t: float = 0.0
    sell_ok: Optional[bool] = None
    sell_fail: int = 0
    sell_t: float = 0.0
    bm: Optional[dict] = None
    bm_t: float = 0.0
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
    safety_fails: list = field(default_factory=lambda: ["not checked yet"])
    # post state
    alerted: bool = False
    tracked: bool = False
    new_posted: bool = False
    final_posted: bool = False
    migr_posted: bool = False


@dataclass
class Call:
    """A posted coin, followed for track_hours from the price at the moment it was posted."""
    mint: str
    symbol: str
    url: str
    t0: float
    price0: float
    mcap0: Optional[float] = None
    liq0: Optional[float] = None
    labels: list = field(default_factory=list)
    channel: str = "radar"
    peak: float = 0.0
    peak_t: float = 0.0
    peak_mcap: Optional[float] = None
    last_price: float = 0.0
    last_t: float = 0.0
    last_mcap: Optional[float] = None
    last_liq: Optional[float] = None
    hits: int = 0
    status: str = "live"
    end_reason: str = ""
    end_t: float = 0.0
    deployer: str = ""
    pair_addr: str = ""

    def mult(self) -> float:
        return self.last_price / self.price0 if self.price0 else 0.0

    def peak_mult(self) -> float:
        return self.peak / self.price0 if self.price0 else 0.0


# ======================================================================= data sources
def snap_from_dex(pair: dict, t: float) -> Optional[Snap]:
    price = _num(pair.get("priceUsd"))
    if not price or price <= 0:
        return None
    vol, chg, tx = pair.get("volume") or {}, pair.get("priceChange") or {}, pair.get("txns") or {}
    m5, h1 = tx.get("m5") or {}, tx.get("h1") or {}
    return Snap(t=t, price=price, liq=_num((pair.get("liquidity") or {}).get("usd")), mcap=_num(pair.get("marketCap") or pair.get("fdv")),
                vol_m5=_num(vol.get("m5")), vol_h1=_num(vol.get("h1")), buys_m5=m5.get("buys"), sells_m5=m5.get("sells"),
                buys_h1=h1.get("buys"), sells_h1=h1.get("sells"), chg_m5=_num(chg.get("m5")), chg_h1=_num(chg.get("h1")),
                dex=pair.get("dexId") or "")


def snap_from_gecko(a: dict, t: float, dex: str = "") -> Optional[Snap]:
    price = _num(a.get("base_token_price_usd"))
    if not price or price <= 0:
        return None
    vol, chg, tx = a.get("volume_usd") or {}, a.get("price_change_percentage") or {}, a.get("transactions") or {}
    m5, h1 = tx.get("m5") or {}, tx.get("h1") or {}
    return Snap(t=t, price=price, liq=_num(a.get("reserve_in_usd")), mcap=_num(a.get("market_cap_usd") or a.get("fdv_usd")),
                vol_m5=_num(vol.get("m5")), vol_h1=_num(vol.get("h1")), buys_m5=m5.get("buys"), sells_m5=m5.get("sells"),
                buys_h1=h1.get("buys"), sells_h1=h1.get("sells"), chg_m5=_num(chg.get("m5")), chg_h1=_num(chg.get("h1")), dex=dex)


def fetch_dex_feed(path: str):
    data, st = http_json("dexfeed", f"{DEXSCREENER}{path}")
    if st != 200 or not isinstance(data, list):
        return None
    return [{"mint": d["tokenAddress"], "description": d.get("description") or "", "links": d.get("links") or [], "icon": d.get("icon")}
            for d in data if isinstance(d, dict) and d.get("chainId") == "solana" and d.get("tokenAddress")]


def fetch_dex_pairs(mints: list):
    """Up to 30 tokens per call -> {mint: deepest pair where the token is the base}; None if unavailable."""
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
        if m in want:
            liq = _num((p.get("liquidity") or {}).get("usd")) or 0
            if m not in best or liq > (_num((best[m].get("liquidity") or {}).get("usd")) or 0):
                best[m] = p
    return best


def _parse_gecko_pools(data) -> list:
    included = {i.get("id"): (i.get("attributes") or {}) for i in (data.get("included") or []) if isinstance(i, dict)}
    out, t = [], time.time()
    for pool in data.get("data") or []:
        a, rel = pool.get("attributes") or {}, pool.get("relationships") or {}
        base = (((rel.get("base_token") or {}).get("data") or {}).get("id") or "").split("_", 1)[-1]
        quote = (((rel.get("quote_token") or {}).get("data") or {}).get("id") or "").split("_", 1)[-1]
        dex = ((rel.get("dex") or {}).get("data") or {}).get("id") or ""
        if not base:
            continue
        mint, usable = (base, True) if base not in STABLES else (quote, False)
        if not mint or mint in STABLES:
            continue
        tok = included.get(f"solana_{mint}") or {}
        symbol = tok.get("symbol") or (a.get("name") or "?").split(" / ")[0].strip()
        h1 = (a.get("transactions") or {}).get("h1") or {}
        item = {"mint": mint, "symbol": symbol, "name": tok.get("name") or symbol, "pair": a.get("address"),
                "created_ms": parse_iso_ms(a.get("pool_created_at")), "icon": tok.get("image_url"),
                "buyers_h1": h1.get("buyers"), "sellers_h1": h1.get("sellers")}
        if usable:
            item["snap"] = snap_from_gecko(a, t, dex)
        out.append(item)
    return out


def fetch_gecko(path: str, params=None):
    data, st = http_json("gecko", f"{GECKO}{path}", params=params, headers=GECKO_HEADERS)
    return data if (st == 200 and isinstance(data, dict)) else None


def fetch_gecko_pools(kind: str):
    data = fetch_gecko(f"/networks/solana/{kind}", {"include": "base_token"})
    return _parse_gecko_pools(data) if data else None


def fetch_gecko_dex_pools(dex_id: str, page: int = 1):
    data = fetch_gecko(f"/networks/solana/dexes/{dex_id}/pools", {"include": "base_token", "page": page})
    return _parse_gecko_pools(data) if data else None


def fetch_pump_dexes():
    """(bonding-curve dex ids, PumpSwap dex ids) found in GeckoTerminal's Solana DEX list."""
    rows = []
    for page in (1, 2, 3):
        data, st = http_json("gecko", f"{GECKO}/networks/solana/dexes", params={"page": page}, headers=GECKO_HEADERS, max_wait=30)
        if st != 200 or not isinstance(data, dict):
            break
        page_rows = [r for r in (data.get("data") or []) if isinstance(r, dict)]
        rows += [(r.get("id") or "", (r.get("attributes") or {}).get("name") or "") for r in page_rows]
        if len(page_rows) < 20:
            break
    bonding, swap = [], []
    for i, name in rows:
        key = f"{i} {name}".lower()
        if "pump" in key:
            (swap if "swap" in key else bonding).append(i)
    return bonding, swap


def fetch_gecko_buyers(pair_addrs: list):
    """Unique buyers/sellers (1h) for up to 30 pools -> {pool: (buyers, sellers)}; None if unavailable."""
    if not pair_addrs:
        return {}
    data = fetch_gecko(f"/networks/solana/pools/multi/{','.join(pair_addrs)}")
    if not data:
        return None
    out = {}
    for pool in data.get("data") or []:
        a = pool.get("attributes") or {}
        h1 = (a.get("transactions") or {}).get("h1") or {}
        if a.get("address") and h1.get("buyers") is not None:
            out[a["address"]] = (h1.get("buyers"), h1.get("sellers"))
    return out


def mint_info(mint):
    """(mint+freeze authority revoked, decimals); (None, None) if the RPC call failed."""
    data, _ = http_json("rpc", RPC_URL, method="POST", max_wait=30,
                        body={"jsonrpc": "2.0", "id": 1, "method": "getAccountInfo", "params": [mint, {"encoding": "jsonParsed"}]})
    try:
        info = data["result"]["value"]["data"]["parsed"]["info"]
        return (info.get("mintAuthority") is None and info.get("freezeAuthority") is None), info.get("decimals")
    except Exception:
        return None, None


def parse_rugcheck(rep) -> dict:
    rc = {"ok": False, "holders": None, "top10_pct": None, "top1_pct": None, "insider_pct": None, "insider_networks": None,
          "lp_locked_pct": None, "rugged": None, "danger_risks": None, "risk_names": [], "warn_names": [],
          "deployer": None, "deployer_pct": None}
    if not isinstance(rep, dict):
        return rc
    rc["ok"] = True
    th = _num(rep.get("totalHolders"))
    rc["holders"] = int(th) if th is not None else None
    known = rep.get("knownAccounts") or {}

    def is_pool(h):  # pools and bonding curves are not holders
        for k in (h.get("address"), h.get("owner")):
            ka = known.get(k) if k else None
            if isinstance(ka, dict):
                typ, name = str(ka.get("type", "")).upper(), str(ka.get("name", "")).lower()
                if typ in ("AMM", "LP", "POOL", "BONDING", "CURVE") or any(w in name for w in ("bonding curve", "amm", "pool", "liquidity")):
                    return True
        return False

    holders = [h for h in (rep.get("topHolders") or []) if isinstance(h, dict) and not is_pool(h)]
    pcts = sorted((p for p in (_num(h.get("pct")) for h in holders) if p is not None), reverse=True)
    if pcts:
        rc["top10_pct"], rc["top1_pct"] = sum(pcts[:10]), pcts[0]
    if any("insider" in h for h in holders):
        rc["insider_pct"] = sum((_num(h.get("pct")) or 0.0) for h in holders if h.get("insider"))
    if isinstance(rep.get("insiderNetworks"), list):
        rc["insider_networks"] = len(rep["insiderNetworks"])
    lp = [v for v in (_num(((m or {}).get("lp") or {}).get("lpLockedPct")) for m in (rep.get("markets") or [])) if v is not None]
    if lp:
        rc["lp_locked_pct"] = max(lp)
    if rep.get("rugged") is not None:
        rc["rugged"] = bool(rep["rugged"])
    risks = [r for r in (rep.get("risks") or []) if isinstance(r, dict)]
    rc["risk_names"] = [str(r.get("name")) for r in risks if str(r.get("level", "")).lower() == "danger"]
    rc["danger_risks"] = len(rc["risk_names"])
    rc["warn_names"] = [str(r.get("name")) for r in risks if str(r.get("level", "")).lower() == "warn"]
    creator = rep.get("creator")
    if isinstance(creator, str) and creator:
        rc["deployer"] = creator
        rc["deployer_pct"] = next((_num(h.get("pct")) for h in holders if creator in (h.get("address"), h.get("owner"))), None)
    return rc


def fetch_rugcheck(mint) -> dict:
    data, st = http_json("rugcheck", f"{RUGCHECK}/tokens/{mint}/report", max_wait=30)
    return parse_rugcheck(data if st == 200 else None)


def fetch_bubblemaps(mint) -> Optional[dict]:
    """Bubblemaps clusters/bundles (needs BUBBLEMAPS_API_KEY; each call uses API credits)."""
    if not BUBBLEMAPS_KEY:
        return None
    data, st = http_json("bubblemaps", f"{BUBBLEMAPS}/tokens/map/solana/{mint}", headers={"X-ApiKey": BUBBLEMAPS_KEY},
                         params={"return_clusters": "true"}, max_wait=30, timeout=20)
    if st != 200 or not isinstance(data, dict):
        return None
    share = lambda v: None if _num(v) is None else (_num(v) / 100 if _num(v) > 1 else _num(v))
    stats = (data.get("metrics") or {}).get("supply_stats") or {}
    clusters = [x for x in (share(c.get("share")) for c in (data.get("clusters") or []) if isinstance(c, dict)) if x is not None]
    return {"top_cluster": max(clusters) if clusters else 0.0, "bundles": share(stats.get("bundles")),
            "score": _num(((data.get("metrics") or {}).get("scores") or {}).get("bubblemaps_score"))}


def sell_route_check(mint, price_usd, decimals, cfg) -> Optional[bool]:
    """Honeypot check: can Jupiter route a small sell to SOL? None = could not tell."""
    if not price_usd or decimals is None:
        return None
    raw = int((cfg.sell_check_usd / price_usd) * (10 ** int(decimals)))
    if raw <= 0:
        return None
    data, st = http_json("jupiter", JUPITER_QUOTE, headers={"x-api-key": JUPITER_KEY} if JUPITER_KEY else None, max_wait=30,
                         params={"inputMint": mint, "outputMint": SOL_MINT, "amount": raw, "slippageBps": 1500})
    if st == 200 and isinstance(data, dict):
        return True if (_num(data.get("outAmount")) or 0) > 0 else None
    if st in (400, 404) and data is not None and any(w in json.dumps(data).lower() for w in ("route", "not_tradable", "not tradable")):
        return False
    return None


def do_enrich(mint, price, decimals, need_auth, need_rc, need_sell, need_bm, cfg) -> dict:
    """Worker thread: returns results only (the main thread applies them)."""
    out = {}
    if need_auth:
        out["authority"], out["decimals"] = mint_info(mint)
        decimals = out["decimals"] if out["decimals"] is not None else decimals
    if need_rc:
        out["rc"] = fetch_rugcheck(mint)
    if need_sell:
        out["sell"] = sell_route_check(mint, price, decimals, cfg)
    if need_bm:
        out["bm"] = fetch_bubblemaps(mint)
    return out


# ======================================================================= features, scores, risk, safety
WEIGHTS = {"momentum": .14, "volume": .14, "buy_sell": .10, "holders": .08, "liquidity": .12,
           "distribution": .10, "insiders": .08, "organic": .08, "narrative": .06, "continuation": .10}
CHEAP_KEYS = ("momentum", "volume", "buy_sell", "liquidity", "continuation")
MCAP_TIERS = (30e3, 50e3, 100e3, 250e3, 500e3, 1e6, 5e6)
SEVERE_RE = re.compile(r"rug|honeypot|freeze|mint auth|blacklist|bundl|copycat|correlat|can.?t sell|cannot sell|creator.*(histor|rug)", re.I)
BLOCK_WARN_RE = re.compile(r"lp unlocked|unlocked lp|single holder|high ownership|top 10|low liquidity|copycat|creator|"
                           r"freeze|mint|bundl|insider|honeypot|transfer fee|blacklist", re.I)


def dex_stage(dex: str):
    d = (dex or "").lower()
    if not d:
        return None
    return "graduated" if ("pumpswap" in d or "pump" not in d) else "bonding"


def snap_ago(c: Candidate, now: float, seconds: float) -> Optional[Snap]:
    best = None
    for s in c.snaps:
        if s.t <= now - seconds:
            best = s
        else:
            break
    return best


def features(c: Candidate, now: float) -> dict:
    s, s0, rc = c.snaps[-1], (c.first_snap or c.snaps[0]), (c.rc or {})
    tb, ts, mb, ms = s.buys_h1 or 0, s.sells_h1 or 0, s.buys_m5 or 0, s.sells_m5 or 0
    f = {"price": s.price, "liq": s.liq, "mcap": s.mcap, "vol_h1": s.vol_h1, "vol_m5": s.vol_m5, "chg_m5": s.chg_m5,
         "chg_h1": s.chg_h1, "buys_h1": tb, "sells_h1": ts, "buys_m5": mb, "sells_m5": ms, "tx_h1": tb + ts,
         "br_h1": tb / (tb + ts) if tb + ts >= 20 else None, "br_m5": mb / (mb + ms) if mb + ms >= 6 else None,
         "obs": len(c.snaps), "span_s": s.t - s0.t}
    f["age_min"] = (now * 1000 - c.created_ms) / 60000 if c.created_ms else (now - c.first_seen) / 60
    f["vol_accel"] = (s.vol_m5 * 12) / s.vol_h1 if (s.vol_m5 is not None and s.vol_h1 and s.vol_h1 >= 1000) else None
    f["turnover"] = s.vol_h1 / s.liq if (s.vol_h1 is not None and s.liq) else None
    f["liq_mcap"] = s.liq / s.mcap if (s.liq and s.mcap) else None
    recent = list(c.snaps)[-7:]
    f["rising_frac"] = (sum(1 for a, b in zip(recent, recent[1:]) if b.price >= a.price * 0.999) / (len(recent) - 1)) if len(recent) >= 4 else None
    peak = max((x.price for x in c.snaps if x.t >= now - 1800), default=s.price)
    f["dd_from_peak"] = s.price / peak - 1 if peak > 0 else None
    l10 = snap_ago(c, now, 600)
    f["liq_chg_10m"] = s.liq / l10.liq - 1 if (l10 and l10.liq and s.liq is not None) else None
    f["liq_chg_first"] = s.liq / s0.liq - 1 if (s0.liq and s.liq is not None) else None
    f["mcap_chg_first"] = s.mcap / s0.mcap - 1 if (s0.mcap and s.mcap is not None) else None
    tier = lambda m: sum(1 for t in MCAP_TIERS if m >= t)
    f["tier_up"] = (tier(s.mcap) - tier(s0.mcap)) if (s.mcap and s0.mcap) else None
    f["dex_stage"] = dex_stage(s.dex)
    f["graduated_recent"] = bool(c.migrated_t is not None or (dex_stage(c.first_dex) == "bonding" and f["dex_stage"] == "graduated"))
    hh = c.holders_hist
    f["holders"] = hh[-1][1] if hh else rc.get("holders")
    growth = None
    if len(hh) >= 2:
        t_now, h_now = hh[-1]
        old = [x for x in hh if now - 3600 <= x[0] <= t_now - 180]
        if old and old[0][1] > 0:
            growth = ((h_now / old[0][1]) - 1) / ((t_now - old[0][0]) / 3600) * 100
    f["holders_growth_hr"] = growth
    f.update(top10=rc.get("top10_pct"), top1=rc.get("top1_pct"), insider=rc.get("insider_pct"),
             nets=rc.get("insider_networks"), lp_locked=rc.get("lp_locked_pct"))
    f["buyers_h1"], f["sellers_h1"] = c.buyers_h1, c.sellers_h1
    f["buys_per_buyer"] = tb / c.buyers_h1 if (c.buyers_h1 and c.buyers_h1 >= 5) else None
    link_types = {str(x.get("type", "")).lower() for x in c.links if isinstance(x, dict)}
    has_site = bool(c.info.get("websites")) or "website" in link_types
    has_social = bool(c.info.get("socials")) or bool(link_types & {"twitter", "telegram", "discord"})
    f["social_score"] = 100.0 if (has_site and has_social) else (50.0 if (has_site or has_social) else 0.0)
    return f


def category_scores(f: dict, narr: dict) -> dict:
    s = {}
    m = wavg([(ramp(f["chg_m5"], -3, 6), .35), (ramp(f["chg_h1"], -10, 40), .25), (ramp(f["rising_frac"], .3, .8), .25),
              (ramp(f["dd_from_peak"], -.35, 0), .15)])
    if m is not None and f["chg_h1"] is not None:
        m *= .35 if f["chg_h1"] > 800 else (.6 if f["chg_h1"] > 300 else 1.0)
    s["momentum"] = m
    s["volume"] = wavg([(ramp(f["vol_h1"], 5_000, 80_000), .4), (ramp(f["vol_accel"], .7, 1.8), .3), (ramp(f["turnover"], 60, 15), .3)])
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
    s["narrative"] = (narr or {}).get("strength") if (narr or {}).get("strength") is not None else 30.0
    s["continuation"] = wavg([(ramp(f["span_s"] / 60, 2, 12), .2), (ramp(f["rising_frac"], .4, .8), .2), (ramp(f["mcap_chg_first"], 0, .5), .2),
                              (100.0 if (f["graduated_recent"] or (f["tier_up"] or 0) >= 1) else 40.0, .15),
                              (ramp(f["liq_chg_first"], -.1, .3), .15), (ramp(f["holders_growth_hr"], 0, 20), .1)])
    return s


def risk_assessment(c: Candidate, f: dict, cfg: Config):
    """(risk 0-100, notes, vetoes). Missing data adds no points; the safety gate handles unknowns."""
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
    severe = [n for n in rc.get("risk_names") or [] if SEVERE_RE.search(n)]
    if severe:
        vetoes.append(f"RugCheck danger: {severe[0]}")
    elif (rc.get("danger_risks") or 0) >= 2:
        vetoes.append(f"RugCheck: {rc['danger_risks']} danger flags")
    elif rc.get("danger_risks") == 1:
        add(25, f"RugCheck danger: {rc['risk_names'][0]}")
    t10, ins = f["top10"], f["insider"]
    if t10 is not None:
        if t10 >= cfg.veto_top10_pct:
            vetoes.append(f"top 10 hold {t10:.0f}%")
        elif t10 > 30:
            add(ramp(t10, 30, cfg.veto_top10_pct) * .25, f"top 10 hold {t10:.0f}%")
        if c.top10_first is not None and t10 - c.top10_first >= 10:
            add(10, "holder concentration rising")
    if ins is not None:
        if ins >= cfg.veto_insider_pct:
            vetoes.append(f"insiders hold {ins:.0f}%")
        elif ins > 10:
            add(ramp(ins, 10, cfg.veto_insider_pct) * .25, f"insiders {ins:.0f}%")
    if f["nets"] and f["nets"] >= 2:
        add(10, f"{f['nets']} connected-wallet clusters")
    if f["lp_locked"] is not None and f["lp_locked"] < 50:
        add(20 if f["lp_locked"] < 10 else 10, "LP not locked")
    r = f["liq_mcap"]
    if r is not None:
        if r < cfg.veto_liq_mcap:
            vetoes.append("liquidity under 0.8% of market cap")
        elif r < .03:
            add(ramp(r, .03, cfg.veto_liq_mcap) * .2, "thin liquidity")
    lc = f["liq_chg_10m"]
    if lc is not None and lc < 0:
        if -lc >= cfg.veto_liq_drop_10m:
            vetoes.append("liquidity collapsing")
        elif -lc >= .15:
            add(ramp(-lc, .15, .5) * .35, f"liquidity down {-lc:.0%} in 10m")
    dd = f["dd_from_peak"]
    if dd is not None and dd <= -.4:
        add(40 if dd <= -.6 else 25, "dumping from recent peak")
    if (f["buys_m5"] + f["sells_m5"]) >= 10 and f["sells_m5"] >= 2 * f["buys_m5"]:
        add(15, "sell pressure surge")
    if f["buys_h1"] >= 40 and f["sells_h1"] <= 2:
        vetoes.append("buys but almost no sells (honeypot-like)")
    if c.sell_ok is False and c.sell_fail >= 2:
        vetoes.append("no sell route found")
    if f["turnover"] is not None and f["turnover"] > 60:
        add(15, "wash-trading-like volume")
    if f["buys_per_buyer"] is not None and f["buys_per_buyer"] > 6:
        add(10, "same wallets buying repeatedly")
    return min(100.0, pts), notes, vetoes


def safety_gate(c: Candidate, now: float, cfg: Config, bad_deployers=()) -> list:
    """Every check must PASS with real data; unknown = fail. Returns the failures (empty = safe)."""
    f, rc, s, fails = c.f, (c.rc or {}), c.snaps[-1], []
    if c.authority is not True:
        fails.append("mint/freeze authority not confirmed revoked")
    if not rc.get("ok") or now - c.rc_ok_t > 600:
        fails.append("no fresh RugCheck report")
    else:
        if rc.get("rugged"):
            fails.append("RugCheck: rugged")
        if rc.get("danger_risks"):
            fails.append("RugCheck danger: " + rc["risk_names"][0])
        warn = [w for w in rc.get("warn_names") or [] if BLOCK_WARN_RE.search(w)]
        if warn:
            fails.append("RugCheck warning: " + warn[0])
        h, t10, t1, ins, nets = f.get("holders"), f.get("top10"), f.get("top1"), f.get("insider"), f.get("nets")
        if h is None or h < cfg.safe_min_holders:
            fails.append("holders unknown" if h is None else f"only {h} holders")
        if t10 is None:
            fails.append("top-10 holder data unavailable")
        elif t10 > cfg.safe_max_top10_pct:
            fails.append(f"top 10 hold {t10:.0f}%")
        if t1 is not None and t1 > cfg.safe_max_top1_pct:
            fails.append(f"largest holder {t1:.0f}%")
        if ins is None and nets is None:
            fails.append("insider data unavailable")
        if ins is not None and ins > cfg.safe_max_insider_pct:
            fails.append(f"insiders {ins:.0f}%")
        if nets is not None and nets > cfg.safe_max_insider_networks:
            fails.append(f"{nets} connected-wallet clusters")
        if rc.get("deployer") and rc["deployer"] in bad_deployers:
            fails.append("deployer's earlier coin rugged")
        if rc.get("deployer_pct") is not None and rc["deployer_pct"] > cfg.safe_max_deployer_pct:
            fails.append(f"dev holds {rc['deployer_pct']:.0f}%")
    if f.get("dex_stage") != "bonding":
        lp = f.get("lp_locked")
        if lp is None:
            fails.append("LP lock unknown")
        elif lp < cfg.safe_min_lp_locked_pct:
            fails.append(f"LP only {lp:.0f}% locked/burned")
        if (s.liq or 0) < cfg.safe_min_liquidity_usd:
            fails.append(f"liquidity {usd(s.liq)}")
        if f.get("liq_mcap") is None or f["liq_mcap"] < cfg.safe_min_liq_mcap:
            fails.append("thin liquidity vs market cap")
    if f.get("age_min", 0) < cfg.safe_min_age_min:
        fails.append(f"only {f.get('age_min', 0):.0f} min old")
    if f.get("span_s", 0) < cfg.safe_min_observe_s:
        fails.append("still being watched")
    if any(x is not None and x <= -cfg.safe_max_liq_drop for x in (f.get("liq_chg_first"), f.get("liq_chg_10m"))):
        fails.append("liquidity dropped while watched")
    tot = f.get("tx_h1") or 0
    if tot < cfg.safe_min_trades_h1:
        fails.append("too few trades")
    elif (f.get("sells_h1") or 0) / tot < cfg.safe_min_sell_share:
        fails.append("almost nobody selling (honeypot sign)")
    if f.get("buys_per_buyer") is not None and f["buys_per_buyer"] > cfg.safe_max_buys_per_buyer:
        fails.append("same wallets buying repeatedly")
    if f.get("turnover") is not None and f["turnover"] > cfg.safe_max_turnover:
        fails.append("wash-trading-like volume")
    if c.sell_ok is not True:
        fails.append("sell route not confirmed (honeypot check)")
    if BUBBLEMAPS_KEY:
        if not c.bm:
            fails.append("Bubblemaps check pending")
        else:
            if (c.bm.get("top_cluster") or 0) > cfg.safe_max_cluster_share:
                fails.append(f"Bubblemaps cluster holds {c.bm['top_cluster']:.0%}")
            if c.bm.get("bundles") is not None and c.bm["bundles"] > cfg.safe_max_bundle_share:
                fails.append(f"bundled supply {c.bm['bundles']:.0%}")
    return fails


def compute_score(subs: dict, risk: float):
    """Score 0-100: weighted categories, cut by risk and by missing data. Not a probability."""
    avail = {k: v for k, v in subs.items() if k in WEIGHTS and v is not None}
    if not avail:
        return None, 0.0
    cov = sum(WEIGHTS[k] for k in avail) / sum(WEIGHTS.values())
    base = sum(WEIGHTS[k] * v for k, v in avail.items()) / sum(WEIGHTS[k] for k in avail)
    return base * max(0.0, 1.0 - 1.15 * (risk / 100.0) ** 1.3) * (0.6 + 0.4 * cov), cov


# ======================================================================= narrative / meta
THEMES = {
    "Dogs": ["dog", "doge", "shib", "inu", "puppy", "pup", "woof", "bark", "bonk", "wif", "floki", "corgi", "husky", "pug"],
    "Cats": ["cat", "kitty", "kitten", "meow", "neko", "popcat", "purr", "mog"],
    "Frogs/Pepe": ["pepe", "frog", "kek", "toad", "ribbit", "wojak"],
    "AI": ["ai", "gpt", "agent", "agents", "agi", "llm", "neural", "robot", "bot", "sentient", "grok", "claude", "openai", "deepseek"],
    "Politics": ["trump", "maga", "biden", "harris", "election", "vote", "president", "senate", "congress", "potus"],
    "Celebrity": ["elon", "musk", "kanye", "drake", "snoop", "mrbeast", "tate"],
    "Animals": ["bear", "monkey", "ape", "penguin", "hippo", "duck", "goat", "cow", "pig", "bird", "fish", "whale", "turtle",
                "capybara", "hamster", "squirrel", "fox", "wolf", "lion", "tiger", "panda", "shark", "owl", "bunny"],
    "Food": ["pizza", "burger", "taco", "cheese", "banana", "coffee", "sushi", "ramen", "donut", "fries", "bacon", "cookie"],
    "Space": ["moon", "mars", "rocket", "alien", "ufo", "space", "galaxy", "cosmic", "astro"],
    "Meme culture": ["chad", "gigachad", "sigma", "based", "npc", "skibidi", "rizz", "doomer", "degen", "wagmi", "hodl", "jeet"],
    "Anime/gaming": ["waifu", "anime", "manga", "otaku", "gamer", "gaming", "kawaii", "senpai"],
}


def load_themes():
    try:
        if os.path.exists("themes.json"):
            with open("themes.json") as fh:
                d = json.load(fh)
            if isinstance(d, dict) and d:
                return {k: [str(x).lower() for x in v] for k, v in d.items()}
    except Exception as e:
        print(f"[warn] themes.json ignored: {e}")
    return THEMES


class Narrative:
    """Matches a coin's name/ticker to a theme, then measures that theme's CURRENT meta from the coins being monitored:
    how many related coins are active, how they move, whether group volume is rising. EARLY/DEVELOPING/TRENDING/
    OVERCROWDED/FADING. Says nothing when it cannot tell."""

    def __init__(self, cfg: Config, themes: dict):
        self.cfg, self.themes, self.groups = cfg, themes, {}
        self.vol_hist = defaultdict(lambda: deque(maxlen=60))
        self.names = defaultdict(dict)

    @staticmethod
    def _key(raw):
        return re.sub(r"[^a-z0-9]", "", (raw or "").lower())

    def note_name(self, c: Candidate, now: float):
        for raw in (c.symbol, c.name):
            if len(self._key(raw)) >= 2:
                self.names[self._key(raw)].setdefault(c.mint, now)

    def copycats(self, c: Candidate, now: float) -> int:
        return len({m for raw in (c.symbol, c.name) for m, t in self.names.get(self._key(raw), {}).items()
                    if m != c.mint and now - t < 43200})

    def match(self, c: Candidate):
        text = f"{c.symbol} {c.name}"
        words = {w.lower() for w in re.findall(r"[A-Z]?[a-z]+|[A-Z]+(?![a-z])|\d+", text)} | set(re.findall(r"[a-z0-9]+", text.lower()))
        joined = self._key(text)
        return {t: [k for k in kws if k in words or (len(k) >= 4 and k in joined)] for t, kws in self.themes.items()
                if any(k in words or (len(k) >= 4 and k in joined) for k in kws)}

    def refresh(self, cands, now):
        cfg, by_theme = self.cfg, defaultdict(list)
        for c in cands:
            if c.snaps and not c.dead_reason and (c.snaps[-1].liq or 0) >= cfg.dead_liquidity_usd:
                c.theme_hits = self.match(c)
                for t in c.theme_hits:
                    by_theme[t].append(c)
        groups = {}
        for t, ms in by_theme.items():
            act = [m for m in ms if (m.snaps[-1].liq or 0) >= cfg.gate_liquidity_usd and (m.snaps[-1].vol_h1 or 0) >= cfg.gate_volume_h1_usd]
            h1s = [m.snaps[-1].chg_h1 for m in act if m.snaps[-1].chg_h1 is not None]
            vol = sum(m.snaps[-1].vol_h1 or 0 for m in act)
            hist = self.vol_hist[t]
            if not hist or now - hist[-1][0] >= 60:
                hist.append((now, vol))
            old = [x for x in hist if x[0] <= now - 600]
            trend = (vol / old[-1][1] - 1) if (old and old[-1][1] > 0) else None
            med = statistics.median(h1s) if h1s else None
            pos = sum(1 for x in h1s if x > 0) / len(h1s) if h1s else None
            groups[t] = {"n_active": len(act), "med_h1": med, "pos_share": pos, "vol_trend": trend,
                         "heat": wavg([(ramp(med, -10, 40), .35), (ramp(pos, .3, .8), .25), (ramp(trend, -.2, .5), .25), (ramp(len(act), 1, 6), .15)])}
        self.groups = groups

    def board(self, top=3):
        rows = [(g["heat"], t) for t, g in self.groups.items() if g["n_active"] >= 2 and g["heat"] is not None]
        return sorted(rows, reverse=True)[:top]

    def analyze(self, c: Candidate, now: float) -> dict:
        hits = c.theme_hits or self.match(c)
        if not hits:
            return {"clear": False, "strength": None}
        theme = max(hits, key=lambda t: (self.groups.get(t) or {}).get("n_active", 0))
        g = self.groups.get(theme) or {"n_active": 0, "med_h1": None, "pos_share": None, "vol_trend": None, "heat": None}
        n, med, trend, pos = g["n_active"], g["med_h1"], g["vol_trend"], g["pos_share"]
        copies = self.copycats(c, now)
        if trend is not None and trend <= -.25 and (med is None or med < 0):
            meta = "FADING"
        elif n >= 15 or copies >= 4:
            meta = "OVERCROWDED"
        elif n >= 5 and (pos or 0) >= .6 and ((trend or 0) >= .10 or (med or 0) >= 15):
            meta = "TRENDING"
        elif n >= 3:
            meta = "DEVELOPING"
        else:
            meta = "EARLY"
        s = c.snaps[-1] if c.snaps else None
        rel = (s.chg_h1 - med) if (s and s.chg_h1 is not None and med is not None) else None
        base = 35 if n < 2 else {"EARLY": 66, "DEVELOPING": 66, "TRENDING": 60, "OVERCROWDED": 25, "FADING": 12}[meta]
        base += ((pos - .5) * 40 if pos is not None else 0) + (clamp(rel / 50, -1, 1) * 10 if rel is not None else 0)
        base -= min(30, max(0, copies - 1) * 10)
        return {"clear": True, "theme": theme, "meta": meta, "confirmed": n >= 2, "strength": clamp(base, 0, 100),
                "heat": g["heat"], "n_active": n, "rel_perf": rel}


# ======================================================================= pick classification
def momentum_state(f):
    m5, rf, acc, dd, br = f["chg_m5"], f["rising_frac"], f["vol_accel"], f["dd_from_peak"], f["br_m5"]
    if m5 is None and rf is None:
        return None
    if (m5 is not None and m5 < -2) or (dd is not None and dd <= -.2) or (acc is not None and acc < .7 and (m5 or 0) <= 0):
        return "FADING"
    if (m5 is not None and m5 > 1) and (rf is None or rf >= .6) and ((acc or 0) >= 1.1 or (br or 0) >= .52):
        return "INCREASING"
    return "STABLE"


def looks_manufactured(f) -> bool:
    return bool((f["turnover"] is not None and f["turnover"] > 60) or (f["buys_per_buyer"] is not None and f["buys_per_buyer"] > 6)
                or (f["insider"] is not None and f["insider"] >= 25) or (f["nets"] and f["nets"] >= 2))


def classify_play(c: Candidate, cfg: Config, thr: float) -> dict:
    """Each pick type has its own gates; one good metric never forces a pick. EARLY GEM is the fallback."""
    f, g, narr = c.f, c.subs.get, (c.narr or {})
    mom, fake = momentum_state(f), looks_manufactured(f)
    out = {"category": "NO PLAY", "score": None, "momentum": mom, "near": None}
    if c.vetoes or c.risk >= 60 or (fake and c.risk >= 35) or ((f["chg_h1"] or 0) > 400 and mom == "FADING"):
        out["category"] = "AVOID"
        return out
    m5, h1, acc = f["chg_m5"], f["chg_h1"], f["vol_accel"]
    br = f["br_m5"] if f["br_m5"] is not None else f["br_h1"]
    liq_ok = g("liquidity") is None or g("liquidity") >= 45
    elig = {
        "QUICK SCALP": mom == "INCREASING" and (acc or 0) >= 1.3 and (br or 0) >= .53 and (m5 or 0) >= .5 and (h1 is None or h1 <= 120),
        "MOMENTUM": ((h1 or 0) >= 25 or (m5 or 0) >= 6) and (h1 is None or h1 <= 400) and mom in ("INCREASING", "STABLE")
                    and (f["vol_h1"] or 0) >= 15_000 and (br is None or br >= .5) and (f["dd_from_peak"] is None or f["dd_from_peak"] >= -.2),
        "NARRATIVE META": bool(narr.get("clear") and narr.get("confirmed") and narr.get("meta") in ("EARLY", "DEVELOPING", "TRENDING")
                               and (narr.get("heat") or 0) >= 50 and (h1 is None or h1 <= 60) and mom != "FADING" and c.risk <= 40),
    }
    gem = sum([(f["holders_growth_hr"] or 0) >= 3, (f["buyers_h1"] or 0) >= 40 and (f["buys_per_buyer"] is None or f["buys_per_buyer"] <= 3),
               f["liq_chg_first"] is not None and f["liq_chg_first"] >= -0.05, (acc or 0) >= 1.0,
               bool(narr.get("clear") and narr.get("meta") in ("EARLY", "DEVELOPING"))])
    elig["EARLY GEM"] = (gem >= 3 and (h1 is None or -15 <= h1 <= 45) and mom != "FADING" and f["span_s"] >= 300
                         and (g("distribution") is None or g("distribution") >= 50) and (g("insiders") is None or g("insiders") >= 55) and c.risk <= 45)
    lag = ramp(None if narr.get("rel_perf") is None else -narr["rel_perf"], -30, 30)
    fits = {
        "QUICK SCALP": wavg([(g("volume"), .25), (g("buy_sell"), .25), (g("momentum"), .25), (g("liquidity"), .15), (ramp(acc, 1.0, 2.2), .10)]),
        "MOMENTUM": wavg([(g("momentum"), .30), (g("volume"), .25), (g("buy_sell"), .20), (g("organic"), .15), (g("liquidity"), .10)]),
        "NARRATIVE META": wavg([(wavg([(narr.get("strength"), .45), (narr.get("heat"), .35), (lag, .20)]), .35), (g("liquidity"), .12),
                                (g("distribution"), .10), (g("insiders"), .10), (g("buy_sell"), .10), (g("organic"), .08), (g("holders"), .10),
                                (g("continuation"), .05)]),
        "EARLY GEM": wavg([(g("holders"), .22), (g("organic"), .20), (g("continuation"), .14), (g("liquidity"), .14), (g("distribution"), .10),
                           (g("insiders"), .10), (g("volume"), .10)]),
    }
    factor = max(0.0, 1.0 - 1.15 * (c.risk / 100.0) ** 1.3) * (0.6 + 0.4 * c.coverage)
    scores = {k: fits[k] * factor for k in CATS if elig[k] and liq_ok and not fake and fits[k] is not None}
    margins = {k: v - (thr + cfg.thr_offset.get(k, 0)) for k, v in scores.items()}
    out["near"] = max(margins.values()) if margins else None

    def pick(group):
        q = {k: margins[k] for k in group if k in margins and margins[k] >= 0}
        return max(q, key=q.get) if q else None

    best = pick(("QUICK SCALP", "MOMENTUM", "NARRATIVE META")) or pick(("EARLY GEM",))
    if best:
        out.update(category=best, score=scores[best])
    return out


# ======================================================================= embeds (trader view)
def links_line(mint: str, pair: Optional[str], url: str, info: dict) -> str:
    links = [f"[Axiom](https://axiom.trade/meme/{pair})" if pair else None,
             f"[DEX]({url or f'https://dexscreener.com/solana/{mint}'})",
             f"[Bubblemaps](https://app.bubblemaps.io/sol/token/{mint})",
             f"[RugCheck](https://rugcheck.xyz/tokens/{mint})"]
    x = next((s.get("url") for s in (info or {}).get("socials") or [] if str(s.get("type", "")).lower() in ("twitter", "x") and s.get("url")), None)
    links.append(f"[X]({x})" if x else f"[X search](https://x.com/search?q={mint})")
    return " · ".join(l for l in links if l)


def mind_line(score: Optional[float]) -> str:
    """Mind Score = the bot's confidence in the setup (not a chance of profit)."""
    if score is None:
        return "🧠 **Mind** n/a"
    score = round(score)
    level ="HIGH" if score >= 80 else ("MEDIUM" if score >= 65 else "LOW")
    return f"🧠 **Mind {score:.0f}/100** · {level} confidence"


def why_line(c: Candidate, label: str) -> str:
    """1-2 sentence reason built only from measured data (nothing invented)."""
    f, n, bits = c.f, (c.narr or {}), []
    if label == "FINAL STRETCH" and f.get("bond_progress"):
        bits.append(f"bonding curve ~{min(f['bond_progress'], 1.0):.0%} full, so migration could bring fresh buyers")
    if label == "MIGRATED" and f.get("mig_min") is not None:
        bits.append(f"just migrated ~{f['mig_min']:.0f}m ago with new liquidity and visibility")
    if n.get("clear") and n.get("confirmed") and n.get("meta") in ("EARLY", "DEVELOPING", "TRENDING"):
        t = f"{n['theme']} meta is {n['meta'].lower()} ({n['n_active']} related coins active)"
        if n.get("rel_perf") is not None and n["rel_perf"] <= -10:
            t += " and this one hasn't run yet"
        bits.append(t)
    acc, br = f.get("vol_accel"), (f.get("br_m5") if f.get("br_m5") is not None else f.get("br_h1"))
    if acc and acc >= 1.3:
        bits.append(f"volume is running {acc:.1f}× its hourly pace")
    if br and br >= 0.55:
        bits.append(f"{br:.0%} of recent trades are buys")
    if (f.get("holders_growth_hr") or 0) >= 3:
        bits.append(f"holders growing {f['holders_growth_hr']:.0f}%/h")
    if (f.get("buyers_h1") or 0) >= 80:
        bits.append(f"{f['buyers_h1']} unique buyers in the last hour")
    if momentum_state(f) == "INCREASING" and f.get("chg_m5") is not None:
        bits.append(f"price {pct(f['chg_m5'])} in 5m and still climbing")
    if (f.get("liq_chg_first") or 0) >= 0.10:
        bits.append("liquidity growing")
    if f.get("top10") is not None and f["top10"] <= 20:
        bits.append(f"clean holder spread (top 10 {f['top10']:.0f}%)")
    bits = bits[:3] or ["steady two-way trading and every safety check passed"]
    text = bits[0] if len(bits) == 1 else ", ".join(bits[:-1]) + ", and " + bits[-1]
    return "💡 **Why it could move:** " + text[0].upper() + text[1:] + "."


def build_coin_embed(c: Candidate, label: str, score: Optional[float] = None) -> dict:
    s, f, rc = c.snaps[-1], c.f, c.rc or {}
    emoji, color = STYLE.get(label, ("•", 0x95A5A6))
    score = score if score is not None else c.score
    tx = f"{f['buys_h1']} buys / {f['sells_h1']} sells" + (f" · {f['buyers_h1']} buyers" if f.get("buyers_h1") else "")
    hold = f"{f['holders']:,}" if f.get("holders") is not None else "n/a"
    p = lambda v: "n/a" if v is None else (f"{v:.1f}%" if v < 10 else f"{v:.0f}%")
    lp = "Bonding curve" if f.get("dex_stage") == "bonding" else f"LP 🔥 {p(f.get('lp_locked'))}"
    lines = [mind_line(score), why_line(c, label), "",
             f"**MC** {usd(s.mcap)} · **Liq** {usd(s.liq)} · **Age** {ago(f['age_min'] * 60)}",
             f"**Vol** 5m {usd(s.vol_m5)} · 1h {usd(s.vol_h1)}",
             f"**Price** 5m {pct(s.chg_m5)} · 1h {pct(s.chg_h1)}",
             f"**Txns 1h** {tx}",
             f"**Holders** {hold} · **Top 10** {p(f.get('top10'))} · **Dev** {p(rc.get('deployer_pct'))}",
             f"🛡️ Mint/Freeze ✅ · {lp} · Insiders {p(f.get('insider'))} · Sell ✅"]
    if c.bm:
        lines.append(f"🫧 Top cluster {c.bm['top_cluster']:.0%}" + (f" · Bundles {c.bm['bundles']:.0%}" if c.bm.get("bundles") is not None else ""))
    n = c.narr or {}
    if n.get("clear") and n.get("confirmed"):
        lines.append(f"🧠 {n['theme']} meta · {n['meta'].lower()}")
    lines.append(links_line(c.mint, c.pair_addr, c.url, c.info))
    embed = {"title": f"{emoji} {label} · ${c.symbol}"[:256], "description": "\n".join(lines)[:4000], "color": color,
             "footer": {"text": "Mind = bot confidence, not a profit chance · DYOR"}, "timestamp": now_iso()}
    if c.url:
        embed["url"] = c.url
    if c.icon:
        embed["thumbnail"] = {"url": c.icon}
    return embed


def build_milestone_embed(call: Call, m: int, now: float) -> dict:
    emoji = "🚀" if m >= 10 else ("🔥" if m >= 5 else "📈")
    desc = (f"**{call.mult():.1f}×** since call · peak **{call.peak_mult():.1f}×**\n"
            f"MC {usd(call.mcap0)} → {usd(call.last_mcap)} · called {ago(now - call.t0)} ago as {call.labels[0] if call.labels else 'call'}\n"
            + links_line(call.mint, call.pair_addr, call.url, {}))
    return {"title": f"{emoji} {m}× · ${call.symbol}", "description": desc, "color": 0x2ECC71 if m < 10 else 0xF1C40F, "timestamp": now_iso()}


def build_dead_embed(call: Call) -> dict:
    return {"title": f"💀 ${call.symbol} died", "color": 0x7F8C8D, "timestamp": now_iso(),
            "description": f"{call.end_reason.capitalize()}. Peak {call.peak_mult():.1f}× · last {call.mult():.2f}×."}


def call_stats(calls: list) -> dict:
    peaks = [c.peak_mult() for c in calls]
    return {"n": len(calls), "x2": sum(p >= 2 for p in peaks), "x5": sum(p >= 5 for p in peaks), "dead": sum(c.status == "dead" for c in calls),
            "best": max(calls, key=lambda c: c.peak_mult()) if calls else None}


def build_board_embed(calls: list, now: float) -> Optional[dict]:
    day = [c for c in calls if c.t0 >= now - 86400]
    if not day:
        return None
    st, icon = call_stats(day), {"live": "🟢", "dead": "💀", "done": "⚪"}
    by_label = defaultdict(list)
    for c in day:
        by_label[c.labels[0] if c.labels else "call"].append(c)
    per = " · ".join(f"{k} {sum(c.peak_mult() >= 2 for c in v)}/{len(v)}" for k, v in sorted(by_label.items(), key=lambda kv: -len(kv[1])))
    top = sorted(day, key=lambda c: -c.peak_mult())[:8]
    lines = [f"{icon.get(c.status, '•')} **${c.symbol}** peak {c.peak_mult():.1f}× · now {c.mult():.2f}× · {ago(now - c.t0)} ago" for c in top]
    desc = (f"**{st['n']} calls** · {st['x2']} hit 2×+ · {st['x5']} hit 5×+ · {st['dead']} died\n"
            f"2×+ by type: {per}\n\n" + "\n".join(lines))
    return {"title": "📊 Calls · last 24h", "description": desc[:4000], "color": 0x3498DB, "timestamp": now_iso(),
            "footer": {"text": "Peaks use sampled prices"}}


def build_heartbeat_embed(bot: "Bot", now: float) -> dict:
    st, lanes = bot.stats, bot.lane_counts()
    meta = ", ".join(t for _, t in bot.narr.board(3)) or "none confirmed"
    feed = bot.feed.status() if bot.feed else "off"
    desc = (f"**Live feed** {feed} · {st['launches']:,} launches · {st['migrations']:,} migrations · "
            f"{st['launches_promoted']:,} got liquidity · {len(bot.pending):,} new launches waiting\n"
            f"**Scanned** {st['discovered']:,} coins · {st['light_scans']:,} refreshes · watching {len(bot.cands)}\n"
            f"**Lanes** New {lanes['NEW PAIR']} · Final stretch {lanes['FINAL STRETCH']} · Migrated {lanes['MIGRATED']}\n"
            f"**Posted** {st['picks']} picks · {st['radar']} radar · rejected {st['vetoed']} unsafe\n"
            f"**Safety** {bot.safety_summary()}\n"
            f"**Tracking** {bot.tracking_line(now)}\n"
            f"**Hot metas** {meta}")
    blockers = bot.blockers()
    if blockers:
        desc += "\n**Blockers** " + "; ".join(blockers)
    return {"title": "💓 Radar heartbeat", "description": desc[:4000], "color": 0xE91E63, "timestamp": now_iso(),
            "footer": {"text": f"{VERSION} · up {ago(now - bot.started)} · not a coin alert"}}


# ======================================================================= live launch feed
def classify_feed_event(msg) -> Optional[tuple]:
    """('launch' | 'migration', mint, msg) from a PumpPortal message; None for anything else (e.g. subscribe acks)."""
    if not isinstance(msg, dict):
        return None
    mint = msg.get("mint") or msg.get("baseMint")
    if not isinstance(mint, str) or len(mint) < 32 or mint in STABLES:
        return None
    tx = str(msg.get("txType") or "").lower()
    if "migrat" in tx or tx in ("complete", "create_pool"):
        return ("migration", mint, msg)
    if tx == "create" or msg.get("name") or msg.get("symbol"):
        return ("launch", mint, msg)
    return None


class LaunchFeed:
    """ONE websocket to PumpPortal's free new-token + migration streams (their rule: one connection at a time).
    Runs in a background thread, reconnects with backoff (10s doubling to 5 min) so it never hammers the server."""

    def __init__(self, url: str = PUMPPORTAL_URL):
        self.url = url + (f"?api-key={PUMPPORTAL_KEY}" if PUMPPORTAL_KEY else "")
        self.q = queue.Queue(maxsize=50_000)
        self.stop, self.connected, self.err = False, False, ""
        self.thread = None

    def start(self) -> bool:
        try:
            import websocket  # noqa: F401  (package: websocket-client)
        except ImportError:
            self.err = "websocket-client not installed (pip install websocket-client)"
            print(f"[feed] {self.err}")
            return False
        self.thread = threading.Thread(target=self._run, name="launch-feed", daemon=True)
        self.thread.start()
        return True

    def _run(self):
        import websocket
        backoff = 10
        while not self.stop:
            ws = None
            try:
                ws = websocket.create_connection(self.url, timeout=30)
                ws.send(json.dumps({"method": "subscribeNewToken"}))
                ws.send(json.dumps({"method": "subscribeMigration"}))
                self.connected, self.err, backoff = True, "", 10
                print("[feed] connected to the live launch feed")
                while not self.stop:
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        ws.ping()
                        continue
                    if not raw:
                        break
                    try:
                        ev = classify_feed_event(json.loads(raw))
                    except ValueError:
                        continue
                    if ev:
                        try:
                            self.q.put_nowait(ev)
                        except queue.Full:
                            pass
            except Exception as e:
                self.err = str(e)[:150]
                print(f"[feed] disconnected: {self.err}")
            finally:
                self.connected = False
                try:
                    if ws:
                        ws.close()
                except Exception:
                    pass
            if not self.stop:
                time.sleep(backoff)
                backoff = min(backoff * 2, 300)

    def drain(self, limit: int = 3000) -> list:
        out = []
        while len(out) < limit:
            try:
                out.append(self.q.get_nowait())
            except queue.Empty:
                break
        return out

    def status(self) -> str:
        if self.thread is None:
            return "off" + (f" ({self.err})" if self.err else "")
        return "connected" if self.connected else ("reconnecting" + (f" ({self.err})" if self.err else ""))


# ======================================================================= scanner
class Bot:
    def __init__(self, cfg: Config, clock=time.time):
        self.cfg, self.clock = cfg, clock
        self.cands: dict = {}
        self.seen: dict = {}
        self.calls: dict = {}
        self.posted: dict = {}            # mint -> labels already posted (survives restarts)
        self.bad_deployers: set = set()
        self.narr = Narrative(cfg, load_themes())
        self.sent_times, self.radar_times = deque(), defaultdict(deque)
        self.sched = defaultdict(float)
        self._narr_next = self._regime_next = self._status_next = self._save_next = 0.0
        self.pool = ThreadPoolExecutor(cfg.workers) if cfg.workers > 0 else None
        self.inflight: dict = {}
        self.regime = {"heat": 0.5, "threshold": cfg.signal_threshold, "budget": 5, "known": False}
        self.grad_obs: list = []
        self.pump_dexes, self.pump_dexes_t, self.dex_future, self._lane_i = None, 0.0, None, 0
        self.started = clock()
        self.last_heartbeat_t = self.last_board_t = 0.0
        self.feed = LaunchFeed() if cfg.launch_feed else None
        self.pending: dict = {}           # new launches waiting for their 3/8/15/30-min check
        self.status_lines = deque(maxlen=60)
        self.stop = False
        self.stats = Counter()

    # ---------- files
    def init_files(self):
        if not os.path.exists(self.cfg.log_file):
            with open(self.cfg.log_file, "w", newline="") as fh:
                csv.writer(fh).writerow(["time", "event", "symbol", "mint", "price", "detail"])
        if not os.path.exists(self.cfg.signals_file):
            open(self.cfg.signals_file, "w").close()

    def log(self, event, symbol, mint, price, detail=""):
        with open(self.cfg.log_file, "a", newline="") as fh:
            csv.writer(fh).writerow([time.strftime("%F %T"), event, symbol, mint, price, detail])
        print(f"[{event}] {symbol} @ {price} {detail}")

    def jlog(self, rec: dict):
        with open(self.cfg.signals_file, "a") as fh:
            fh.write(json.dumps(rec, default=str) + "\n")

    def submit(self, fn, *a):
        if self.pool:
            return self.pool.submit(fn, *a)
        fut = Future()
        try:
            fut.set_result(fn(*a))
        except Exception as e:
            fut.set_exception(e)
        return fut

    # ---------- lanes (Axiom Pulse style)
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
        elif f["dex_stage"] == "graduated" and "pumpswap" in (c.dex or "").lower():
            mig = f["age_min"]
        if mig is not None and mig <= cfg.migrated_window_min:
            lanes.append("MIGRATED")
        if f["age_min"] <= cfg.new_pair_max_age_min:
            lanes.append("NEW PAIR")
        return {"lanes": lanes, "bond_progress": prog, "mig_min": mig}

    def lane_pools(self, kind: str):
        now = time.time()
        if self.pump_dexes is not None and not any(self.pump_dexes) and now - self.pump_dexes_t > 600:
            self.pump_dexes = self.dex_future = None
        if self.pump_dexes is None:
            if self.dex_future is None:
                self.dex_future = self.submit(fetch_pump_dexes)
            if not self.dex_future.done():
                return None
            try:
                self.pump_dexes = self.dex_future.result()
            except Exception:
                self.pump_dexes = ([], [])
            self.pump_dexes_t = now
        ids = self.pump_dexes[0] if kind == "final" else self.pump_dexes[1]
        if not ids:
            return []
        self._lane_i += 1
        return fetch_gecko_dex_pools(ids[self._lane_i % len(ids)], 1 + (self._lane_i // len(ids)) % 2)

    # ---------- discovery
    def discover(self, now):
        c = self.cfg

        def ranked(items):
            for i, it in enumerate(items or []):
                it["rank"] = i + 1
            return items

        jobs = (("new", c.discovery_new_s, lambda: fetch_gecko_pools("new_pools")),
                ("trending", c.discovery_trending_s, lambda: ranked(fetch_gecko_pools("trending_pools"))),
                ("final", c.discovery_final_s, lambda: self.lane_pools("final")),
                ("migrated", c.discovery_migrated_s, lambda: self.lane_pools("migrated")),
                ("profiles", c.discovery_profiles_s, lambda: fetch_dex_feed("/token-profiles/latest/v1")),
                ("boosts", c.discovery_boosts_s, lambda: fetch_dex_feed("/token-boosts/latest/v1")))
        for name, every, fn in jobs:
            if now >= self.sched[name]:
                items = fn()
                self.sched[name] = now + (every if items is not None else 5)
                for it in items or []:
                    self.ingest(it, now)
        if now >= self.sched["buyers"]:
            self.sched["buyers"] = now + c.buyers_refresh_s
            pick = sorted((x for x in self.cands.values() if x.pair_addr and (x.tier in ("strong", "watch") or x.tracked)),
                          key=lambda x: -(x.cheap or 0))[:30]
            res = fetch_gecko_buyers([x.pair_addr for x in pick]) or {}
            for x in pick:
                if x.pair_addr in res:
                    x.buyers_h1, x.sellers_h1 = res[x.pair_addr]
                    x.dirty = True

    def ingest(self, it: dict, now: float):
        mint = it["mint"]
        c = self.cands.get(mint)
        if c is None:
            if (self.seen.get(mint) or 0) > now:
                return
            if len(self.cands) >= self.cfg.max_candidates and not self.evict_weakest():
                return
            c = Candidate(mint=mint, first_seen=now)
            self.remember(c)
            self.cands[mint] = c
            self.stats["discovered"] += 1
        for k_src, k_dst in (("symbol", "symbol"), ("name", "name"), ("description", "description"), ("icon", "icon"),
                             ("pair", "pair_addr"), ("created_ms", "created_ms")):
            if it.get(k_src) and getattr(c, k_dst, None) in (None, "", "?"):
                setattr(c, k_dst, it[k_src])
        if it.get("links") and not c.links:
            c.links = it["links"]
        if it.get("buyers_h1") is not None:
            c.buyers_h1, c.sellers_h1 = it["buyers_h1"], it.get("sellers_h1")
        if it.get("rank"):
            c.trend_rank = it["rank"]
        sn = it.get("snap")
        if sn is not None and (not c.snaps or sn.t - c.snaps[-1].t >= 5):
            self.add_snap(c, sn, now)
        self.narr.note_name(c, now)

    # ---------- live launch feed
    def drain_feed(self, now):
        if not self.feed:
            return
        cfg = self.cfg
        for kind, mint, msg in self.feed.drain():
            if kind == "launch":
                self.stats["launches"] += 1
                if mint in self.cands or mint in self.pending or (self.seen.get(mint) or 0) > now:
                    continue
                if len(self.pending) >= cfg.max_pending_launches:
                    self.pending.pop(next(iter(self.pending)))          # drop the oldest
                self.pending[mint] = {"t": now, "n": 0, "next": now + cfg.launch_checks_s[0],
                                      "symbol": str(msg.get("symbol") or "?")[:20], "name": str(msg.get("name") or "")[:60]}
            else:  # migration: always worth a look right away
                self.stats["migrations"] += 1
                self.pending.pop(mint, None)
                c = self.cands.get(mint)
                if c is None and (self.seen.get(mint) or 0) <= now:
                    if len(self.cands) < cfg.max_candidates or self.evict_weakest():
                        c = Candidate(mint=mint, first_seen=now, source="migration")
                        self.remember(c)
                        self.cands[mint] = c
                        self.stats["discovered"] += 1
                if c is not None:
                    c.migrated_t = c.migrated_t or now
                    c.hot_until, c.next_check, c.dirty = now + 300, 0.0, True

    def check_pending(self, now):
        """New launches: promote the ones that got real liquidity + volume; forget the ones still dead at 30 min."""
        cfg = self.cfg
        due = [m for m, p in self.pending.items() if p["next"] <= now][:60]
        for i in range(0, len(due), 30):
            batch = due[i:i + 30]
            pairs = fetch_dex_pairs(batch)
            if pairs is None:
                return
            for mint in batch:
                p, pair = self.pending.get(mint), pairs.get(mint)
                if p is None:
                    continue
                snap = snap_from_dex(pair, now) if pair else None
                if snap and (snap.liq or 0) >= cfg.gate_liquidity_usd and (snap.vol_h1 or 0) >= cfg.gate_volume_h1_usd:
                    del self.pending[mint]
                    self.stats["launches_promoted"] += 1
                    bt = pair.get("baseToken") or {}
                    self.ingest({"mint": mint, "symbol": bt.get("symbol") or p["symbol"], "name": bt.get("name") or p["name"],
                                 "pair": pair.get("pairAddress"), "created_ms": pair.get("pairCreatedAt"), "snap": snap}, now)
                    continue
                p["n"] += 1
                if p["n"] >= len(cfg.launch_checks_s):
                    del self.pending[mint]                               # never got going
                else:
                    p["next"] = p["t"] + cfg.launch_checks_s[p["n"]]

    def evict_weakest(self) -> bool:
        weak = [c for c in self.cands.values() if c.tier == "weak" and not c.tracked]
        if not weak:
            return False
        del self.cands[min(weak, key=lambda c: (c.cheap or 0, -c.first_seen)).mint]
        return True

    # ---------- refresh (batched, tiered)
    def add_snap(self, c: Candidate, s: Snap, now: float):
        prev = c.snaps[-1] if c.snaps else None
        if prev and dex_stage(prev.dex) == "bonding" and dex_stage(s.dex) == "graduated" and c.migrated_t is None:
            c.migrated_t, c.hot_until = now, now + 300
            if s.mcap:
                self.grad_obs = (self.grad_obs + [s.mcap])[-50:]
        c.snaps.append(s)
        self.stats["light_scans"] += 1
        if c.first_snap is None:
            c.first_snap, c.first_dex = s, s.dex
        c.dex = s.dex or c.dex
        if prev and prev.price > 0:   # big change -> re-check soon (and re-verify safety on liquidity moves)
            if abs(s.price / prev.price - 1) >= 0.08:
                c.hot_until = now + 120
            if prev.liq and s.liq and abs(s.liq / prev.liq - 1) >= 0.20:
                c.hot_until, c.rc_t = now + 120, 0.0
        c.dirty = True

    def refresh_due(self, now):
        due = sorted((c for c in self.cands.values() if c.next_check <= now), key=lambda c: (TIER_RANK.get(c.tier, 9), c.next_check))
        cfg = self.cfg
        for i in range(0, min(len(due), 90), 30):
            batch = due[i:i + 30]
            pairs = fetch_dex_pairs([c.mint for c in batch])
            if pairs is None:
                break
            for c in batch:
                p = pairs.get(c.mint)
                s = snap_from_dex(p, now) if p else None
                if s:
                    c.no_pair_since = None
                    bt = p.get("baseToken") or {}
                    c.symbol, c.name = bt.get("symbol") or c.symbol, bt.get("name") or c.name
                    c.url, c.pair_addr = p.get("url") or c.url, p.get("pairAddress") or c.pair_addr
                    c.created_ms = p.get("pairCreatedAt") or c.created_ms
                    if p.get("info"):
                        c.info, c.icon = p["info"], p["info"].get("imageUrl") or c.icon
                    self.add_snap(c, s, now)
                else:
                    c.no_pair_since = c.no_pair_since or now
                    c.dirty = True
                every = {"strong": cfg.interval_strong, "new": cfg.interval_new, "watch": cfg.interval_watch}.get(c.tier, cfg.interval_weak)
                c.next_check = now + (4 if now < c.hot_until else every)

    # ---------- safety lookups (worker threads; only for promising coins)
    def schedule_enrichment(self, now):
        cfg = self.cfg
        free = max(cfg.workers, 1) - len(self.inflight)
        if free <= 0:
            return
        want = []
        for c in self.cands.values():
            if c.dead_reason or c.mint in self.inflight or not c.snaps:
                continue
            s = c.snaps[-1]
            lanes = set(c.f.get("lanes") or [])
            interesting = bool(c.trend_rank) or bool({"FINAL STRETCH", "MIGRATED"} & lanes) or (c.cheap or 0) >= 35
            if not c.tracked and ((s.liq or 0) < cfg.gate_liquidity_usd or (s.vol_h1 or 0) < cfg.gate_volume_h1_usd
                                  or c.f.get("age_min", 0) < cfg.min_pair_age_min or len(c.snaps) < 2 or not interesting):
                continue
            ttl = cfg.rugcheck_ttl_strong if (c.tier == "strong" or c.tracked) else cfg.rugcheck_ttl_watch
            na = c.authority is not True and now - c.authority_t >= cfg.authority_recheck_s
            nr = now - c.rc_t >= ttl
            ns = c.sell_ok is not True and c.sell_fail < 3 and bool(c.rc.get("ok")) and c.authority is True and now - c.sell_t >= 120
            core = [x for x in c.safety_fails if not x.startswith(("Bubblemaps", "still being watched", "sell route", "not checked"))]
            nb = bool(BUBBLEMAPS_KEY) and c.bm is None and not core and now - c.bm_t >= 600
            if na or nr or ns or nb:
                want.append((c, na, nr, ns, nb))
        want.sort(key=lambda x: -((x[0].cheap or 0) + (100 if x[0].tracked else 0)))
        for c, na, nr, ns, nb in want[:free]:
            self.inflight[c.mint] = self.submit(do_enrich, c.mint, c.snaps[-1].price, c.decimals, na, nr, ns, nb, cfg)
            c.authority_t = now if na else c.authority_t
            c.rc_t = now if nr else c.rc_t
            c.sell_t = now if ns else c.sell_t
            c.bm_t = now if nb else c.bm_t

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
                print(f"[warn] lookup failed for {mint}: {e}")
                continue
            if c is None:
                continue
            if "authority" in res:
                if res["authority"] is None:
                    c.authority_t = now - cfg.authority_recheck_s + 20
                else:
                    c.authority = res["authority"]
                c.decimals = res.get("decimals") if res.get("decimals") is not None else c.decimals
            if "rc" in res:
                rc = res["rc"]
                if rc.get("ok"):
                    c.rc, c.rc_ok_t = rc, now
                    if c.top10_first is None:
                        c.top10_first = rc.get("top10_pct")
                    if rc.get("holders") is not None and (not c.holders_hist or now - c.holders_hist[-1][0] >= 60):
                        c.holders_hist = (c.holders_hist + [(now, rc["holders"])])[-60:]
                else:
                    c.rc_t = now - (cfg.rugcheck_ttl_strong if c.tier == "strong" else cfg.rugcheck_ttl_watch) + 30
            if "sell" in res and res["sell"] is not None:
                c.sell_ok, c.sell_fail = (True, 0) if res["sell"] else (False, c.sell_fail + 1)
            if res.get("bm"):
                c.bm = res["bm"]
            c.dirty = True

    # ---------- evaluation
    def dead_reason(self, c: Candidate, f: dict, now: float) -> str:
        cfg, s = self.cfg, c.snaps[-1]
        if c.no_pair_since and now - c.no_pair_since > 600:
            return "no trading pair for 10 min"
        if f["age_min"] > cfg.max_pair_age_min:
            return "too old"
        if s.liq is not None and s.liq < cfg.dead_liquidity_usd and f["obs"] >= 3:
            return "liquidity gone"
        if f["dd_from_peak"] is not None and f["dd_from_peak"] <= -0.85:
            return "down 85% from peak"
        if f["age_min"] > 45 and (s.vol_h1 or 0) < 300:
            return "volume dried up"
        if c.tier == "weak" and now - c.first_seen > 7200 and (c.cheap or 0) < 35:
            return "stale"
        return ""

    def evaluate(self, c: Candidate, now: float) -> bool:
        cfg = self.cfg
        f = features(c, now)
        f.update(self.lane_info(c, f, now))
        c.f, c.narr = f, self.narr.analyze(c, now)
        c.subs = category_scores(f, c.narr)
        c.risk, c.risk_notes, c.vetoes = risk_assessment(c, f, cfg)
        if (c.rc or {}).get("deployer") in self.bad_deployers:
            c.vetoes.append("deployer's earlier coin rugged")
        c.score, c.coverage = compute_score(c.subs, c.risk)
        c.cheap = wavg([(c.subs.get(k), WEIGHTS[k]) for k in CHEAP_KEYS])
        c.safety_fails = safety_gate(c, now, cfg, self.bad_deployers)
        reason = self.dead_reason(c, f, now) or ("veto: " + c.vetoes[0] if c.vetoes else "")
        if reason:
            c.dead_reason = reason
            if not c.tracked:
                self.cands.pop(c.mint, None)
                self.seen[c.mint] = now + (86400 if reason.startswith("veto") else 3600)
                self.stats["dropped"] += 1
                self.stats["vetoed"] += reason.startswith("veto")
                return False
        c.play = classify_play(c, cfg, self.regime["threshold"])
        if c.tracked:
            call = self.calls.get(c.mint)
            c.tier = "strong" if (call and now - call.t0 < 3600) else "watch"
        elif len(c.snaps) < cfg.min_observations or now - c.first_seen < 90:
            c.tier = "new"
        elif now < c.hot_until or (c.play.get("near") is not None and c.play["near"] >= -12) or (c.cheap or 0) >= 60:
            c.tier = "strong"
        else:
            c.tier = "watch" if (c.cheap or 0) >= 40 else "weak"
        return True

    def process_dirty(self, now):
        for c in list(self.cands.values()):
            if c.dirty and c.snaps:
                c.dirty = False
                if self.evaluate(c, now):
                    self.consider_pick(c, now)
                    self.radar_check(c, now)

    # ---------- posting
    def consider_pick(self, c: Candidate, now: float):
        cfg, p, b = self.cfg, c.play, []
        if c.alerted or p.get("category") not in CATS:
            c.blockers = [] if c.alerted else [p.get("category", "no play").lower()]
            return
        if len(c.snaps) < cfg.min_observations or now - c.first_seen < cfg.min_observe_seconds:
            b.append("still observing")
        if c.safety_fails:
            b.append("safety: " + c.safety_fails[0])
        if c.risk > cfg.max_risk:
            b.append(f"risk {c.risk:.0f}")
        if c.coverage < cfg.min_data_coverage:
            b.append("not enough data")
        while self.sent_times and self.sent_times[0] < now - 3600:
            self.sent_times.popleft()
        if not b and len(self.sent_times) >= self.regime["budget"] and p["score"] < self.regime["threshold"] + 8:
            b.append("hourly budget (standouts only)")
        c.blockers = b
        if b:
            return
        c.alerted = True
        self.seen[c.mint] = now + 30 * 86400
        self.sent_times.append(now)
        self.stats["picks"] += 1
        self.log("PICK", c.symbol, c.mint, c.snaps[-1].price, p["category"])
        self.jlog({"type": "pick", "t": now_iso(), "mint": c.mint, "symbol": c.symbol, "category": p["category"], "score": round(p["score"], 1),
                   "risk": round(c.risk, 1), "subs": c.subs, "features": c.f, "narrative": c.narr})
        notify(build_coin_embed(c, p["category"], p["score"]), ca=c.mint, hook=DISCORD_WEBHOOK)
        self.start_call(c, now, p["category"], "signals")

    def radar_check(self, c: Candidate, now: float):
        """#radar mirrors Axiom Pulse: NEW PAIR / FINAL STRETCH / MIGRATED. Only safe coins, no chasing pumps or dumps."""
        cfg, f = self.cfg, c.f
        if not RADAR_WEBHOOK or c.dead_reason or c.vetoes or c.safety_fails or c.risk > cfg.max_risk:
            return
        if (f["chg_h1"] or 0) > cfg.radar_max_h1_gain or (f["dd_from_peak"] is not None and f["dd_from_peak"] < -cfg.radar_max_drawdown):
            return
        lanes = f.get("lanes") or []
        for label, flag in (("MIGRATED", "migr_posted"), ("FINAL STRETCH", "final_posted"), ("NEW PAIR", "new_posted")):
            if label in lanes and not getattr(c, flag):
                dq = self.radar_times[label]
                while dq and dq[0] < now - 3600:
                    dq.popleft()
                if len(dq) >= cfg.radar_per_hour.get(label, 8):
                    continue
                setattr(c, flag, True)
                self.posted.setdefault(c.mint, {"labels": [], "t": now})["labels"].append(label)
                if notify(build_coin_embed(c, label), ca=c.mint, hook=RADAR_WEBHOOK):
                    dq.append(now)
                    self.stats["radar"] += 1
                    self.log("RADAR", c.symbol, c.mint, c.snaps[-1].price, label)
                    self.start_call(c, now, label, "radar")
                return   # one radar post per refresh

    # ---------- call tracker (no exits; just how each posted coin did)
    def remember(self, c: Candidate):
        labels = set((self.posted.get(c.mint) or {}).get("labels") or [])
        c.new_posted = c.new_posted or bool(labels & {"NEW PAIR", "EARLY RADAR"})
        c.final_posted = c.final_posted or "FINAL STRETCH" in labels
        c.migr_posted = c.migr_posted or "MIGRATED" in labels
        call = self.calls.get(c.mint)
        if call:
            c.tracked = call.status == "live"
            c.alerted = c.alerted or call.channel == "signals"

    def start_call(self, c: Candidate, now: float, label: str, channel: str):
        call, s = self.calls.get(c.mint), c.snaps[-1]
        if call and call.status == "live":
            call.labels += [label] if label not in call.labels else []
            call.channel = "signals" if channel == "signals" else call.channel
            return
        self.calls[c.mint] = Call(mint=c.mint, symbol=c.symbol, url=c.url, t0=now, price0=s.price, mcap0=s.mcap, liq0=s.liq, labels=[label],
                                  channel=channel, peak=s.price, peak_t=now, peak_mcap=s.mcap, last_price=s.price, last_t=s.t,
                                  last_mcap=s.mcap, last_liq=s.liq, deployer=(c.rc or {}).get("deployer") or "", pair_addr=c.pair_addr or "")
        c.tracked = True

    def call_hook(self, call: Call):
        return TRACKER_WEBHOOK or (DISCORD_WEBHOOK if call.channel == "signals" else RADAR_WEBHOOK)

    def end_call(self, call: Call, c: Optional[Candidate], now: float, status: str, reason: str):
        call.status, call.end_reason, call.end_t = status, reason, now
        if c:
            c.tracked = False
        if status == "dead" and any(w in reason for w in ("no trading pair", "rugged", "liquidity was pulled")) and call.deployer:
            self.bad_deployers.add(call.deployer)
            print(f"[blacklist] deployer {short_addr(call.deployer)} ({call.symbol} rugged)")
        self.jlog({"type": "call_end", "t": now_iso(), "mint": call.mint, "symbol": call.symbol, "status": status, "reason": reason,
                   "peak_mult": round(call.peak_mult(), 3), "last_mult": round(call.mult(), 3)})
        if status == "dead" and (call.channel == "signals" or TRACKER_WEBHOOK):
            notify(build_dead_embed(call), ca=call.mint, hook=self.call_hook(call))

    def track_calls(self, now):
        cfg = self.cfg
        for mint, call in list(self.calls.items()):
            if call.status != "live":
                continue
            c = self.cands.get(mint)
            if c is None:
                c = Candidate(mint=mint, first_seen=now, symbol=call.symbol, url=call.url, pair_addr=call.pair_addr or None, tracked=True)
                self.remember(c)
                self.cands[mint] = c
                continue
            c.tracked = True
            s = c.snaps[-1] if c.snaps else None
            if s and s.t > call.last_t:
                call.last_price, call.last_t, call.last_mcap, call.last_liq = s.price, s.t, s.mcap, s.liq
                if s.price > call.peak:
                    call.peak, call.peak_t, call.peak_mcap = s.price, s.t, s.mcap
                reached = sum(1 for m in cfg.track_milestones if call.mult() >= m)
                if reached > call.hits:
                    call.hits, m = reached, cfg.track_milestones[reached - 1]
                    self.log("MILESTONE", call.symbol, mint, s.price, f"{m}x")
                    if self.call_hook(call):
                        notify(build_milestone_embed(call, m, now), ca=mint, hook=self.call_hook(call))
            died = ("no trading pair any more (possible rug)" if (c.no_pair_since and now - c.no_pair_since >= cfg.dead_no_price_min * 60)
                    else "RugCheck marks it as rugged" if (c.rc or {}).get("rugged")
                    else "liquidity was pulled" if (s and call.liq0 and s.liq is not None and s.liq < 0.25 * call.liq0 and s.liq < 2 * cfg.dead_liquidity_usd)
                    else "down 90%+ from the call" if (s and s.price <= 0.1 * call.price0) else "")
            if died:
                self.end_call(call, c, now, "dead", died)
            elif now - call.t0 >= cfg.track_hours * 3600:
                self.end_call(call, c, now, "done", "tracking window ended")

    def safety_summary(self) -> str:
        pool = [c for c in self.cands.values() if c.snaps and not c.dead_reason and (c.cheap or 0) >= 40]
        reasons = Counter(c.safety_fails[0].split(" (")[0] for c in pool if c.safety_fails)
        top = ", ".join(f"{r} ({n})" for r, n in reasons.most_common(3))
        return f"{sum(not c.safety_fails for c in pool)}/{len(pool)} promising coins pass" + (f" · top blocks: {top}" if top else "")

    def tracking_line(self, now) -> str:
        st = call_stats([c for c in self.calls.values() if c.t0 >= now - 86400])
        best = st["best"]
        return (f"{sum(c.status == 'live' for c in self.calls.values())} live · 24h: {st['n']} calls, {st['x2']} hit 2×+, {st['dead']} died"
                + (f" · best ${best.symbol} {best.peak_mult():.1f}×" if best and best.peak_mult() >= 1.2 else ""))

    def lane_counts(self):
        return Counter(l for c in self.cands.values() for l in (c.f.get("lanes") or []))

    def blockers(self):
        out = [f"{h} rate-limiting (backing off)" for h, lim in LIMITERS.items() if lim.blocked_until > time.time()]
        if self.feed and self.feed.thread is not None and not self.feed.connected:
            out.append("live launch feed reconnecting")
        if not DISCORD_WEBHOOK:
            out.append("#signals off")
        if not RADAR_WEBHOOK:
            out.append("#radar off")
        return out

    def periodic_posts(self, now):
        cfg = self.cfg
        hook = TRACKER_WEBHOOK or DISCORD_WEBHOOK
        if cfg.board_minutes > 0 and hook and now - self.last_board_t >= cfg.board_minutes * 60:
            emb = build_board_embed(list(self.calls.values()), now)
            if emb:
                self.last_board_t = now
                notify(emb, hook=hook)
        if HEARTBEAT_WEBHOOK and cfg.heartbeat_minutes > 0:
            due = (now - self.started >= 90) if not self.last_heartbeat_t else (now - self.last_heartbeat_t >= cfg.heartbeat_minutes * 60)
            if due:
                self.last_heartbeat_t = now
                notify(build_heartbeat_embed(self, now), hook=HEARTBEAT_WEBHOOK)

    # ---------- market mood -> how picky to be
    def update_regime(self, now):
        if now < self._regime_next:
            return
        cfg, self._regime_next = self.cfg, now + 20
        act = [c for c in self.cands.values() if c.snaps and c.f and (c.snaps[-1].liq or 0) >= cfg.gate_liquidity_usd]
        if len(act) < 8:
            heat, known = 0.5, False
        else:
            pos = sum((c.snaps[-1].chg_h1 or 0) > 0 for c in act) / len(act)
            brs = [c.f["br_h1"] for c in act if c.f.get("br_h1") is not None]
            heat = 0.6 * pos + 0.4 * (ramp(statistics.median(brs), .45, .58) / 100 if brs else 0.5)
            known = True
        self.regime = {"heat": heat, "known": known, "threshold": clamp(cfg.signal_threshold + (0.5 - heat) * 12, 62, 88),
                       "budget": int(round(2 + 8 * heat))}

    # ---------- state
    def save_state(self):
        now = self.clock()
        st = {"seen": dict(list(self.seen.items())[-3000:]), "sent_times": list(self.sent_times), "grad_obs": self.grad_obs,
              "last_heartbeat_t": self.last_heartbeat_t, "last_board_t": self.last_board_t,
              "calls": [vars(c) for c in sorted(self.calls.values(), key=lambda c: c.t0) if c.t0 >= now - self.cfg.calls_keep_days * 86400][-2000:],
              "posted": {m: d for m, d in self.posted.items() if d.get("t", 0) >= now - 3 * 86400},
              "bad_deployers": sorted(self.bad_deployers)[-5000:],
              "pending": dict(list(self.pending.items())[-3000:])}
        tmp = self.cfg.state_file + ".tmp"
        with open(tmp, "w") as fh:
            json.dump(st, fh, default=str)
        os.replace(tmp, self.cfg.state_file)

    def load_state(self):
        if not os.path.exists(self.cfg.state_file):
            return
        now = self.clock()
        try:
            with open(self.cfg.state_file) as fh:
                s = json.load(fh)
            self.seen = {k: v for k, v in (s.get("seen") or {}).items() if isinstance(v, (int, float))}
            self.sent_times = deque(s.get("sent_times", []))
            self.grad_obs = s.get("grad_obs", [])
            self.last_heartbeat_t, self.last_board_t = s.get("last_heartbeat_t", 0.0), s.get("last_board_t", 0.0)
            self.bad_deployers = set(s.get("bad_deployers") or [])
            self.pending = {m: p for m, p in (s.get("pending") or {}).items() if isinstance(p, dict) and now - p.get("t", 0) < 2400}
            self.posted = s.get("posted") or {m: {"labels": d.get("kinds", []), "t": d.get("t", now)} for m, d in (s.get("radar_done") or {}).items()}
            for d in s.get("calls", []):
                try:
                    self.calls[d["mint"]] = Call(**d)
                except TypeError:
                    pass
            for mint, call in self.calls.items():
                if call.status == "live":
                    c = Candidate(mint=mint, first_seen=now, symbol=call.symbol, url=call.url, pair_addr=call.pair_addr or None)
                    self.remember(c)
                    self.cands[mint] = c
        except Exception as e:
            print(f"[warn] could not load state, starting fresh: {e}")

    # ---------- loop
    def tick(self, now):
        self.drain_feed(now)
        self.check_pending(now)
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
        self.track_calls(now)
        self.periodic_posts(now)

    def write_status_file(self, line: str):
        """last_status.txt (committed with state) lets you or Claude see what the bot is doing without opening logs."""
        self.status_lines.append(line)
        try:
            with open("last_status.txt", "w") as fh:
                fh.write(f"{VERSION} | updated {now_iso()} | up {ago(self.clock() - self.started)}\n")
                fh.write(f"feeds: signals={'on' if DISCORD_WEBHOOK else 'OFF'} radar={'on' if RADAR_WEBHOOK else 'OFF'} "
                         f"heartbeat={'on' if HEARTBEAT_WEBHOOK else 'OFF'} tracker={'on' if TRACKER_WEBHOOK else 'off'} "
                         f"rpc={'custom' if os.getenv('SOLANA_RPC_URL') else 'public'} bubblemaps={'on' if BUBBLEMAPS_KEY else 'off'}\n")
                fh.write(f"blockers: {'; '.join(self.blockers()) or 'none'}\n\n" + "\n".join(self.status_lines) + "\n")
        except Exception as e:
            print(f"[warn] status file: {e}")

    def status(self, now):
        lanes, r = self.lane_counts(), self.regime
        line = (f"[status] {now_iso()} feed={self.feed.status() if self.feed else 'off'} launches={self.stats['launches']} "
              f"pending={len(self.pending)} watching={len(self.cands)} new={lanes['NEW PAIR']} final={lanes['FINAL STRETCH']} "
              f"migrated={lanes['MIGRATED']} | picks={self.stats['picks']} radar={self.stats['radar']} | {self.safety_summary()} | "
              f"thr={r['threshold']:.0f} | api/min dex={LIMITERS['dex'].per_min()} gecko={LIMITERS['gecko'].per_min()} "
              f"rugcheck={LIMITERS['rugcheck'].per_min()}")
        print(line)
        self.write_status_file(line)

    def run(self, max_minutes=None):
        self.init_files()
        self.load_state()
        for sg in (os_signal.SIGINT, os_signal.SIGTERM):
            try:
                os_signal.signal(sg, lambda *_: setattr(self, "stop", True))
            except ValueError:
                pass
        on = lambda v: "on" if v else "off"
        if self.feed:
            self.feed.start()
        print(f"{VERSION} started | #signals {on(DISCORD_WEBHOOK)} #radar {on(RADAR_WEBHOOK)} #heartbeat {on(HEARTBEAT_WEBHOOK)} "
              f"#tracker {on(TRACKER_WEBHOOK)} | bubblemaps {on(BUBBLEMAPS_KEY)}")
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
        if self.feed:
            self.feed.stop = True
        try:
            self.save_state()
        finally:
            if self.pool:
                self.pool.shutdown(wait=False, cancel_futures=True)
        print("Stopped; state saved.")


# ======================================================================= tools
def sample_coin(symbol: str, chg_h1: float, now: float, cfg: Config) -> Candidate:
    """A fake coin pushed through the real scoring code, for previewing posts."""
    c = Candidate(mint=f"Sample{symbol}1111111111111111111111111111111", first_seen=now - 900, symbol=symbol, name=f"{symbol.title()} Dog",
                  url="https://dexscreener.com/solana", pair_addr="SamplePair111111111111111111111111111111", created_ms=(now - 50 * 60) * 1000,
                  authority=True, sell_ok=True, buyers_h1=210, sellers_h1=150, dex="raydium",
                  info={"socials": [{"type": "twitter", "url": "https://x.com/example"}]},
                  rc={"ok": True, "holders": 980, "top10_pct": 19.0, "top1_pct": 4.5, "insider_pct": 3.0, "insider_networks": 0,
                      "lp_locked_pct": 100.0, "rugged": False, "danger_risks": 0, "risk_names": [], "warn_names": [],
                      "deployer": "SampleDev", "deployer_pct": 0.8},
                  holders_hist=[(now - 1500, 700), (now, 980)], rc_ok_t=now)
    for i in range(12):
        k = i / 11
        c.snaps.append(Snap(t=now - 660 + i * 60, price=0.0004 * (1 + 0.06 * k), liq=48_000 * (0.92 + 0.08 * k), mcap=900_000 * (1 + 0.06 * k),
                            vol_m5=9_000, vol_h1=42_000, buys_m5=60, sells_m5=30, buys_h1=520, sells_h1=380, chg_m5=3.0, chg_h1=chg_h1, dex="raydium"))
    c.first_snap = c.snaps[0]
    bot = Bot(cfg)
    f = features(c, now)
    f.update(bot.lane_info(c, f, now))
    c.f, c.narr = f, {}
    c.subs = category_scores(f, c.narr)
    c.risk, c.risk_notes, c.vetoes = risk_assessment(c, f, cfg)
    c.score, c.coverage = compute_score(c.subs, c.risk)
    c.safety_fails = safety_gate(c, now, cfg)
    c.play = classify_play(c, cfg, cfg.signal_threshold)
    return c


def send_test():
    cfg, now = Config(workers=0), time.time()
    if not any((DISCORD_WEBHOOK, RADAR_WEBHOOK, HEARTBEAT_WEBHOOK, TRACKER_WEBHOOK)):
        print("No webhook variables set.")
        return
    c = sample_coin("ZAP", 14.0, now, cfg)
    if DISCORD_WEBHOOK:
        label = c.play["category"] if c.play["category"] in CATS else "QUICK SCALP"
        notify(build_coin_embed(c, label, c.play.get("score") or 80), ca=c.mint, hook=DISCORD_WEBHOOK)
        print(f"[test] #signals: {label}")
    if RADAR_WEBHOOK:
        for label in ("NEW PAIR", "FINAL STRETCH", "MIGRATED"):
            c.f["lanes"], c.f["bond_progress"] = [label], 0.88
            notify(build_coin_embed(c, label), ca=c.mint, hook=RADAR_WEBHOOK)
            print(f"[test] #radar: {label}")
            time.sleep(1)
    hook = TRACKER_WEBHOOK or DISCORD_WEBHOOK
    if hook:
        calls = [Call(mint=f"Demo{s}111111111111111111111111111111", symbol=s, url="https://dexscreener.com/solana", t0=now - (i + 2) * 3000,
                      price0=1.0, mcap0=300_000, labels=[lab], channel="signals", peak=pk, last_price=last, last_mcap=300_000 * last,
                      status=st, pair_addr="SamplePair111111111111111111111111111111")
                 for i, (s, lab, pk, last, st) in enumerate((("SEED", "EARLY GEM", 5.6, 4.8, "live"), ("ZAP", "NEW PAIR", 3.4, 1.9, "live"),
                                                             ("RUN", "MOMENTUM", 2.2, 1.4, "live"), ("DOGGO", "MIGRATED", 1.3, 0.05, "dead")))]
        notify(build_milestone_embed(calls[0], 5, now), ca=calls[0].mint, hook=hook)
        notify(build_board_embed(calls, now), hook=hook)
        print("[test] tracker: 5x ping + calls board")
    if HEARTBEAT_WEBHOOK:
        bot = Bot(cfg)
        bot.stats.update(discovered=1234, light_scans=18_420, picks=2, radar=9, vetoed=61)
        notify(build_heartbeat_embed(bot, now), hook=HEARTBEAT_WEBHOOK)
        print("[test] #heartbeat")
    print("Done (fake coins).")


def probe(mint: str):
    data, st = http_json("rugcheck", f"{RUGCHECK}/tokens/{mint}/report", max_wait=30)
    print("RugCheck:", parse_rugcheck(data if st == 200 else None))
    pair = (fetch_dex_pairs([mint]) or {}).get(mint)
    print("DexScreener:", snap_from_dex(pair, time.time()) if pair else "unavailable")
    print("Mint/freeze revoked, decimals:", mint_info(mint))
    if BUBBLEMAPS_KEY:
        print("Bubblemaps:", fetch_bubblemaps(mint))


if __name__ == "__main__":
    args = sys.argv
    if "--test" in args:
        send_test()
    elif "--probe" in args:
        i = args.index("--probe")
        probe(args[i + 1]) if len(args) > i + 1 else print("usage: --probe MINT")
    else:
        Bot(Config()).run(max_minutes=float(args[args.index("--max-minutes") + 1]) if "--max-minutes" in args else None)
