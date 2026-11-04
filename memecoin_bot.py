"""
Solana memecoin filter + tracker for Discord. Scans new coins, throws out the bad ones, and posts ONE kind of alert
for the coins that pass every filter. Then it tracks each alert. No trading, no wallet, no keys.

Discord (webhooks):
  DISCORD_WEBHOOK_URL          alerts: one message per coin (card + tap-to-copy CA)
  DISCORD_TRACKER_WEBHOOK_URL  (optional) tracker: 2x/3x/5x... pings, death notes, calls board.
                               Not set = tracker posts go to the alerts channel.
Optional: SOLANA_RPC_URL, BUBBLEMAPS_API_KEY (adds cluster/bundle checks), JUPITER_API_KEY

Where coins come from: the live pump.fun feed (PumpPortal: every launch and migration), GeckoTerminal new and trending
pools, DexScreener profiles and boosts.

A coin is posted only if it passes BOTH filters (unknown data = fail):
  SAFETY   mint/freeze revoked, clean RugCheck, LP >= 90% locked/burned, top 10 <= 30%, largest holder <= 10%,
           insiders <= 10%, <= 1 wallet cluster, dev <= 5%, liquidity >= $20K and >= 3% of MC, >= 20 min old,
           watched >= 5 min with no liquidity drop, real sellers, no wash trading, Jupiter sell route found,
           deployer not blacklisted, Bubblemaps clusters/bundles under the limits (with a key)
  QUALITY  1h volume >= $15K, >= 100 trades and >= 50 buyers in 1h, more buys than sells, >= 250 holders,
           market cap <= $10M, not already up > 150% in 1h, not down > 25% in 1h or from its recent peak,
           quality score >= 60/100
A deployer whose alerted coin rugs is blacklisted. No filter can guarantee a coin is legit.

Run:   pip install requests websocket-client
       python memecoin_bot.py                  run forever
       python memecoin_bot.py --max-minutes N  run N minutes, save, exit (GitHub Actions runs ~5h40m at a time)
       python memecoin_bot.py --test           send sample posts (fake coin) to the configured channel(s)
       python memecoin_bot.py --probe MINT     show what the bot reads for one token
"""
import csv
import dataclasses
import json
import os
import queue
import re
import signal as os_signal
import sys
import threading
import time
import traceback
from collections import Counter, deque
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import requests

VERSION = "FILTER-5.0"
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
TRACKER_WEBHOOK = os.getenv("DISCORD_TRACKER_WEBHOOK_URL")
PUMPPORTAL_URL = os.getenv("PUMPPORTAL_URL") or "wss://pumpportal.fun/api/data"
PUMPPORTAL_KEY = os.getenv("PUMPPORTAL_API_KEY")

SOL_MINT = "So11111111111111111111111111111111111111112"
STABLES = {SOL_MINT, "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v", "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
TIER_RANK = {"strong": 0, "new": 1, "watch": 2, "weak": 3}


@dataclass
class Config:
    # watch list entry (cheap, before any safety lookups)
    gate_liquidity_usd: float = 8_000
    gate_volume_h1_usd: float = 3_000
    min_pair_age_min: float = 8
    max_pair_age_min: float = 48 * 60
    dead_liquidity_usd: float = 3_000
    max_candidates: int = 1500
    # live launch feed: check each new launch at 3, 8, 15, 30 min old; drop it if still dead
    launch_feed: bool = True
    launch_checks_s: tuple = (180, 480, 900, 1800)
    max_pending_launches: int = 5000
    # refresh cadence (seconds)
    interval_strong: float = 10
    interval_new: float = 15
    interval_watch: float = 30
    interval_weak: float = 60
    discovery_new_s: float = 20
    discovery_trending_s: float = 90
    discovery_profiles_s: float = 30
    discovery_boosts_s: float = 60
    buyers_refresh_s: float = 90
    tick_seconds: float = 1.0
    workers: int = 4
    # lookups
    rugcheck_ttl_strong: float = 90
    rugcheck_ttl_watch: float = 240
    authority_recheck_s: float = 120
    sell_check_usd: float = 20
    # drop from the watch list for good (hard red flags)
    veto_top10_pct: float = 50
    veto_insider_pct: float = 25
    veto_liq_drop_10m: float = 0.50
    veto_liq_mcap: float = 0.008
    # SAFETY filter (unknown = fail)
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
    # QUALITY filter
    min_volume_h1_usd: float = 15_000
    min_trades_h1: int = 100
    min_buyers_h1: int = 50
    min_buy_share: float = 0.50
    min_holders: int = 250
    max_mcap_usd: float = 10_000_000
    max_h1_gain_pct: float = 150
    max_h1_drop_pct: float = 25
    max_drawdown: float = 0.25
    alert_score: float = 60
    min_observations: int = 3
    # posting
    max_alerts_per_hour: int = 6
    standout_bonus: float = 12       # past the hourly cap, only coins scoring alert_score + this get through
    # tracker
    track_hours: float = 24
    track_milestones: tuple = (2, 3, 5, 10, 20, 50, 100)
    board_minutes: float = 120
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
    """ONE message: the CA as the message text (long-press -> copy gives just the CA) with the card under it."""
    url = hook or DISCORD_WEBHOOK
    if not url:
        return False
    payload = {"embeds": [embed], "allowed_mentions": {"parse": []}}
    if ca:
        payload["content"] = f"`{ca}`"
    return _post(url, payload)


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
    symbol: str = "?"
    name: str = ""
    icon: Optional[str] = None
    pair_addr: Optional[str] = None
    url: str = ""
    created_ms: Optional[float] = None
    info: dict = field(default_factory=dict)
    snaps: deque = field(default_factory=lambda: deque(maxlen=120))
    first_snap: Optional[Snap] = None
    tier: str = "new"
    next_check: float = 0.0
    hot_until: float = 0.0
    no_pair_since: Optional[float] = None
    dirty: bool = True
    dead_reason: str = ""
    # lookups
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
    buyers_h1: Optional[int] = None
    sellers_h1: Optional[int] = None
    # filter results
    f: dict = field(default_factory=dict)
    score: Optional[float] = None
    vetoes: list = field(default_factory=list)
    safety_fails: list = field(default_factory=lambda: ["not checked yet"])
    quality_fails: list = field(default_factory=list)
    # post state
    alerted: bool = False
    tracked: bool = False


@dataclass
class Call:
    """An alerted coin, tracked for track_hours from the price at the moment it was posted."""
    mint: str
    symbol: str
    url: str
    t0: float
    price0: float
    mcap0: Optional[float] = None
    liq0: Optional[float] = None
    score0: Optional[float] = None
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


CALL_FIELDS = {f.name for f in dataclasses.fields(Call)}


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
    return [{"mint": d["tokenAddress"], "icon": d.get("icon")}
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
    """(mint+freeze authority revoked, decimals) from the RPC; (None, None) if the call failed."""
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
          "deployer": None, "deployer_pct": None, "authority_revoked": None, "decimals": None}
    if not isinstance(rep, dict):
        return rc
    rc["ok"] = True
    th = _num(rep.get("totalHolders"))
    rc["holders"] = int(th) if th is not None else None
    # mint/freeze authority straight from the report (so the public RPC is only a fallback)
    for src in (rep, rep.get("token") if isinstance(rep.get("token"), dict) else {}):
        if "mintAuthority" in src and "freezeAuthority" in src:
            rc["authority_revoked"] = not src.get("mintAuthority") and not src.get("freezeAuthority")
            break
    dec = _num((rep.get("token") or {}).get("decimals")) if isinstance(rep.get("token"), dict) else None
    rc["decimals"] = int(dec) if dec is not None else None
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
    return {"top_cluster": max(clusters) if clusters else 0.0, "bundles": share(stats.get("bundles"))}


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


def do_enrich(mint, price, decimals, need_rc, need_auth, need_sell, need_bm, cfg) -> dict:
    """Worker thread: returns results only (the main thread applies them)."""
    out = {}
    if need_rc:
        out["rc"] = fetch_rugcheck(mint)
        decimals = out["rc"].get("decimals") if out["rc"].get("decimals") is not None else decimals
    if need_auth:
        out["authority"], out["decimals"] = mint_info(mint)
        decimals = out["decimals"] if out["decimals"] is not None else decimals
    if need_sell:
        out["sell"] = sell_route_check(mint, price, decimals, cfg)
    if need_bm:
        out["bm"] = fetch_bubblemaps(mint)
    return out


# ======================================================================= features + filters
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
         "br_h1": tb / (tb + ts) if tb + ts >= 20 else None, "obs": len(c.snaps), "span_s": s.t - s0.t}
    f["age_min"] = (now * 1000 - c.created_ms) / 60000 if c.created_ms else (now - c.first_seen) / 60
    f["turnover"] = s.vol_h1 / s.liq if (s.vol_h1 is not None and s.liq) else None
    f["liq_mcap"] = s.liq / s.mcap if (s.liq and s.mcap) else None
    peak = max((x.price for x in c.snaps if x.t >= now - 1800), default=s.price)
    f["dd_from_peak"] = s.price / peak - 1 if peak > 0 else None
    l10 = snap_ago(c, now, 600)
    f["liq_chg_10m"] = s.liq / l10.liq - 1 if (l10 and l10.liq and s.liq is not None) else None
    f["liq_chg_first"] = s.liq / s0.liq - 1 if (s0.liq and s.liq is not None) else None
    f["dex_stage"] = dex_stage(s.dex)
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
    return f


def quality_score(f: dict) -> Optional[float]:
    """0-100 from real market + holder data (missing pieces are skipped). Not a chance of profit."""
    return wavg([(ramp(f["liq"], 15_000, 80_000), .15), (ramp(f["liq_mcap"], .03, .10), .05),
                 (ramp(f["vol_h1"], 10_000, 100_000), .15), (ramp(f["br_h1"], .50, .62), .15),
                 (ramp(f["holders"], 150, 1500), .10), (ramp(f["holders_growth_hr"], -2, 15), .10),
                 (ramp(f["buyers_h1"], 50, 300), .10), (ramp(f["buys_per_buyer"], 4, 1.5), .05),
                 (ramp(f["top10"], 30, 12), .10), (ramp(f["insider"], 10, 0), .05)])


def hard_vetoes(c: Candidate, f: dict, cfg: Config, bad_deployers=()) -> list:
    """Red flags that drop a coin from the watch list (it is not looked at again for 24h)."""
    rc, out = (c.rc or {}), []
    if c.authority is False:
        out.append("mint/freeze authority not revoked")
    if rc.get("rugged"):
        out.append("RugCheck: rugged")
    severe = [n for n in rc.get("risk_names") or [] if SEVERE_RE.search(n)]
    if severe:
        out.append(f"RugCheck danger: {severe[0]}")
    elif (rc.get("danger_risks") or 0) >= 2:
        out.append(f"RugCheck: {rc['danger_risks']} danger flags")
    if f["top10"] is not None and f["top10"] >= cfg.veto_top10_pct:
        out.append(f"top 10 hold {f['top10']:.0f}%")
    if f["insider"] is not None and f["insider"] >= cfg.veto_insider_pct:
        out.append(f"insiders hold {f['insider']:.0f}%")
    if f["liq_mcap"] is not None and f["liq_mcap"] < cfg.veto_liq_mcap:
        out.append("liquidity under 0.8% of market cap")
    if f["liq_chg_10m"] is not None and f["liq_chg_10m"] <= -cfg.veto_liq_drop_10m:
        out.append("liquidity collapsing")
    if f["buys_h1"] >= 40 and f["sells_h1"] <= 2:
        out.append("buys but almost no sells (honeypot-like)")
    if c.sell_ok is False and c.sell_fail >= 2:
        out.append("no sell route found")
    if rc.get("deployer") and rc["deployer"] in bad_deployers:
        out.append("deployer's earlier coin rugged")
    return out


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


def quality_filter(f: dict, score: Optional[float], cfg: Config) -> list:
    """Is it worth a look? Real demand, room to run, not already pumped, not dumping. Returns the failures."""
    fails = []
    if (f["vol_h1"] or 0) < cfg.min_volume_h1_usd:
        fails.append(f"1h volume only {usd(f['vol_h1'])}")
    if f["tx_h1"] < cfg.min_trades_h1:
        fails.append(f"only {f['tx_h1']} trades in 1h")
    if f["br_h1"] is None or f["br_h1"] < cfg.min_buy_share:
        fails.append("more selling than buying")
    if f["buyers_h1"] is not None and f["buyers_h1"] < cfg.min_buyers_h1:
        fails.append(f"only {f['buyers_h1']} buyers in 1h")
    if f["holders"] is not None and f["holders"] < cfg.min_holders:
        fails.append(f"only {f['holders']} holders")
    if f["mcap"] is not None and f["mcap"] > cfg.max_mcap_usd:
        fails.append(f"market cap already {usd(f['mcap'])}")
    if f["chg_h1"] is not None and f["chg_h1"] > cfg.max_h1_gain_pct:
        fails.append(f"already up {f['chg_h1']:.0f}% in 1h")
    if f["chg_h1"] is not None and f["chg_h1"] < -cfg.max_h1_drop_pct:
        fails.append(f"down {-f['chg_h1']:.0f}% in 1h")
    if f["dd_from_peak"] is not None and f["dd_from_peak"] < -cfg.max_drawdown:
        fails.append("dumping from recent peak")
    if (f["buys_m5"] + f["sells_m5"]) >= 10 and f["sells_m5"] >= 2 * f["buys_m5"]:
        fails.append("sell pressure right now")
    if score is None or score < cfg.alert_score:
        fails.append(f"score {score or 0:.0f}/100 (needs {cfg.alert_score:.0f})")
    return fails


# ======================================================================= embeds
def links_line(mint: str, pair: Optional[str], url: str, info: dict) -> str:
    links = [f"[Axiom](https://axiom.trade/meme/{pair})" if pair else None,
             f"[DEX]({url or f'https://dexscreener.com/solana/{mint}'})",
             f"[Bubblemaps](https://app.bubblemaps.io/sol/token/{mint})",
             f"[RugCheck](https://rugcheck.xyz/tokens/{mint})"]
    x = next((s.get("url") for s in (info or {}).get("socials") or [] if str(s.get("type", "")).lower() in ("twitter", "x") and s.get("url")), None)
    links.append(f"[X]({x})" if x else f"[X search](https://x.com/search?q={mint})")
    return " · ".join(l for l in links if l)


def why_line(f: dict) -> str:
    """Short reason built only from measured data."""
    bits = []
    if f.get("br_h1") is not None:
        bits.append(f"{f['br_h1']:.0%} of 1h trades are buys")
    if (f.get("buyers_h1") or 0) >= 80:
        bits.append(f"{f['buyers_h1']} unique buyers in 1h")
    if (f.get("holders_growth_hr") or 0) >= 3:
        bits.append(f"holders growing {f['holders_growth_hr']:.0f}%/h")
    if (f.get("liq_chg_first") or 0) >= 0.10:
        bits.append("liquidity growing")
    if f.get("top10") is not None and f["top10"] <= 20:
        bits.append(f"clean holder spread (top 10 {f['top10']:.0f}%)")
    bits = bits[:3] or ["steady two-way trading"]
    text = bits[0] if len(bits) == 1 else ", ".join(bits[:-1]) + " and " + bits[-1]
    return "💡 " + text[0].upper() + text[1:] + " · every safety check passed."


def build_alert_embed(c: Candidate) -> dict:
    s, f, rc = c.snaps[-1], c.f, c.rc or {}
    p = lambda v: "n/a" if v is None else (f"{v:.1f}%" if v < 10 else f"{v:.0f}%")
    hold = f"{f['holders']:,}" if f.get("holders") is not None else "n/a"
    tx = f"{f['buys_h1']} buys / {f['sells_h1']} sells" + (f" · {f['buyers_h1']} buyers" if f.get("buyers_h1") else "")
    lp = "Bonding curve" if f.get("dex_stage") == "bonding" else f"LP 🔥 {p(f.get('lp_locked'))}"
    lines = [why_line(f), "",
             f"**MC** {usd(s.mcap)} · **Liq** {usd(s.liq)} · **Age** {ago(f['age_min'] * 60)}",
             f"**Vol** 5m {usd(s.vol_m5)} · 1h {usd(s.vol_h1)}",
             f"**Price** 5m {pct(s.chg_m5)} · 1h {pct(s.chg_h1)}",
             f"**Txns 1h** {tx}",
             f"**Holders** {hold} · **Top 10** {p(f.get('top10'))} · **Dev** {p(rc.get('deployer_pct'))} · **Insiders** {p(f.get('insider'))}",
             f"🛡️ Mint/Freeze ✅ · {lp} · RugCheck ✅ · Sell ✅"]
    if c.bm:
        lines.append(f"🫧 Top cluster {c.bm['top_cluster']:.0%}" + (f" · Bundles {c.bm['bundles']:.0%}" if c.bm.get("bundles") is not None else ""))
    lines.append(links_line(c.mint, c.pair_addr, c.url, c.info))
    embed = {"title": f"🟢 ${c.symbol} · score {c.score or 0:.0f}/100"[:256], "description": "\n".join(lines)[:4000], "color": 0x2ECC71,
             "footer": {"text": "Passed every filter · not financial advice · DYOR"}, "timestamp": now_iso()}
    if c.url:
        embed["url"] = c.url
    if c.icon:
        embed["thumbnail"] = {"url": c.icon}
    return embed


def build_milestone_embed(call: Call, m: int, now: float) -> dict:
    emoji = "🚀" if m >= 10 else ("🔥" if m >= 5 else "📈")
    desc = (f"**{call.mult():.1f}×** since alert · peak **{call.peak_mult():.1f}×**\n"
            f"MC {usd(call.mcap0)} → {usd(call.last_mcap)} · alerted {ago(now - call.t0)} ago\n"
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
    top = sorted(day, key=lambda c: -c.peak_mult())[:10]
    lines = [f"{icon.get(c.status, '•')} **${c.symbol}** peak {c.peak_mult():.1f}× · now {c.mult():.2f}× · {ago(now - c.t0)} ago" for c in top]
    desc = f"**{st['n']} alerts** · {st['x2']} hit 2×+ · {st['x5']} hit 5×+ · {st['dead']} died\n\n" + "\n".join(lines)
    return {"title": "📊 Tracker · last 24h", "description": desc[:4000], "color": 0x3498DB, "timestamp": now_iso(),
            "footer": {"text": "Peaks use sampled prices"}}


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
    Runs in a background thread, reconnects with backoff (10s doubling to 5 min)."""

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


# ======================================================================= bot
class Bot:
    def __init__(self, cfg: Config, clock=time.time):
        self.cfg, self.clock = cfg, clock
        self.cands: dict = {}
        self.seen: dict = {}              # mint -> don't look again before this time
        self.calls: dict = {}
        self.bad_deployers: set = set()
        self.sent_times = deque()
        self.sched = {}
        self._status_next = self._save_next = 0.0
        self.pool = ThreadPoolExecutor(cfg.workers) if cfg.workers > 0 else None
        self.inflight: dict = {}
        self.started = clock()
        self.last_board_t = 0.0
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

    # ---------- discovery
    def discover(self, now):
        cfg = self.cfg

        def ranked(items):
            for i, it in enumerate(items or []):
                it["rank"] = i + 1
            return items

        jobs = (("new", cfg.discovery_new_s, lambda: fetch_gecko_pools("new_pools")),
                ("trending", cfg.discovery_trending_s, lambda: ranked(fetch_gecko_pools("trending_pools"))),
                ("profiles", cfg.discovery_profiles_s, lambda: fetch_dex_feed("/token-profiles/latest/v1")),
                ("boosts", cfg.discovery_boosts_s, lambda: fetch_dex_feed("/token-boosts/latest/v1")))
        for name, every, fn in jobs:
            if now >= self.sched.get(name, 0.0):
                items = fn()
                self.sched[name] = now + (every if items is not None else 5)
                for it in items or []:
                    self.ingest(it, now)
        if now >= self.sched.get("buyers", 0.0):
            self.sched["buyers"] = now + cfg.buyers_refresh_s
            pick = sorted((x for x in self.cands.values() if x.pair_addr and (self.worth_checking(x) or x.tracked)),
                          key=lambda x: -(x.score or 0))[:30]
            res = fetch_gecko_buyers([x.pair_addr for x in pick]) or {}
            for x in pick:
                if x.pair_addr in res:
                    x.buyers_h1, x.sellers_h1 = res[x.pair_addr]
                    x.dirty = True

    def add_candidate(self, mint: str, now: float) -> Optional[Candidate]:
        if (self.seen.get(mint) or 0) > now:
            return None
        if len(self.cands) >= self.cfg.max_candidates and not self.evict_weakest():
            return None
        c = Candidate(mint=mint, first_seen=now)
        self.remember(c)
        self.cands[mint] = c
        self.stats["discovered"] += 1
        return c

    def ingest(self, it: dict, now: float):
        c = self.cands.get(it["mint"]) or self.add_candidate(it["mint"], now)
        if c is None:
            return
        for k_src, k_dst in (("symbol", "symbol"), ("name", "name"), ("icon", "icon"), ("pair", "pair_addr"), ("created_ms", "created_ms")):
            if it.get(k_src) and getattr(c, k_dst, None) in (None, "", "?"):
                setattr(c, k_dst, it[k_src])
        if it.get("buyers_h1") is not None:
            c.buyers_h1, c.sellers_h1 = it["buyers_h1"], it.get("sellers_h1")
        sn = it.get("snap")
        if sn is not None and (not c.snaps or sn.t - c.snaps[-1].t >= 5):
            self.add_snap(c, sn, now)

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
            else:  # migration: worth a look right away
                self.stats["migrations"] += 1
                self.pending.pop(mint, None)
                c = self.cands.get(mint) or self.add_candidate(mint, now)
                if c is not None:
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
        del self.cands[min(weak, key=lambda c: (c.score or 0, -c.first_seen)).mint]
        return True

    # ---------- refresh (batched, tiered)
    def add_snap(self, c: Candidate, s: Snap, now: float):
        prev = c.snaps[-1] if c.snaps else None
        c.snaps.append(s)
        self.stats["refreshes"] += 1
        if c.first_snap is None:
            c.first_snap = s
        if prev and prev.price > 0:   # big move -> re-check soon (and re-verify RugCheck on liquidity moves)
            if abs(s.price / prev.price - 1) >= 0.08:
                c.hot_until = now + 120
            if prev.liq and s.liq and abs(s.liq / prev.liq - 1) >= 0.20:
                c.hot_until, c.rc_t = now + 120, 0.0
        c.dirty = True

    def refresh_due(self, now):
        cfg = self.cfg
        due = sorted((c for c in self.cands.values() if c.next_check <= now), key=lambda c: (TIER_RANK.get(c.tier, 9), c.next_check))
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

    # ---------- safety lookups (worker threads; only for coins that look worth it)
    def worth_checking(self, c: Candidate) -> bool:
        """Cheap market pre-filter: only these coins get RugCheck / RPC / Jupiter lookups."""
        cfg, f = self.cfg, c.f
        if not f or len(c.snaps) < 2 or c.dead_reason:
            return False
        s = c.snaps[-1]
        if f["dex_stage"] != "bonding" and (s.liq or 0) < cfg.safe_min_liquidity_usd:
            return False
        return ((s.vol_h1 or 0) >= cfg.min_volume_h1_usd and f["age_min"] >= cfg.min_pair_age_min
                and (f["chg_h1"] is None or f["chg_h1"] <= cfg.max_h1_gain_pct) and (c.score or 0) >= cfg.alert_score - 15)

    def schedule_enrichment(self, now):
        cfg = self.cfg
        free = max(cfg.workers, 1) - len(self.inflight)
        if free <= 0:
            return
        want = []
        for c in self.cands.values():
            if c.mint in self.inflight or not c.snaps or not (c.tracked or self.worth_checking(c)):
                continue
            ttl = cfg.rugcheck_ttl_strong if c.tier == "strong" else cfg.rugcheck_ttl_watch
            nr = now - c.rc_t >= ttl
            # RugCheck gives authority + decimals; the RPC is only asked when RugCheck didn't say
            na = (c.authority is None or c.decimals is None) and c.rc_ok_t > 0 and now - c.authority_t >= cfg.authority_recheck_s
            ns = (c.sell_ok is not True and c.sell_fail < 3 and c.authority is True and c.decimals is not None
                  and now - c.sell_t >= 120)
            core = [x for x in c.safety_fails if not x.startswith(("Bubblemaps", "still being watched", "sell route", "not checked"))]
            nb = bool(BUBBLEMAPS_KEY) and c.bm is None and not core and not c.quality_fails and now - c.bm_t >= 600
            if nr or na or ns or nb:
                want.append((c, nr, na, ns, nb))
        want.sort(key=lambda x: -((x[0].score or 0) + (100 if x[0].tracked else 0)))
        for c, nr, na, ns, nb in want[:free]:
            self.inflight[c.mint] = self.submit(do_enrich, c.mint, c.snaps[-1].price, c.decimals, nr, na, ns, nb, cfg)
            c.rc_t = now if nr else c.rc_t
            c.authority_t = now if na else c.authority_t
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
            if "rc" in res:
                rc = res["rc"]
                if rc.get("ok"):
                    c.rc, c.rc_ok_t = rc, now
                    if rc.get("authority_revoked") is False:
                        c.authority = False
                    elif rc.get("authority_revoked") and c.authority is None:
                        c.authority = True
                    if rc.get("decimals") is not None:
                        c.decimals = rc["decimals"]
                    if rc.get("holders") is not None and (not c.holders_hist or now - c.holders_hist[-1][0] >= 60):
                        c.holders_hist = (c.holders_hist + [(now, rc["holders"])])[-60:]
                else:
                    c.rc_t = now - (cfg.rugcheck_ttl_strong if c.tier == "strong" else cfg.rugcheck_ttl_watch) + 30
            if "authority" in res:
                if res["authority"] is not None:
                    c.authority = res["authority"]
                if res.get("decimals") is not None:
                    c.decimals = res["decimals"]
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
        if c.tier == "weak" and now - c.first_seen > 7200 and (c.score or 0) < 35:
            return "stale"
        return ""

    def evaluate(self, c: Candidate, now: float) -> bool:
        cfg = self.cfg
        c.f = f = features(c, now)
        c.score = quality_score(f)
        c.vetoes = hard_vetoes(c, f, cfg, self.bad_deployers)
        reason = self.dead_reason(c, f, now) or ("veto: " + c.vetoes[0] if c.vetoes else "")
        if reason and not c.tracked:
            self.cands.pop(c.mint, None)
            self.seen[c.mint] = now + (86400 if reason.startswith("veto") else 3600)
            self.stats["vetoed" if reason.startswith("veto") else "dropped"] += 1
            return False
        c.dead_reason = reason
        c.safety_fails = safety_gate(c, now, cfg, self.bad_deployers)
        c.quality_fails = quality_filter(f, c.score, cfg)
        if c.tracked:
            call = self.calls.get(c.mint)
            c.tier = "strong" if (call and now - call.t0 < 3600) else "watch"
        elif len(c.snaps) < cfg.min_observations or now - c.first_seen < 90:
            c.tier = "new"
        elif now < c.hot_until or (c.score or 0) >= cfg.alert_score - 10:
            c.tier = "strong"
        else:
            c.tier = "watch" if (c.score or 0) >= 40 else "weak"
        return True

    def process_dirty(self, now):
        for c in list(self.cands.values()):
            if c.dirty and c.snaps:
                c.dirty = False
                if self.evaluate(c, now):
                    self.consider_alert(c, now)

    # ---------- the one alert
    def consider_alert(self, c: Candidate, now: float):
        cfg = self.cfg
        if c.alerted or c.tracked or c.dead_reason or c.vetoes or len(c.snaps) < cfg.min_observations:
            return
        if c.safety_fails or c.quality_fails:
            return
        while self.sent_times and self.sent_times[0] < now - 3600:
            self.sent_times.popleft()
        if len(self.sent_times) >= cfg.max_alerts_per_hour and (c.score or 0) < cfg.alert_score + cfg.standout_bonus:
            return
        c.alerted = True
        self.seen[c.mint] = now + 30 * 86400
        self.sent_times.append(now)
        self.stats["alerts"] += 1
        s = c.snaps[-1]
        self.log("ALERT", c.symbol, c.mint, s.price, f"score {c.score:.0f}")
        self.jlog({"type": "alert", "t": now_iso(), "mint": c.mint, "symbol": c.symbol, "score": round(c.score, 1),
                   "features": c.f, "rugcheck": c.rc, "bubblemaps": c.bm})
        notify(build_alert_embed(c), ca=c.mint, hook=DISCORD_WEBHOOK)
        self.calls[c.mint] = Call(mint=c.mint, symbol=c.symbol, url=c.url, t0=now, price0=s.price, mcap0=s.mcap, liq0=s.liq,
                                  score0=round(c.score, 1), peak=s.price, peak_t=now, peak_mcap=s.mcap, last_price=s.price,
                                  last_t=s.t, last_mcap=s.mcap, last_liq=s.liq, deployer=(c.rc or {}).get("deployer") or "",
                                  pair_addr=c.pair_addr or "")
        c.tracked = True

    # ---------- tracker
    def remember(self, c: Candidate):
        call = self.calls.get(c.mint)
        if call:
            c.alerted, c.tracked = True, call.status == "live"

    def tracker_hook(self):
        return TRACKER_WEBHOOK or DISCORD_WEBHOOK

    def end_call(self, call: Call, c: Optional[Candidate], now: float, status: str, reason: str):
        call.status, call.end_reason, call.end_t = status, reason, now
        if c:
            c.tracked = False
        if status == "dead" and any(w in reason for w in ("no trading pair", "rugged", "liquidity was pulled")) and call.deployer:
            self.bad_deployers.add(call.deployer)
            print(f"[blacklist] deployer {short_addr(call.deployer)} ({call.symbol} rugged)")
        self.log("END", call.symbol, call.mint, call.last_price, f"{status}: {reason} · peak {call.peak_mult():.2f}x")
        self.jlog({"type": "call_end", "t": now_iso(), "mint": call.mint, "symbol": call.symbol, "status": status, "reason": reason,
                   "score0": call.score0, "peak_mult": round(call.peak_mult(), 3), "last_mult": round(call.mult(), 3)})
        if status == "dead":
            notify(build_dead_embed(call), hook=self.tracker_hook())

    def track_calls(self, now):
        cfg = self.cfg
        for mint, call in list(self.calls.items()):
            if call.status != "live":
                continue
            c = self.cands.get(mint)
            if c is None:
                c = Candidate(mint=mint, first_seen=now, symbol=call.symbol, url=call.url, pair_addr=call.pair_addr or None,
                              alerted=True, tracked=True)
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
                    notify(build_milestone_embed(call, m, now), ca=mint, hook=self.tracker_hook())
            died = ("no trading pair any more (possible rug)" if (c.no_pair_since and now - c.no_pair_since >= cfg.dead_no_price_min * 60)
                    else "RugCheck marks it as rugged" if (c.rc or {}).get("rugged")
                    else "liquidity was pulled" if (s and call.liq0 and s.liq is not None and s.liq < 0.25 * call.liq0 and s.liq < 2 * cfg.dead_liquidity_usd)
                    else "down 90%+ from the alert" if (s and s.price <= 0.1 * call.price0) else "")
            if died:
                self.end_call(call, c, now, "dead", died)
            elif now - call.t0 >= cfg.track_hours * 3600:
                self.end_call(call, c, now, "done", "tracking window ended")

    def post_board(self, now):
        if not self.last_board_t:
            self.last_board_t = now
        if self.cfg.board_minutes <= 0 or not self.tracker_hook() or now - self.last_board_t < self.cfg.board_minutes * 60:
            return
        emb = build_board_embed(list(self.calls.values()), now)
        if emb:
            self.last_board_t = now
            notify(emb, hook=self.tracker_hook())

    # ---------- status
    def filter_summary(self) -> str:
        pool = [c for c in self.cands.values() if not c.tracked and self.worth_checking(c)]
        reasons = Counter((c.safety_fails + c.quality_fails)[0].split(" (")[0] for c in pool if c.safety_fails or c.quality_fails)
        top = ", ".join(f"{r} ({n})" for r, n in reasons.most_common(3))
        passing = sum(1 for c in pool if not c.safety_fails and not c.quality_fails)
        return f"{passing}/{len(pool)} checked coins pass" + (f" · top blocks: {top}" if top else "")

    def blockers(self):
        out = [f"{h} rate-limiting (backing off)" for h, lim in LIMITERS.items() if lim.blocked_until > time.time()]
        if self.feed and self.feed.thread is not None and not self.feed.connected:
            out.append("live launch feed reconnecting")
        if not DISCORD_WEBHOOK:
            out.append("DISCORD_WEBHOOK_URL not set (alerts off)")
        return out

    def status(self, now):
        live = sum(c.status == "live" for c in self.calls.values())
        st = call_stats([c for c in self.calls.values() if c.t0 >= now - 86400])
        line = (f"[status] {now_iso()} feed={self.feed.status() if self.feed else 'off'} launches={self.stats['launches']} "
                f"pending={len(self.pending)} watching={len(self.cands)} vetoed={self.stats['vetoed']} | alerts={self.stats['alerts']} "
                f"tracking={live} 24h: {st['n']} alerts {st['x2']} hit 2x {st['dead']} died | {self.filter_summary()} | "
                f"api/min dex={LIMITERS['dex'].per_min()} gecko={LIMITERS['gecko'].per_min()} rugcheck={LIMITERS['rugcheck'].per_min()} "
                f"rpc={LIMITERS['rpc'].per_min()}")
        print(line)
        self.status_lines.append(line)
        try:
            with open("last_status.txt", "w") as fh:
                fh.write(f"{VERSION} | updated {now_iso()} | up {ago(self.clock() - self.started)}\n")
                fh.write(f"alerts={'on' if DISCORD_WEBHOOK else 'OFF'} tracker={'own channel' if TRACKER_WEBHOOK else 'alerts channel'} "
                         f"rpc={'custom' if os.getenv('SOLANA_RPC_URL') else 'public'} bubblemaps={'on' if BUBBLEMAPS_KEY else 'off'}\n")
                fh.write(f"blockers: {'; '.join(self.blockers()) or 'none'}\n\n" + "\n".join(self.status_lines) + "\n")
        except Exception as e:
            print(f"[warn] status file: {e}")

    # ---------- state
    def save_state(self):
        now = self.clock()
        st = {"seen": dict(list(self.seen.items())[-3000:]), "sent_times": list(self.sent_times), "last_board_t": self.last_board_t,
              "calls": [vars(c) for c in sorted(self.calls.values(), key=lambda c: c.t0) if c.t0 >= now - self.cfg.calls_keep_days * 86400][-2000:],
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
            self.last_board_t = s.get("last_board_t", 0.0)
            self.bad_deployers = set(s.get("bad_deployers") or [])
            self.pending = {m: p for m, p in (s.get("pending") or {}).items() if isinstance(p, dict) and now - p.get("t", 0) < 2400}
            for d in s.get("calls", []):
                try:
                    self.calls[d["mint"]] = Call(**{k: v for k, v in d.items() if k in CALL_FIELDS})
                except (TypeError, KeyError):
                    pass
            for mint, call in self.calls.items():
                if call.status == "live":
                    self.cands[mint] = Candidate(mint=mint, first_seen=now, symbol=call.symbol, url=call.url,
                                                 pair_addr=call.pair_addr or None, alerted=True, tracked=True)
        except Exception as e:
            print(f"[warn] could not load state, starting fresh: {e}")

    # ---------- loop
    def tick(self, now):
        self.drain_feed(now)
        self.check_pending(now)
        self.discover(now)
        self.harvest(now)
        self.refresh_due(now)
        self.process_dirty(now)
        self.schedule_enrichment(now)
        self.harvest(now)
        self.track_calls(now)
        self.post_board(now)

    def run(self, max_minutes=None):
        self.init_files()
        self.load_state()
        for sg in (os_signal.SIGINT, os_signal.SIGTERM):
            try:
                os_signal.signal(sg, lambda *_: setattr(self, "stop", True))
            except ValueError:
                pass
        if self.feed:
            self.feed.start()
        print(f"{VERSION} started | alerts {'on' if DISCORD_WEBHOOK else 'OFF'} | tracker {'own channel' if TRACKER_WEBHOOK else 'alerts channel'} "
              f"| bubblemaps {'on' if BUBBLEMAPS_KEY else 'off'}")
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
def sample_coin(now: float, cfg: Config) -> Candidate:
    """A fake coin pushed through the real filter code, for previewing posts."""
    c = Candidate(mint="SampleZAP1111111111111111111111111111111111", first_seen=now - 900, symbol="ZAP", name="Zap Dog",
                  url="https://dexscreener.com/solana", pair_addr="SamplePair111111111111111111111111111111", created_ms=(now - 50 * 60) * 1000,
                  authority=True, decimals=6, sell_ok=True, buyers_h1=210, sellers_h1=150,
                  info={"socials": [{"type": "twitter", "url": "https://x.com/example"}]},
                  rc={"ok": True, "holders": 980, "top10_pct": 19.0, "top1_pct": 4.5, "insider_pct": 3.0, "insider_networks": 0,
                      "lp_locked_pct": 100.0, "rugged": False, "danger_risks": 0, "risk_names": [], "warn_names": [],
                      "deployer": "SampleDev", "deployer_pct": 0.8},
                  holders_hist=[(now - 1500, 700), (now, 980)], rc_ok_t=now)
    for i in range(12):
        k = i / 11
        c.snaps.append(Snap(t=now - 660 + i * 60, price=0.0004 * (1 + 0.06 * k), liq=48_000 * (0.92 + 0.08 * k), mcap=900_000 * (1 + 0.06 * k),
                            vol_m5=9_000, vol_h1=60_000, buys_m5=60, sells_m5=30, buys_h1=520, sells_h1=380, chg_m5=3.0, chg_h1=14.0, dex="raydium"))
    c.first_snap = c.snaps[0]
    c.f = features(c, now)
    c.score = quality_score(c.f)
    c.vetoes = hard_vetoes(c, c.f, cfg)
    c.safety_fails = safety_gate(c, now, cfg)
    c.quality_fails = quality_filter(c.f, c.score, cfg)
    return c


def send_test():
    cfg, now = Config(workers=0), time.time()
    if not (DISCORD_WEBHOOK or TRACKER_WEBHOOK):
        print("No webhook variables set.")
        return
    c = sample_coin(now, cfg)
    print(f"[test] sample coin: score {c.score:.0f}, safety fails {c.safety_fails}, quality fails {c.quality_fails}")
    if DISCORD_WEBHOOK:
        notify(build_alert_embed(c), ca=c.mint, hook=DISCORD_WEBHOOK)
        print("[test] alert sent")
    hook = TRACKER_WEBHOOK or DISCORD_WEBHOOK
    calls = [Call(mint=f"Demo{s}111111111111111111111111111111", symbol=s, url="https://dexscreener.com/solana", t0=now - (i + 2) * 3000,
                  price0=1.0, mcap0=300_000, peak=pk, last_price=last, last_mcap=300_000 * last, status=st,
                  pair_addr="SamplePair111111111111111111111111111111")
             for i, (s, pk, last, st) in enumerate((("SEED", 5.6, 4.8, "live"), ("ZAP", 3.4, 1.9, "live"),
                                                    ("RUN", 2.2, 1.4, "live"), ("DOGGO", 1.3, 0.05, "dead")))]
    notify(build_milestone_embed(calls[0], 5, now), ca=calls[0].mint, hook=hook)
    notify(build_board_embed(calls, now), hook=hook)
    print("[test] tracker: 5x ping + board sent. Done (fake coins).")


def probe(mint: str):
    cfg = Config(workers=0)
    rc = fetch_rugcheck(mint)
    print("RugCheck:", rc)
    pair = (fetch_dex_pairs([mint]) or {}).get(mint)
    print("DexScreener:", snap_from_dex(pair, time.time()) if pair else "unavailable")
    print("Mint/freeze revoked (RugCheck):", rc.get("authority_revoked"), "| (RPC), decimals:", mint_info(mint))
    if BUBBLEMAPS_KEY:
        print("Bubblemaps:", fetch_bubblemaps(mint))
    if pair:
        print("Sell route:", sell_route_check(mint, _num(pair.get("priceUsd")), rc.get("decimals"), cfg))


if __name__ == "__main__":
    args = sys.argv
    if "--test" in args:
        send_test()
    elif "--probe" in args:
        i = args.index("--probe")
        probe(args[i + 1]) if len(args) > i + 1 else print("usage: --probe MINT")
    else:
        Bot(Config()).run(max_minutes=float(args[args.index("--max-minutes") + 1]) if "--max-minutes" in args else None)
