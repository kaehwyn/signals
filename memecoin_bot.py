"""
Solana memecoin scanner for Discord. Two kinds of alerts, and a coin is only posted if it PASSES the rug checks:

  🔎 EARLY WATCH  pump.fun coins still on the bonding curve (before migration). Read live from the PumpPortal feed
                  and straight from the blockchain (bonding curve account, mint account, largest holders).
  🔥 BREAKOUT     coins already trading on a DEX (migrated / new pools) showing breakout behaviour:
                  liquidity vs MC, volume vs MC, more buys than sells, price speeding up. Checked with RugCheck + RPC.

Rug checks (unknown data = fail, so nothing half-checked gets posted):
  EARLY      mint + freeze authority revoked, no risky token-2022 extensions, top holder / top 10 / dev share
             under the limits (bonding curve excluded), early "bundle" wallets hold little, dev isn't a serial
             launcher or blacklisted, buyers look organic (not one wallet buying over and over)
  BREAKOUT   all of the above where it applies + clean RugCheck (no danger flags), LP locked/burned,
             enough holders, insiders under the limit
A dev whose alerted coin dies gets blacklisted. No filter can promise a coin won't rug or will run.

After an alert the coin is tracked: 2x / 3x / 5x / 10x pings and a note if it dies.

Discord webhooks:  DISCORD_WEBHOOK_URL (alerts)   DISCORD_TRACKER_WEBHOOK_URL (optional; tracker posts)
Optional:          SOLANA_RPC_URL (a free Helius/QuickNode/etc. key is much faster than the public RPC)

Run:   pip install requests websocket-client
       python memecoin_bot.py                  run forever
       python memecoin_bot.py --max-minutes N  run N minutes, save, exit (GitHub Actions runs ~5h40m at a time)
       python memecoin_bot.py --test           post sample alerts (fake coins) to the channel(s)
       python memecoin_bot.py --probe MINT     run the rug checks on one token and print the result
"""
import base64
import csv
import json
import os
import queue
import re
import signal as os_signal
import struct
import sys
import threading
import time
import traceback
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Optional

import requests

VERSION = "SCANNER-6.0"
RPC_URL = os.getenv("SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com"
DEXSCREENER = "https://api.dexscreener.com"
GECKO = "https://api.geckoterminal.com/api/v2"
GECKO_HEADERS = {"Accept": "application/json;version=20230302"}
RUGCHECK = "https://api.rugcheck.xyz/v1"
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")
TRACKER_WEBHOOK = os.getenv("DISCORD_TRACKER_WEBHOOK_URL")
PUMPPORTAL_URL = os.getenv("PUMPPORTAL_URL") or "wss://pumpportal.fun/api/data"
PUMPPORTAL_KEY = os.getenv("PUMPPORTAL_API_KEY")

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
STABLES = {SOL_MINT, USDC_MINT, "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
RAYDIUM_AMM_AUTHORITY = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
PUMP_SUPPLY_RAW = 1_000_000_000 * 10 ** 6          # every pump.fun coin: 1B tokens, 6 decimals
PUMP_CURVE_TOKENS_RAW = 793_100_000 * 10 ** 6       # tokens sold on the curve before it completes
RISKY_EXTENSIONS = {"transferHook", "permanentDelegate", "nonTransferable", "defaultAccountState", "confidentialTransferMint"}


@dataclass
class Config:
    # ---------------- EARLY WATCH (pump.fun bonding curve)
    early_min_age_min: float = 8
    early_max_age_min: float = 120
    early_min_mcap: float = 10_000
    early_max_mcap: float = 60_000
    early_min_buyers: int = 30
    early_min_unique_ratio: float = 0.35      # unique buyers / buy txs (low = one wallet churning)
    early_min_buy_sell_5m: float = 1.2        # SOL bought vs sold in the last 5 min
    early_min_growth_5m: float = 0.10         # MC up at least 10% over 5 min
    early_max_drawdown: float = 0.25          # not more than 25% under its peak
    early_max_top1_pct: float = 4.0           # % of total supply (bonding curve excluded)
    early_max_top10_pct: float = 22.0
    early_max_dev_pct: float = 4.0
    early_max_bundle_pct: float = 8.0         # wallets that bought in the first seconds, still held
    bundle_window_s: float = 3.0
    max_creator_launches_24h: int = 3
    early_drop_after_min: float = 5           # after this, drop launches with < 8 buyers or MC < $5K
    max_trade_subs: int = 400
    # ---------------- BREAKOUT (DEX pairs)
    br_min_age_min: float = 10
    br_max_age_h: float = 24
    br_min_mcap: float = 30_000
    br_max_mcap: float = 3_000_000
    br_min_liq: float = 15_000
    br_min_liq_mcap: float = 0.08
    br_min_vol_h1: float = 20_000
    br_min_vol_mcap: float = 0.40
    br_min_buy_sell_h1: float = 1.3
    br_min_trades_h1: int = 150
    br_min_chg_m5: float = 3
    br_min_chg_h1: float = 10
    br_max_chg_h1: float = 200
    br_max_drawdown: float = 0.20
    br_min_holders: int = 200
    br_max_top1_pct: float = 10
    br_max_top10_pct: float = 30
    br_max_insider_pct: float = 10
    br_max_dev_pct: float = 5
    br_min_lp_locked: float = 90
    max_pairs: int = 800
    pair_refresh_s: float = 30
    # ---------------- shared
    recheck_s: float = 60                     # min time between deep checks of the same coin
    rugcheck_ttl_s: float = 180
    max_alerts_per_hour: int = 8
    quiet_post_h: float = 2                   # "NO QUALITY PLAYS RIGHT NOW" after this long with no alert
    # ---------------- tracker
    track_hours: float = 24
    milestones: tuple = (2, 3, 5, 10, 20, 50, 100)
    dead_ratio: float = 0.40                  # price <= 40% of the alert price = dead
    calls_keep_days: float = 7
    # ---------------- files
    log_file: str = "radar_log.csv"
    signals_file: str = "signals.jsonl"
    state_file: str = "state.json"
    status_file: str = "last_status.txt"


# ======================================================================= HTTP (rate limited per host)
class Limiter:
    def __init__(self, min_interval: float):
        self.min_interval, self.lock = min_interval, threading.Lock()
        self.next_ok = self.blocked_until = 0.0
        self.calls = deque()

    def acquire(self, max_wait: float) -> bool:
        with self.lock:
            now = time.time()
            if now < self.blocked_until:
                return False
            start = max(now, self.next_ok)
            if start - now > max_wait:
                return False
            self.next_ok = start + self.min_interval
            self.calls.append(start)
        if start > now:
            time.sleep(start - now)
        return True

    def penalize(self, seconds: float):
        self.blocked_until = max(self.blocked_until, time.time() + seconds)

    def per_min(self) -> int:
        cut = time.time() - 60
        while self.calls and self.calls[0] < cut:
            self.calls.popleft()
        return len(self.calls)


LIMITERS = {"dex": Limiter(0.3), "dexfeed": Limiter(1.2), "gecko": Limiter(3.0), "rugcheck": Limiter(1.0),
            "rpc": Limiter(0.12 if os.getenv("SOLANA_RPC_URL") else 0.35)}


def http_json(host, url, params=None, method="GET", body=None, headers=None, max_wait=0.5, timeout=10):
    """(json or None, status). status 0 = limiter said wait, -1 = network error."""
    lim = LIMITERS[host]
    if not lim.acquire(max_wait):
        return None, 0
    try:
        r = requests.request(method, url, params=params, json=body, headers=headers, timeout=timeout)
    except Exception as e:
        print(f"[warn] {host} request failed: {str(e)[:120]}")
        return None, -1
    if r.status_code == 429:
        try:
            ra = float(r.headers.get("Retry-After", 20))
        except ValueError:
            ra = 20.0
        lim.penalize(max(5.0, min(ra, 120.0)))
        return None, 429
    try:
        return r.json(), r.status_code
    except ValueError:
        return None, r.status_code


# ======================================================================= helpers
def num(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def usd(n):
    n = num(n)
    if n is None:
        return "n/a"
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= div:
            return f"${n / div:.1f}{suf}" if n / div < 100 else f"${n / div:.0f}{suf}"
    return f"${n:,.0f}"


def sign_pct(n):
    n = num(n)
    return "n/a" if n is None else (f"{n:+.0f}%" if abs(n) >= 10 else f"{n:+.1f}%")


def ago(seconds: float) -> str:
    m = max(0, int(seconds // 60))
    return f"{m}m" if m < 60 else (f"{m // 60}h {m % 60}m" if m < 1440 else f"{m // 1440}d")


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def parse_iso_ms(s):
    try:
        return datetime.fromisoformat(str(s).replace("Z", "+00:00")).timestamp() * 1000 if s else None
    except ValueError:
        return None


def check(ok: Optional[bool], label: str) -> tuple:
    """(passed, line). None (could not verify) counts as a fail."""
    return (ok is True, f"{'✅' if ok else ('❔' if ok is None else '❌')} {label}")


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


def notify(embed: dict, hook=None) -> bool:
    url = hook or DISCORD_WEBHOOK
    if not url:
        print(f"[discord off] {embed.get('title')}")
        return False
    return _post(url, {"embeds": [embed], "allowed_mentions": {"parse": []}})


# ======================================================================= on-chain (RPC)
def rpc(method, params, max_wait=20):
    data, _ = http_json("rpc", RPC_URL, method="POST", max_wait=max_wait, timeout=15,
                        body={"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
    return data.get("result") if isinstance(data, dict) and "result" in data else None


def mint_info(mint) -> Optional[dict]:
    """Authorities, supply, decimals and risky token-2022 extensions from the mint account."""
    res = rpc("getAccountInfo", [mint, {"encoding": "jsonParsed"}])
    try:
        info = res["value"]["data"]["parsed"]["info"]
    except Exception:
        return None
    bad = []
    for ext in info.get("extensions") or []:
        name = ext.get("extension") if isinstance(ext, dict) else None
        if name in RISKY_EXTENSIONS:
            bad.append(name)
        elif name == "transferFeeConfig":
            st = ext.get("state") or {}
            fee = max(num((st.get("newerTransferFee") or {}).get("transferFeeBasisPoints")) or 0,
                      num((st.get("olderTransferFee") or {}).get("transferFeeBasisPoints")) or 0)
            if fee > 0:
                bad.append(f"transfer fee {fee / 100:.1f}%")
    return {"revoked": info.get("mintAuthority") is None and info.get("freezeAuthority") is None,
            "supply": int(info.get("supply") or 0), "decimals": info.get("decimals"), "bad_ext": bad}


def decode_curve(raw: bytes) -> Optional[dict]:
    """pump.fun bonding curve account: 8-byte discriminator, 5 x u64, complete flag."""
    if len(raw) < 49:
        return None
    v_tok, v_sol, r_tok, r_sol, supply = struct.unpack_from("<QQQQQ", raw, 8)
    complete = bool(raw[48])
    progress = max(0.0, min(1.0, 1 - r_tok / PUMP_CURVE_TOKENS_RAW)) if r_tok <= PUMP_CURVE_TOKENS_RAW else 0.0
    price_sol = (v_sol / 1e9) / (v_tok / 1e6) if v_tok else None
    return {"reserve_sol": r_sol / 1e9, "progress": 1.0 if complete else progress, "complete": complete,
            "mcap_sol": price_sol * (supply / 1e6) if price_sol else None, "supply_raw": supply}


def curve_state(curve_key) -> Optional[dict]:
    res = rpc("getAccountInfo", [curve_key, {"encoding": "base64"}])
    try:
        return decode_curve(base64.b64decode(res["value"]["data"][0]))
    except Exception:
        return None


def largest_holders(mint, exclude_owners=()) -> Optional[list]:
    """[(owner, raw amount)] for the 20 largest token accounts, merged per owner, pools/curves excluded. None = failed."""
    res = rpc("getTokenLargestAccounts", [mint])
    try:
        accts = [(a["address"], int(a["amount"])) for a in res["value"]]
    except Exception:
        return None
    if not accts:
        return []
    res2 = rpc("getMultipleAccounts", [[a for a, _ in accts], {"encoding": "jsonParsed"}])
    try:
        owners = [((v or {}).get("data") or {}).get("parsed", {}).get("info", {}).get("owner") for v in res2["value"]]
    except Exception:
        return None
    skip = set(x for x in exclude_owners if x) | {RAYDIUM_AMM_AUTHORITY}
    merged = Counter()
    for (addr, amt), owner in zip(accts, owners):
        if amt <= 0 or addr in skip or (owner or addr) in skip:
            continue
        merged[owner or addr] += amt
    return merged.most_common()


def holder_stats(holders: Optional[list], supply_raw: int, dev: Optional[str] = None) -> dict:
    if holders is None or not supply_raw:
        return {"top1": None, "top10": None, "dev": None}
    pcts = [a / supply_raw * 100 for _, a in holders]
    dev_pct = next((a / supply_raw * 100 for o, a in holders if dev and o == dev), 0.0) if dev else None
    return {"top1": pcts[0] if pcts else 0.0, "top10": sum(pcts[:10]), "dev": dev_pct}


# ======================================================================= DexScreener / Gecko / RugCheck
def fetch_dex_pairs(mints: list):
    """Up to 30 tokens per call -> {mint: deepest pair where the token is the base}; None if unavailable."""
    if not mints:
        return {}
    data, st = http_json("dex", f"{DEXSCREENER}/tokens/v1/solana/{','.join(mints)}", max_wait=2.0)
    if st != 200 or data is None:
        return None
    pairs = data if isinstance(data, list) else (data.get("pairs") or [])
    want, best = set(mints), {}
    for p in pairs:
        if not isinstance(p, dict) or p.get("chainId") != "solana":
            continue
        m = (p.get("baseToken") or {}).get("address")
        if m in want:
            liq = num((p.get("liquidity") or {}).get("usd")) or 0
            if m not in best or liq > (num((best[m].get("liquidity") or {}).get("usd")) or 0):
                best[m] = p
    return best


def fetch_sol_usd() -> Optional[float]:
    data, st = http_json("dex", f"{DEXSCREENER}/tokens/v1/solana/{SOL_MINT}", max_wait=2.0)
    if st != 200 or not isinstance(data, list):
        return None
    best = None
    for p in data:
        if (p.get("baseToken") or {}).get("address") == SOL_MINT and (p.get("quoteToken") or {}).get("address") in STABLES:
            liq = num((p.get("liquidity") or {}).get("usd")) or 0
            if best is None or liq > best[0]:
                best = (liq, num(p.get("priceUsd")))
    return best[1] if best else None


def fetch_dex_feed(path: str) -> list:
    data, st = http_json("dexfeed", f"{DEXSCREENER}{path}")
    if st != 200 or not isinstance(data, list):
        return []
    return [d["tokenAddress"] for d in data if isinstance(d, dict) and d.get("chainId") == "solana" and d.get("tokenAddress")]


def fetch_gecko_mints(kind: str) -> list:
    data, st = http_json("gecko", f"{GECKO}/networks/solana/{kind}", headers=GECKO_HEADERS)
    if st != 200 or not isinstance(data, dict):
        return []
    out = []
    for pool in data.get("data") or []:
        rel = pool.get("relationships") or {}
        base = (((rel.get("base_token") or {}).get("data") or {}).get("id") or "").split("_", 1)[-1]
        quote = (((rel.get("quote_token") or {}).get("data") or {}).get("id") or "").split("_", 1)[-1]
        mint = base if base and base not in STABLES else quote
        if mint and mint not in STABLES:
            out.append(mint)
    return out


def parse_rugcheck(rep) -> Optional[dict]:
    if not isinstance(rep, dict):
        return None
    known = rep.get("knownAccounts") or {}

    def is_pool(h):
        for k in (h.get("address"), h.get("owner")):
            ka = known.get(k) if k else None
            if isinstance(ka, dict):
                typ, name = str(ka.get("type", "")).upper(), str(ka.get("name", "")).lower()
                if typ in ("AMM", "LP", "POOL", "BONDING", "CURVE") or any(w in name for w in ("bonding curve", "amm", "pool", "liquidity")):
                    return True
        return False

    holders = [h for h in (rep.get("topHolders") or []) if isinstance(h, dict) and not is_pool(h)]
    pcts = sorted((p for p in (num(h.get("pct")) for h in holders) if p is not None), reverse=True)
    risks = [r for r in (rep.get("risks") or []) if isinstance(r, dict)]
    lp = [v for v in (num(((m or {}).get("lp") or {}).get("lpLockedPct")) for m in (rep.get("markets") or [])) if v is not None]
    tok = rep.get("token") if isinstance(rep.get("token"), dict) else {}
    creator = rep.get("creator") if isinstance(rep.get("creator"), str) else None
    revoked = None
    for src in (rep, tok):
        if "mintAuthority" in src and "freezeAuthority" in src:
            revoked = not src.get("mintAuthority") and not src.get("freezeAuthority")
            break
    th = num(rep.get("totalHolders"))
    return {
        "holders": int(th) if th is not None else None,
        "top1": pcts[0] if pcts else None,
        "top10": sum(pcts[:10]) if pcts else None,
        "insiders": sum((num(h.get("pct")) or 0) for h in holders if h.get("insider")) if any("insider" in h for h in holders) else None,
        "lp_locked": max(lp) if lp else None,
        "rugged": bool(rep.get("rugged")),
        "dangers": [str(r.get("name")) for r in risks if str(r.get("level", "")).lower() == "danger"],
        "creator": creator,
        "dev": next((num(h.get("pct")) for h in holders if creator and creator in (h.get("address"), h.get("owner"))), 0.0) if creator else None,
        "revoked": revoked,
    }


def fetch_rugcheck(mint) -> Optional[dict]:
    data, st = http_json("rugcheck", f"{RUGCHECK}/tokens/{mint}/report", max_wait=30, timeout=20)
    return parse_rugcheck(data) if st == 200 else None


# ======================================================================= live pump.fun feed
def classify_feed_event(msg) -> Optional[tuple]:
    """('launch' | 'trade' | 'migration', mint, msg) from a PumpPortal message."""
    if not isinstance(msg, dict):
        return None
    mint = msg.get("mint")
    if not isinstance(mint, str) or len(mint) < 32 or mint in STABLES:
        return None
    tx = str(msg.get("txType") or "").lower()
    if tx in ("buy", "sell"):
        return ("trade", mint, msg)
    if "migrat" in tx:
        return ("migration", mint, msg)
    if tx == "create":
        return ("launch", mint, msg)
    return None


class PumpFeed:
    """ONE websocket to PumpPortal (their rule): new tokens, migrations, and trades for the coins we watch.
    Background thread; reconnects with backoff and re-subscribes."""

    def __init__(self, url: str = PUMPPORTAL_URL):
        self.url = url + (f"?api-key={PUMPPORTAL_KEY}" if PUMPPORTAL_KEY else "")
        self.events = queue.Queue(maxsize=100_000)
        self.cmds = queue.Queue()
        self.subs = set()                       # owned by the feed thread
        self.stop, self.connected, self.err, self.thread = False, False, "", None

    def start(self) -> bool:
        try:
            import websocket  # noqa: F401
        except ImportError:
            self.err = "websocket-client not installed"
            print(f"[feed] {self.err}")
            return False
        self.thread = threading.Thread(target=self._run, name="pump-feed", daemon=True)
        self.thread.start()
        return True

    def watch(self, mints):
        if mints:
            self.cmds.put(("sub", list(mints)))

    def unwatch(self, mints):
        if mints:
            self.cmds.put(("unsub", list(mints)))

    @staticmethod
    def _send_keys(ws, method, keys):
        keys = list(keys)
        for i in range(0, len(keys), 100):
            ws.send(json.dumps({"method": method, "keys": keys[i:i + 100]}))

    def _run(self):
        import websocket
        backoff = 5
        while not self.stop:
            ws = None
            try:
                ws = websocket.create_connection(self.url, timeout=10)
                ws.send(json.dumps({"method": "subscribeNewToken"}))
                ws.send(json.dumps({"method": "subscribeMigration"}))
                if self.subs:
                    self._send_keys(ws, "subscribeTokenTrade", self.subs)
                ws.settimeout(1)
                self.connected, self.err, backoff = True, "", 5
                print("[feed] connected")
                last_ping = time.time()
                while not self.stop:
                    while True:
                        try:
                            op, keys = self.cmds.get_nowait()
                        except queue.Empty:
                            break
                        if op == "sub":
                            new = [k for k in keys if k not in self.subs]
                            self.subs.update(new)
                            if new:
                                self._send_keys(ws, "subscribeTokenTrade", new)
                        else:
                            old = [k for k in keys if k in self.subs]
                            self.subs.difference_update(old)
                            if old:
                                self._send_keys(ws, "unsubscribeTokenTrade", old)
                    if time.time() - last_ping > 20:
                        ws.ping()
                        last_ping = time.time()
                    try:
                        raw = ws.recv()
                    except websocket.WebSocketTimeoutException:
                        continue
                    if not raw:
                        break
                    try:
                        ev = classify_feed_event(json.loads(raw))
                    except ValueError:
                        continue
                    if ev:
                        try:
                            self.events.put_nowait((time.time(),) + ev)
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
                backoff = min(backoff * 2, 120)

    def drain(self, limit=20_000) -> list:
        out = []
        while len(out) < limit:
            try:
                out.append(self.events.get_nowait())
            except queue.Empty:
                break
        return out

    def status(self) -> str:
        if self.thread is None:
            return "off" + (f" ({self.err})" if self.err else "")
        return "connected" if self.connected else "reconnecting" + (f" ({self.err})" if self.err else "")


# ======================================================================= state objects
@dataclass
class Launch:
    """A pump.fun coin on the bonding curve, built from live feed events."""
    mint: str
    name: str
    symbol: str
    creator: str
    curve: str
    t0: float
    mcap_sol: float = 0.0
    peak_sol: float = 0.0
    n_buys: int = 0
    n_sells: int = 0
    buyers: set = field(default_factory=set)
    sellers: set = field(default_factory=set)
    balances: dict = field(default_factory=dict)     # wallet -> latest token balance (raw, 6 dp -> tokens)
    early: dict = field(default_factory=dict)        # wallet -> tokens bought in the first seconds
    flow: deque = field(default_factory=lambda: deque(maxlen=4000))   # (t, side, sol)
    hist: deque = field(default_factory=lambda: deque(maxlen=400))    # (t, mcap_sol)
    dev_sold: bool = False
    last_trade: float = 0.0
    last_eval: float = 0.0
    last_deep: float = 0.0
    blocked: str = ""

    def on_trade(self, t: float, m: dict):
        side, who = str(m.get("txType")).lower(), m.get("traderPublicKey")
        sol, tokens = num(m.get("solAmount")) or 0.0, num(m.get("tokenAmount")) or 0.0
        mc = num(m.get("marketCapSol"))
        if mc:
            self.mcap_sol, self.peak_sol = mc, max(self.peak_sol, mc)
            if not self.hist or t - self.hist[-1][0] >= 5:
                self.hist.append((t, mc))
        bal = num(m.get("newTokenBalance"))
        if who and bal is not None:
            self.balances[who] = bal
        if side == "buy":
            self.n_buys += 1
            if who and who != self.creator:
                self.buyers.add(who)
            if who and t - self.t0 <= Config.bundle_window_s:
                self.early[who] = self.early.get(who, 0.0) + tokens
        else:
            self.n_sells += 1
            if who:
                self.sellers.add(who)
            if who == self.creator:
                self.dev_sold = True
        self.flow.append((t, side, sol))
        self.last_trade = t

    def mcap_at(self, t: float) -> Optional[float]:
        best = None
        for ts, mc in self.hist:
            if ts <= t:
                best = mc
            else:
                break
        return best

    def flow_5m(self, now: float) -> tuple:
        b = s = 0.0
        for t, side, sol in reversed(self.flow):
            if t < now - 300:
                break
            if side == "buy":
                b += sol
            else:
                s += sol
        return b, s

    def bundle_tokens(self) -> float:
        return sum(self.balances.get(w, amt) for w, amt in self.early.items())


@dataclass
class Pair:
    mint: str
    symbol: str = "?"
    name: str = ""
    pair: str = ""
    dex: str = ""
    url: str = ""
    created_ms: Optional[float] = None
    snap: dict = field(default_factory=dict)
    peak_price: float = 0.0
    misses: int = 0
    added: float = 0.0
    next_refresh: float = 0.0
    last_eval: float = 0.0
    rc: Optional[dict] = None
    rc_t: float = 0.0
    blocked: str = ""


@dataclass
class Call:
    mint: str
    symbol: str
    kind: str
    t0: float
    price0: float
    creator: str = ""
    peak: float = 0.0
    last_price: float = 0.0
    hit: list = field(default_factory=list)
    dead: bool = False
    closed: bool = False
    last_seen: float = 0.0


def pair_snap(p: dict, t: float) -> dict:
    tx, vol, chg = p.get("txns") or {}, p.get("volume") or {}, p.get("priceChange") or {}
    h1 = tx.get("h1") or {}
    return {"t": t, "price": num(p.get("priceUsd")), "mcap": num(p.get("marketCap") or p.get("fdv")),
            "liq": num((p.get("liquidity") or {}).get("usd")), "vol_h1": num(vol.get("h1")),
            "buys_h1": h1.get("buys") or 0, "sells_h1": h1.get("sells") or 0,
            "chg_m5": num(chg.get("m5")), "chg_h1": num(chg.get("h1"))}


# ======================================================================= checks (pure functions, easy to test)
def early_gates(l: Launch, now: float, sol_usd: float, cfg: Config, creator_launches: int, blacklisted: bool) -> list:
    """Cheap filters from the live feed. [] = go do the on-chain checks."""
    mc = l.mcap_sol * sol_usd
    age = (now - l.t0) / 60
    if age < cfg.early_min_age_min:
        return [f"only {age:.0f}m old"]
    if blacklisted:
        return ["dev blacklisted"]
    if creator_launches > cfg.max_creator_launches_24h:
        return [f"dev launched {creator_launches} coins in 24h"]
    fails = []
    if not cfg.early_min_mcap <= mc <= cfg.early_max_mcap:
        fails.append(f"MC {usd(mc)} out of range")
    if len(l.buyers) < cfg.early_min_buyers:
        fails.append(f"only {len(l.buyers)} buyers")
    if l.n_buys and len(l.buyers) / l.n_buys < cfg.early_min_unique_ratio:
        fails.append("same wallets buying over and over")
    b, s = l.flow_5m(now)
    if b < s * cfg.early_min_buy_sell_5m or b == 0:
        fails.append("more selling than buying (5m)")
    past = l.mcap_at(now - 300)
    if not past or l.mcap_sol / past - 1 < cfg.early_min_growth_5m:
        fails.append("not climbing (5m)")
    if l.peak_sol and l.mcap_sol < l.peak_sol * (1 - cfg.early_max_drawdown):
        fails.append("dumping from its peak")
    return fails


def early_safety(l: Launch, chain: dict, cfg: Config) -> tuple:
    """(all passed, check lines) from the on-chain results."""
    mi, hs = chain.get("mint") or {}, chain.get("holders") or {}
    supply = mi.get("supply") or PUMP_SUPPLY_RAW
    unit = 10 ** int(mi.get("decimals") if mi.get("decimals") is not None else 6)
    dev_pct = hs.get("dev")
    if l.creator in l.balances:                              # dev's balance from the live feed; take the larger
        feed_pct = l.balances[l.creator] * unit / supply * 100
        dev_pct = feed_pct if dev_pct is None else max(dev_pct, feed_pct)
    bundle_pct = l.bundle_tokens() * unit / supply * 100
    t1, t10 = hs.get("top1"), hs.get("top10")
    rows = [
        check(mi.get("revoked") if mi else None, "Mint & freeze revoked"),
        check((not mi.get("bad_ext")) if mi else None, "No risky token settings" + (f" ({', '.join(mi['bad_ext'])})" if mi and mi.get("bad_ext") else "")),
        check(None if t1 is None else t1 <= cfg.early_max_top1_pct, f"Top holder {t1:.1f}%" if t1 is not None else "Top holder n/a"),
        check(None if t10 is None else t10 <= cfg.early_max_top10_pct, f"Top 10 {t10:.1f}%" if t10 is not None else "Top 10 n/a"),
        check(None if dev_pct is None else dev_pct <= cfg.early_max_dev_pct,
              f"Dev holds {dev_pct:.1f}%" + (" (sold some)" if l.dev_sold else "") if dev_pct is not None else "Dev holdings n/a"),
        check(bundle_pct <= cfg.early_max_bundle_pct, f"Early/bundle wallets {bundle_pct:.1f}%"),
        check(True, f"Organic buyers ({len(l.buyers)} wallets)"),
    ]
    return all(p for p, _ in rows), [line for _, line in rows]


def breakout_gates(p: Pair, now: float, cfg: Config) -> list:
    s = p.snap
    if not s or not s.get("price"):
        return ["no market data"]
    age_min = (now * 1000 - p.created_ms) / 60000 if p.created_ms else None
    mc, liq, vol = s.get("mcap") or 0, s.get("liq") or 0, s.get("vol_h1") or 0
    buys, sells = s.get("buys_h1") or 0, s.get("sells_h1") or 0
    m5, h1 = s.get("chg_m5"), s.get("chg_h1")
    fails = []
    if age_min is None or age_min < cfg.br_min_age_min or age_min > cfg.br_max_age_h * 60:
        fails.append("age out of window")
    if not cfg.br_min_mcap <= mc <= cfg.br_max_mcap:
        fails.append(f"MC {usd(mc)} out of range")
    if liq < cfg.br_min_liq or (mc and liq / mc < cfg.br_min_liq_mcap):
        fails.append("thin liquidity")
    if vol < cfg.br_min_vol_h1 or (mc and vol / mc < cfg.br_min_vol_mcap):
        fails.append("low volume")
    if buys + sells < cfg.br_min_trades_h1:
        fails.append("too few trades")
    if buys < sells * cfg.br_min_buy_sell_h1:
        fails.append("not enough buy pressure")
    if m5 is None or h1 is None or m5 < cfg.br_min_chg_m5 or not cfg.br_min_chg_h1 <= h1 <= cfg.br_max_chg_h1:
        fails.append("no breakout move")
    elif m5 * 12 < h1:
        fails.append("not accelerating")
    if p.peak_price and s["price"] < p.peak_price * (1 - cfg.br_max_drawdown):
        fails.append("dumping from its peak")
    return fails


def pump_migrated(p: Pair) -> bool:
    return p.mint.endswith("pump") and "pumpswap" in (p.dex or "").lower()


def breakout_safety(p: Pair, chain: dict, cfg: Config, blacklisted: bool) -> tuple:
    rc, mi = chain.get("rc"), chain.get("mint")
    dangers = [d for d in (rc or {}).get("dangers", []) if not (pump_migrated(p) and "lp" in d.lower() and "unlock" in d.lower())]
    lp = (rc or {}).get("lp_locked")
    lp_ok = True if pump_migrated(p) else (None if lp is None else lp >= cfg.br_min_lp_locked)
    revoked = mi.get("revoked") if mi else (rc or {}).get("revoked")
    hold = (rc or {}).get("holders")
    t1, t10 = (rc or {}).get("top1"), (rc or {}).get("top10")
    hs = chain.get("holders") or {}
    t1 = t1 if t1 is not None else hs.get("top1")
    t10 = t10 if t10 is not None else hs.get("top10")
    ins, dev = (rc or {}).get("insiders"), (rc or {}).get("dev")
    rows = [
        check(None if rc is None else (not rc["rugged"] and not dangers), "RugCheck clean" if rc and not dangers else
              ("RugCheck: " + ", ".join(dangers[:2]) if dangers else "RugCheck n/a")),
        check(revoked, "Mint & freeze revoked"),
        check((not mi.get("bad_ext")) if mi else None, "No risky token settings"),
        check(lp_ok, "LP burned (pump.fun migration)" if pump_migrated(p) else (f"LP locked {lp:.0f}%" if lp is not None else "LP lock n/a")),
        check(None if hold is None else hold >= cfg.br_min_holders, f"{hold:,} holders" if hold is not None else "Holders n/a"),
        check(None if t1 is None else t1 <= cfg.br_max_top1_pct, f"Top holder {t1:.1f}%" if t1 is not None else "Top holder n/a"),
        check(None if t10 is None else t10 <= cfg.br_max_top10_pct, f"Top 10 {t10:.1f}%" if t10 is not None else "Top 10 n/a"),
        check(True if ins is None else ins <= cfg.br_max_insider_pct, f"Insiders {ins:.1f}%" if ins is not None else "Insiders none flagged"),
        check(True if dev is None else dev <= cfg.br_max_dev_pct, f"Dev holds {dev:.1f}%" if dev is not None else "Dev wallet not in top holders"),
        check(not blacklisted, "Dev not blacklisted"),
    ]
    return all(ok for ok, _ in rows), [line for _, line in rows]


def deep_early(l_mint: str, curve: str, creator: str) -> dict:
    mi = mint_info(l_mint)
    supply = (mi or {}).get("supply") or PUMP_SUPPLY_RAW
    return {"mint": mi, "curve": curve_state(curve),
            "holders": holder_stats(largest_holders(l_mint, exclude_owners=[curve]), supply, creator)}


def deep_breakout(mint: str, pair_addr: str, cached_rc: Optional[dict] = None) -> dict:
    out = {"mint": mint_info(mint), "rc": cached_rc or fetch_rugcheck(mint), "rc_fresh": cached_rc is None}
    if not out["rc"] or out["rc"].get("top10") is None:
        supply = (out["mint"] or {}).get("supply")
        if supply:
            out["holders"] = holder_stats(largest_holders(mint, exclude_owners=[pair_addr]), supply)
    return out


# ======================================================================= embeds
FOOTER = {"text": f"{VERSION} · rug checks passed ≠ guaranteed safe · not financial advice"}


def links(mint: str, axiom_key: str, pump: bool) -> str:
    out = []
    if pump:
        out.append(f"[pump.fun](https://pump.fun/coin/{mint})")
    out += [f"[DEX](https://dexscreener.com/solana/{mint})"]
    if axiom_key:
        out.append(f"[Axiom](https://axiom.trade/meme/{axiom_key})")
    out += [f"[RugCheck](https://rugcheck.xyz/tokens/{mint})", f"[Solscan](https://solscan.io/token/{mint})",
            f"[X search](https://x.com/search?q={mint})"]
    return " · ".join(out)


def early_embed(l: Launch, chain: dict, rows: list, now: float, sol_usd: float) -> dict:
    cs = chain.get("curve") or {}
    mc = l.mcap_sol * sol_usd
    b, s = l.flow_5m(now)
    past = l.mcap_at(now - 300)
    growth = (l.mcap_sol / past - 1) * 100 if past else None
    market = f"MC **{usd(mc)}**"
    if cs:
        market += f" · Curve **{cs['progress'] * 100:.0f}%** · Reserve **{cs['reserve_sol']:.1f} SOL**"
    flow = (f"Age **{ago(now - l.t0)}** · Buyers **{len(l.buyers)}** · Buys/Sells **{l.n_buys}/{l.n_sells}**\n"
            f"MC **{sign_pct(growth)}** in 5m · 5m flow **{b:.1f}** SOL in / **{s:.1f}** out")
    return {"title": f"🔎 EARLY WATCH — {l.symbol}"[:256], "color": 0x3BA55D,
            "description": f"**{l.name}** · pump.fun, before migration\n✅ **Passed every rug check**",
            "fields": [{"name": "Market", "value": market, "inline": False},
                       {"name": "Flow", "value": flow, "inline": False},
                       {"name": "Rug checks", "value": "\n".join(rows), "inline": False},
                       {"name": "Links", "value": links(l.mint, l.curve, True), "inline": False},
                       {"name": "CA", "value": f"```{l.mint}```", "inline": False}],
            "footer": FOOTER}


def breakout_embed(p: Pair, rows: list, now: float) -> dict:
    s = p.snap
    mc, liq, vol = s.get("mcap") or 0, s.get("liq") or 0, s.get("vol_h1") or 0
    age = now - p.created_ms / 1000 if p.created_ms else None
    why = [f"liquidity {liq / mc * 100:.0f}% of MC" if mc else None, f"1h volume {vol / mc:.2f}x MC" if mc else None,
           f"buys/sells {s['buys_h1']}/{s['sells_h1']} in 1h", f"price {sign_pct(s.get('chg_m5'))} in 5m, {sign_pct(s.get('chg_h1'))} in 1h"]
    return {"title": f"🔥 BREAKOUT — {p.symbol}"[:256], "color": 0xF0A020,
            "description": f"**{p.name or p.symbol}** · {p.dex or 'dex'}\n✅ **Passed every rug check**",
            "fields": [{"name": "Market", "value": f"MC **{usd(mc)}** · Liq **{usd(liq)}** · Vol 1h **{usd(vol)}**", "inline": False},
                       {"name": "Flow", "value": f"Age **{ago(age) if age is not None else 'n/a'}** · Trades 1h **{s['buys_h1'] + s['sells_h1']}**", "inline": False},
                       {"name": "Why it fired", "value": "\n".join(f"• {w}" for w in why if w), "inline": False},
                       {"name": "Rug checks", "value": "\n".join(rows), "inline": False},
                       {"name": "Links", "value": links(p.mint, p.pair, p.mint.endswith("pump")), "inline": False},
                       {"name": "CA", "value": f"```{p.mint}```", "inline": False}],
            "footer": FOOTER}


def milestone_embed(c: Call, x: int, now: float) -> dict:
    return {"title": f"🚀 {c.symbol} hit {x}x", "color": 0x57F287,
            "description": f"{'Early watch' if c.kind == 'early' else 'Breakout'} called {ago(now - c.t0)} ago · peak **{c.peak / c.price0:.1f}x**\n```{c.mint}```"}


def dead_embed(c: Call) -> dict:
    return {"title": f"☠️ {c.symbol} died", "color": 0xED4245,
            "description": f"Down to {c.last_price / c.price0:.2f}x of the alert price (peak {c.peak / c.price0:.1f}x). Dev blacklisted."}


# ======================================================================= bot
class Bot:
    def __init__(self, cfg: Config, feed: Optional[PumpFeed] = None, clock=time.time):
        self.cfg, self.clock = cfg, clock
        self.feed = feed
        self.launches: dict = {}
        self.pairs: dict = {}
        self.calls: dict = {}
        self.creator_times = defaultdict(deque)
        self.blacklist: set = set()
        self.seen: dict = {}                 # mint -> skip pair discovery until
        self.pending: dict = {}              # mint -> (kind, future)
        self.pool = ThreadPoolExecutor(max_workers=4)
        self.alert_times = deque()
        self.last_alert = self.last_quiet = clock()
        self.sol_usd, self.sol_t = None, 0.0
        self.blocks = Counter()
        self.quiet_blocks = Counter()
        self.counts = Counter()
        self.timers = defaultdict(float)
        self.started = clock()
        self.status_lines = deque(maxlen=60)
        self.load_state()

    # ---------------- persistence
    def load_state(self):
        try:
            with open(self.cfg.state_file) as f:
                st = json.load(f)
        except Exception:
            return
        if not isinstance(st, dict) or st.get("version", "").split("-")[0] != VERSION.split("-")[0]:
            return                                          # old bot's state: start clean
        self.blacklist = set(st.get("blacklist") or [])
        for d in st.get("calls") or []:
            try:
                c = Call(**d)
                self.calls[c.mint] = c
            except TypeError:
                pass
        self.last_alert = st.get("last_alert") or self.last_alert
        self.last_quiet = st.get("last_quiet") or self.last_quiet

    def save_state(self):
        keep = self.clock() - self.cfg.calls_keep_days * 86400
        st = {"version": VERSION, "saved": now_iso(), "blacklist": sorted(self.blacklist)[-5000:],
              "calls": [asdict(c) for c in self.calls.values() if c.t0 >= keep],
              "last_alert": self.last_alert, "last_quiet": self.last_quiet}
        tmp = self.cfg.state_file + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f, indent=1)
        os.replace(tmp, self.cfg.state_file)

    def log(self, event, symbol, mint, price, detail=""):
        new = not os.path.exists(self.cfg.log_file)
        with open(self.cfg.log_file, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["time", "event", "symbol", "mint", "price", "detail"])
            w.writerow([time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), event, symbol, mint, f"{price:.4g}" if price else "", detail])

    def jlog(self, rec: dict):
        with open(self.cfg.signals_file, "a") as f:
            f.write(json.dumps(rec, default=str) + "\n")

    # ---------------- helpers
    def every(self, key, seconds, now) -> bool:
        if now - self.timers[key] >= seconds:
            self.timers[key] = now
            return True
        return False

    def creator_launches(self, creator, now) -> int:
        dq = self.creator_times.get(creator)
        if not dq:
            return 0
        while dq and dq[0] < now - 86400:
            dq.popleft()
        return len(dq)

    def alerts_last_hour(self, now) -> int:
        while self.alert_times and self.alert_times[0] < now - 3600:
            self.alert_times.popleft()
        return len(self.alert_times)

    def submit(self, mint, kind, fn, *args):
        self.pending[mint] = (kind, self.pool.submit(fn, *args))

    # ---------------- feed
    def on_feed(self, events):
        subs, unsubs = [], []
        for t, kind, mint, m in events:
            if kind == "launch":
                creator = m.get("traderPublicKey") or ""
                if creator:
                    self.creator_times[creator].append(t)
                if mint in self.launches or len(self.launches) >= self.cfg.max_trade_subs:
                    continue
                l = Launch(mint=mint, name=str(m.get("name") or "?")[:60], symbol=str(m.get("symbol") or "?")[:20],
                           creator=creator, curve=m.get("bondingCurveKey") or "", t0=t, last_trade=t)
                mc = num(m.get("marketCapSol")) or 0.0
                l.mcap_sol = l.peak_sol = mc
                l.hist.append((t, mc))
                init = num(m.get("initialBuy")) or 0.0
                if creator and init > 0:
                    l.balances[creator] = init
                    l.early[creator] = init
                self.launches[mint] = l
                self.counts["launches"] += 1
                subs.append(mint)
            elif kind == "trade":
                l = self.launches.get(mint)
                if l:
                    l.on_trade(t, m)
            elif kind == "migration":
                self.counts["migrations"] += 1
                if self.launches.pop(mint, None):
                    unsubs.append(mint)
                self.add_pair(mint, t, force=True)
        if self.feed:
            self.feed.watch(subs)
            self.feed.unwatch(unsubs)

    def prune_launches(self, now):
        drop = []
        for mint, l in self.launches.items():
            age = (now - l.t0) / 60
            mc = l.mcap_sol * (self.sol_usd or 0)
            if mint in self.calls or mint in self.pending:
                continue
            if (age > self.cfg.early_max_age_min or now - l.last_trade > 600
                    or (age > self.cfg.early_drop_after_min and (len(l.buyers) < 8 or (self.sol_usd and mc < 5_000)))):
                drop.append(mint)
        for m in drop:
            del self.launches[m]
        if self.feed and drop:
            self.feed.unwatch(drop)

    # ---------------- discovery of DEX pairs
    def add_pair(self, mint, now, force=False):
        if mint in STABLES or mint in self.pairs or mint in self.calls:
            return
        if not force and self.seen.get(mint, 0) > now:
            return
        if len(self.pairs) >= self.cfg.max_pairs:
            return
        self.pairs[mint] = Pair(mint=mint, added=now, next_refresh=now)

    def discover(self, now):
        if self.every("gecko_new", 30, now):
            for m in fetch_gecko_mints("new_pools"):
                self.add_pair(m, now)
        if self.every("gecko_trend", 120, now):
            for m in fetch_gecko_mints("trending_pools"):
                self.add_pair(m, now)
        if self.every("dex_profiles", 30, now):
            for m in fetch_dex_feed("/token-profiles/latest/v1"):
                self.add_pair(m, now)
        if self.every("dex_boosts", 60, now):
            for m in fetch_dex_feed("/token-boosts/latest/v1"):
                self.add_pair(m, now)

    def refresh_pairs(self, now):
        due = [p for p in self.pairs.values() if p.next_refresh <= now]
        due.sort(key=lambda p: p.next_refresh)
        for i in range(0, min(len(due), 120), 30):
            batch = due[i:i + 30]
            res = fetch_dex_pairs([p.mint for p in batch])
            if res is None:
                return
            for p in batch:
                p.next_refresh = now + self.cfg.pair_refresh_s
                d = res.get(p.mint)
                if not d:
                    p.misses += 1
                    if p.misses >= 4:
                        self.drop_pair(p.mint, now, 1800)
                    continue
                p.misses = 0
                if (d.get("dexId") or "").lower() == "pumpfun":        # still on the curve: early lane's job
                    self.drop_pair(p.mint, now, 1800)
                    continue
                p.symbol = (d.get("baseToken") or {}).get("symbol") or p.symbol
                p.name = (d.get("baseToken") or {}).get("name") or p.name
                p.pair, p.dex, p.url = d.get("pairAddress") or p.pair, d.get("dexId") or p.dex, d.get("url") or p.url
                p.created_ms = num(d.get("pairCreatedAt")) or p.created_ms
                p.snap = pair_snap(d, now)
                price = p.snap.get("price") or 0
                p.peak_price = max(p.peak_price, price)
                age_h = (now * 1000 - p.created_ms) / 3.6e6 if p.created_ms else 0
                if age_h > self.cfg.br_max_age_h or (p.snap.get("liq") or 0) < 3_000 or (p.snap.get("mcap") or 0) > self.cfg.br_max_mcap * 3:
                    self.drop_pair(p.mint, now, 6 * 3600)

    def drop_pair(self, mint, now, ttl):
        self.pairs.pop(mint, None)
        self.seen[mint] = now + ttl

    # ---------------- evaluation
    def evaluate(self, now):
        sol = self.sol_usd
        if sol:
            for l in list(self.launches.values()):
                if l.mint in self.calls or l.mint in self.pending or now - l.last_eval < 20:
                    continue
                l.last_eval = now
                fails = early_gates(l, now, sol, self.cfg, self.creator_launches(l.creator, now), l.creator in self.blacklist)
                if fails:
                    l.blocked = fails[0]
                    if "old" not in fails[0]:
                        self.block(fails[0])
                    continue
                if not l.curve or now - l.last_deep < self.cfg.recheck_s:
                    continue
                l.last_deep = now
                self.submit(l.mint, "early", deep_early, l.mint, l.curve, l.creator)
        for p in list(self.pairs.values()):
            if p.mint in self.calls or p.mint in self.pending or now - p.last_eval < self.cfg.recheck_s:
                continue
            fails = breakout_gates(p, now, self.cfg)
            if fails:
                p.blocked = fails[0]
                continue
            p.last_eval = now
            cached = p.rc if (p.rc is not None and now - p.rc_t <= self.cfg.rugcheck_ttl_s) else None
            self.submit(p.mint, "breakout", deep_breakout, p.mint, p.pair, cached)

    def collect(self, now):
        for mint, (kind, fut) in list(self.pending.items()):
            if not fut.done():
                continue
            del self.pending[mint]
            try:
                chain = fut.result()
            except Exception as e:
                print(f"[warn] check failed for {mint}: {e}")
                continue
            if kind == "early":
                l = self.launches.get(mint)
                if not l or not self.sol_usd:
                    continue
                if (chain.get("curve") or {}).get("complete"):
                    continue
                ok, rows = early_safety(l, chain, self.cfg)
                if ok:
                    self.alert_early(l, chain, rows, now)
                else:
                    self.block_from_rows(rows)
            else:
                p = self.pairs.get(mint)
                if not p:
                    continue
                if chain.get("rc_fresh") and chain.get("rc") is not None:
                    p.rc, p.rc_t = chain["rc"], now
                creator = (p.rc or {}).get("creator")
                ok, rows = breakout_safety(p, chain, self.cfg, bool(creator and creator in self.blacklist))
                if ok and not breakout_gates(p, now, self.cfg):
                    self.alert_breakout(p, rows, now)
                elif not ok:
                    self.block_from_rows(rows)
                    if (p.rc or {}).get("rugged") or (chain.get("mint") or {}).get("revoked") is False:
                        self.drop_pair(mint, now, 86400)

    def block(self, reason: str):
        key = re.sub(r"\$?\d[\d.,]*[KMB%]?", "N", reason)[:48]
        self.blocks[key] += 1
        self.quiet_blocks[key] += 1

    def block_from_rows(self, rows):
        for r in rows:
            if not r.startswith("✅"):
                self.block(r[2:].split(" (")[0])
                return

    def can_alert(self, now) -> bool:
        if self.alerts_last_hour(now) >= self.cfg.max_alerts_per_hour:
            self.block("hourly alert cap")
            return False
        return True

    def alert_early(self, l: Launch, chain: dict, rows: list, now: float):
        if not self.can_alert(now):
            return
        price = l.mcap_sol * self.sol_usd / 1e9
        if notify(early_embed(l, chain, rows, now, self.sol_usd)):
            self.record_call(l.mint, l.symbol, "early", price, l.creator, now)
            self.jlog({"type": "alert", "kind": "early", "t": now_iso(), "mint": l.mint, "symbol": l.symbol,
                       "mcap": l.mcap_sol * self.sol_usd, "buyers": len(l.buyers), "checks": rows})

    def alert_breakout(self, p: Pair, rows: list, now: float):
        if not self.can_alert(now):
            return
        if notify(breakout_embed(p, rows, now)):
            self.record_call(p.mint, p.symbol, "breakout", p.snap["price"], (p.rc or {}).get("creator") or "", now)
            self.jlog({"type": "alert", "kind": "breakout", "t": now_iso(), "mint": p.mint, "symbol": p.symbol,
                       "snap": p.snap, "checks": rows})
            self.pairs.pop(p.mint, None)

    def record_call(self, mint, symbol, kind, price, creator, now):
        self.calls[mint] = Call(mint=mint, symbol=symbol, kind=kind, t0=now, price0=price, creator=creator,
                                peak=price, last_price=price, last_seen=now)
        self.alert_times.append(now)
        self.last_alert = now
        self.counts["alerts"] += 1
        self.log(f"ALERT_{kind.upper()}", symbol, mint, price)
        print(f"[alert] {kind} {symbol} {mint}")

    # ---------------- tracker
    def track(self, now):
        live = [c for c in self.calls.values() if not c.closed]
        for c in live:
            if now - c.t0 > self.cfg.track_hours * 3600:
                c.closed = True
        live = [c for c in live if not c.closed]
        for i in range(0, len(live), 30):
            batch = live[i:i + 30]
            res = fetch_dex_pairs([c.mint for c in batch])
            if res is None:
                return
            for c in batch:
                d = res.get(c.mint)
                price = num(d.get("priceUsd")) if d else None
                if not price:
                    l = self.launches.get(c.mint)
                    price = l.mcap_sol * self.sol_usd / 1e9 if (l and self.sol_usd) else None
                if not price:
                    continue
                self.update_call(c, price, now)

    def update_call(self, c: Call, price: float, now: float):
        c.last_price, c.last_seen = price, now
        c.peak = max(c.peak, price)
        hook = TRACKER_WEBHOOK or DISCORD_WEBHOOK
        reached = [x for x in self.cfg.milestones if c.peak >= c.price0 * x and x not in c.hit]
        if reached:
            x = max(reached)
            c.hit.extend(reached)
            notify(milestone_embed(c, x, now), hook=hook)
            self.log("MILESTONE", c.symbol, c.mint, price, f"{x}x")
        if not c.dead and price <= c.price0 * self.cfg.dead_ratio:
            c.dead = c.closed = True
            if c.creator:
                self.blacklist.add(c.creator)
            notify(dead_embed(c), hook=hook)
            self.log("DEAD", c.symbol, c.mint, price, f"peak {c.peak / c.price0:.2f}x")
            self.jlog({"type": "outcome", "t": now_iso(), "mint": c.mint, "symbol": c.symbol, "kind": c.kind,
                       "peak_x": round(c.peak / c.price0, 2), "dead": True})

    def stats_24h(self, now) -> str:
        recent = [c for c in self.calls.values() if c.t0 >= now - 86400]
        return (f"24h: {len(recent)} alerts, {sum(1 for c in recent if 2 in c.hit)} hit 2x, "
                f"{sum(1 for c in recent if c.dead)} died")

    # ---------------- quiet + status
    def quiet_check(self, now):
        gap = self.cfg.quiet_post_h * 3600
        if now - self.last_alert >= gap and now - self.last_quiet >= gap:
            top = ", ".join(f"{k} ({v})" for k, v in self.quiet_blocks.most_common(3)) or "nothing reached the checks"
            self.quiet_blocks.clear()
            notify({"title": "NO QUALITY PLAYS RIGHT NOW", "color": 0x747F8D,
                    "description": f"Nothing passed every rug check in the last {self.cfg.quiet_post_h:g}h.\nMost common blocks: {top}\n{self.stats_24h(now)}"})
            self.last_quiet = now

    def status(self, now):
        top = ", ".join(f"{k} ({v})" for k, v in self.blocks.most_common(4)) or "none"
        line = (f"[status] {now_iso()} feed={self.feed.status() if self.feed else 'off'} sol=${self.sol_usd or 0:.0f} "
                f"launches={self.counts['launches']} watching={len(self.launches)} pairs={len(self.pairs)} "
                f"checking={len(self.pending)} | {self.stats_24h(now)} | top blocks: {top} | "
                f"api/min dex={LIMITERS['dex'].per_min()} rpc={LIMITERS['rpc'].per_min()} rugcheck={LIMITERS['rugcheck'].per_min()}")
        print(line)
        self.status_lines.append(line)
        self.blocks.clear()
        with open(self.cfg.status_file, "w") as f:
            f.write(f"{VERSION} | updated {now_iso()} | up {ago(now - self.started)}\n"
                    f"alerts={'on' if DISCORD_WEBHOOK else 'OFF (no DISCORD_WEBHOOK_URL)'} "
                    f"tracker={'own channel' if TRACKER_WEBHOOK else 'alerts channel'} "
                    f"rpc={'custom' if os.getenv('SOLANA_RPC_URL') else 'public'}\n\n" + "\n".join(self.status_lines) + "\n")

    # ---------------- main loop
    def tick(self, now):
        if self.sol_usd is None or now - self.sol_t > 120:
            v = fetch_sol_usd()
            if v:
                self.sol_usd, self.sol_t = v, now
        if self.feed:
            self.on_feed(self.feed.drain())
        if self.every("prune", 15, now):
            self.prune_launches(now)
        self.discover(now)
        self.refresh_pairs(now)
        self.evaluate(now)
        self.collect(now)
        if self.every("track", 30, now):
            self.track(now)
        if self.every("quiet", 300, now):
            self.quiet_check(now)
        if self.every("status", 60, now):
            self.status(now)
        if self.every("save", 300, now):
            self.save_state()

    def run(self, max_minutes: Optional[float] = None):
        stop = {"flag": False}

        def handler(*_):
            stop["flag"] = True
        os_signal.signal(os_signal.SIGTERM, handler)
        os_signal.signal(os_signal.SIGINT, handler)
        if self.feed:
            self.feed.start()
        end = self.started + max_minutes * 60 if max_minutes else None
        print(f"[start] {VERSION} rpc={'custom' if os.getenv('SOLANA_RPC_URL') else 'public'} "
              f"discord={'on' if DISCORD_WEBHOOK else 'off'}")
        try:
            while not stop["flag"] and (end is None or self.clock() < end):
                try:
                    self.tick(self.clock())
                except Exception:
                    traceback.print_exc()
                time.sleep(1.0)
        finally:
            if self.feed:
                self.feed.stop = True
            self.save_state()
            self.status(self.clock())
            self.pool.shutdown(wait=False, cancel_futures=True)
            print("[stop] saved")


# ======================================================================= test + probe
def send_test():
    now, cfg = time.time(), Config()
    l = Launch(mint="DemoEarly1111111111111111111111111111111pump", name="Demo Early", symbol="DEMO",
               creator="Dev1111111111111111111111111111111111111111", curve="Curve11111111111111111111111111111111111111", t0=now - 900)
    l.hist.extend([(now - 400, 60.0), (now - 10, 95.0)])
    l.mcap_sol = l.peak_sol = 95.0
    l.buyers = {f"w{i}" for i in range(84)}
    l.n_buys, l.n_sells = 140, 38
    l.flow.extend([(now - 60, "buy", 12.0), (now - 30, "sell", 4.0)])
    ok, rows = early_safety(l, {"mint": {"revoked": True, "supply": PUMP_SUPPLY_RAW, "bad_ext": []},
                                "holders": {"top1": 2.9, "top10": 14.1, "dev": 0.0}}, cfg)
    notify(early_embed(l, {"curve": {"progress": 0.31, "reserve_sol": 18.4}}, rows, now, 160.0))
    p = Pair(mint="DemoBreakout11111111111111111111111111111pump", symbol="KYTRO", name="Kytro (sample)", pair="DemoPair",
             dex="pumpswap", created_ms=(now - 42 * 60) * 1000,
             snap={"t": now, "price": 6e-5, "mcap": 59_678, "liq": 17_604, "vol_h1": 93_641, "buys_h1": 410,
                   "sells_h1": 180, "chg_m5": 12, "chg_h1": 64})
    ok, rows = breakout_safety(p, {"rc": {"holders": 612, "top1": 3.2, "top10": 18.5, "insiders": 0.0, "lp_locked": None,
                                          "rugged": False, "dangers": [], "creator": None, "dev": None, "revoked": True},
                                   "mint": {"revoked": True, "bad_ext": []}}, cfg, False)
    notify(breakout_embed(p, rows, now))
    c = Call(mint=p.mint, symbol="KYTRO", kind="breakout", t0=now - 3000, price0=6e-5, peak=1.25e-4)
    notify(milestone_embed(c, 2, now), hook=TRACKER_WEBHOOK or DISCORD_WEBHOOK)
    print("[test] sent sample posts")


def probe(mint: str):
    cfg = Config()
    print(f"mint account: {mint_info(mint)}")
    d = (fetch_dex_pairs([mint]) or {}).get(mint)
    print(f"dex pair: {d.get('dexId') if d else None} {d.get('pairAddress') if d else ''}")
    p = Pair(mint=mint, pair=(d or {}).get("pairAddress", ""), dex=(d or {}).get("dexId", ""),
             created_ms=num((d or {}).get("pairCreatedAt")), snap=pair_snap(d, time.time()) if d else {})
    chain = deep_breakout(mint, p.pair, True)
    print(f"rugcheck: {json.dumps(chain.get('rc'), default=str)}")
    ok, rows = breakout_safety(p, chain, cfg, False)
    print("\n".join(rows))
    print(f"breakout gates: {breakout_gates(p, time.time(), cfg) or 'all passed'}")
    print(f"=> rug checks {'PASSED' if ok else 'FAILED'}")


def main(argv):
    if "--test" in argv:
        send_test()
        return
    if "--probe" in argv:
        probe(argv[argv.index("--probe") + 1])
        return
    mm = float(argv[argv.index("--max-minutes") + 1]) if "--max-minutes" in argv else None
    Bot(Config(), feed=PumpFeed()).run(max_minutes=mm)


if __name__ == "__main__":
    main(sys.argv[1:])
