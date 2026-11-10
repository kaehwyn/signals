"""Data sources: rate-limited HTTP, Solana RPC, DexScreener, GeckoTerminal, RugCheck and the live pump.fun feed.
Recoverable errors never crash the process: calls return None and the caller retries with backoff (§6.3)."""
import base64
import json
import os
import queue
import struct
import threading
import time
from collections import Counter, deque
from datetime import datetime
from typing import Optional

import requests

RPC_URL = os.getenv("SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com"
DEXSCREENER = "https://api.dexscreener.com"
GECKO = "https://api.geckoterminal.com/api/v2"
GECKO_HEADERS = {"Accept": "application/json;version=20230302"}
RUGCHECK = "https://api.rugcheck.xyz/v1"
PUMPPORTAL_URL = os.getenv("PUMPPORTAL_URL") or "wss://pumpportal.fun/api/data"
PUMPPORTAL_KEY = os.getenv("PUMPPORTAL_API_KEY")

SOL_MINT = "So11111111111111111111111111111111111111112"
USDC_MINT = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"
STABLES = {SOL_MINT, USDC_MINT, "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB"}
RAYDIUM_AMM_AUTHORITY = "5Q544fKrFoe6tsEbD7S8EmxGTJYAKtTVhAW5Q5pge4j1"
PUMP_SUPPLY_RAW = 1_000_000_000 * 10 ** 6
PUMP_CURVE_TOKENS_RAW = 793_100_000 * 10 ** 6
RISKY_EXTENSIONS = {"transferHook", "permanentDelegate", "nonTransferable", "defaultAccountState", "confidentialTransferMint"}
BACKOFF_STEPS = (5, 15, 45, 90, 180)          # §6.3: 5s, 15s, 45s, 90s, cap 3 min

# every failed / rate-limited call is recorded here so the bot can warn #bot about flapping (§6.3)
FAILURES = deque(maxlen=5000)


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

    def fail(self):
        """Exponential backoff for this host: 5s, 15s, 45s, 90s, then 3 min (never restarts the process)."""
        self.streak = getattr(self, "streak", 0) + 1
        self.penalize(BACKOFF_STEPS[min(self.streak, len(BACKOFF_STEPS)) - 1])

    def ok(self):
        self.streak = 0

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
        lim.fail()
        FAILURES.append((time.time(), host))
        return None, -1
    if r.status_code == 429:
        try:
            ra = float(r.headers.get("Retry-After", 20))
        except ValueError:
            ra = 20.0
        lim.penalize(max(5.0, min(ra, 120.0)))
        FAILURES.append((time.time(), host))
        return None, 429
    if r.status_code >= 500:
        lim.fail()
        FAILURES.append((time.time(), host))
        return None, r.status_code
    lim.ok()
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


def fetch_dex_all_pairs(mints: list):
    """Up to 30 tokens per call -> {mint: [every solana pair where it is the base]}; None if unavailable."""
    if not mints:
        return {}
    data, st = http_json("dex", f"{DEXSCREENER}/tokens/v1/solana/{','.join(mints)}", max_wait=2.0)
    if st != 200 or data is None:
        return None
    pairs = data if isinstance(data, list) else (data.get("pairs") or [])
    want, out = set(mints), {}
    for p in pairs:
        if isinstance(p, dict) and p.get("chainId") == "solana":
            m = (p.get("baseToken") or {}).get("address")
            if m in want:
                out.setdefault(m, []).append(p)
    return out


def token_balance(owner: str, mint: str) -> Optional[float]:
    """Raw token balance of a wallet for one mint (sum of its token accounts). None = lookup failed."""
    res = rpc("getTokenAccountsByOwner", [owner, {"mint": mint}, {"encoding": "jsonParsed"}])
    try:
        return float(sum(int(a["account"]["data"]["parsed"]["info"]["tokenAmount"]["amount"]) for a in res["value"]))
    except Exception:
        return None


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
        backoff_i = 0
        while not self.stop:
            ws = None
            try:
                ws = websocket.create_connection(self.url, timeout=10)
                ws.send(json.dumps({"method": "subscribeNewToken"}))
                ws.send(json.dumps({"method": "subscribeMigration"}))
                if self.subs:
                    self._send_keys(ws, "subscribeTokenTrade", self.subs)
                ws.settimeout(1)
                self.connected, self.err, backoff_i = True, "", 0
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
                time.sleep(BACKOFF_STEPS[min(backoff_i, len(BACKOFF_STEPS) - 1)])
                backoff_i += 1

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


