"""
Solana memecoin signal bot (PAPER mode) -> Discord embeds, with an evidence score that learns.

How it works
  1. Discover new Solana tokens (DexScreener).
  2. Hard gates: liquidity/volume/age floors, mint + freeze authority revoked (on-chain), not rugged.
  3. Score 12 evidence checks (liquidity, volume, holders, top-10 %, insiders %, LP locked, ...).
  4. Estimate a win chance with a tiny logistic model that starts from hand-set priors.
  5. If the evidence is strong enough, post an embed to Discord and track it as a paper trade.
  6. When a paper trade ends (take-profit, stop-loss, timeout) the model learns from the result.

No wallet, no keys, no real trades. You act on signals manually (e.g. in Axiom or Fomo).

Setup:   pip install requests
Env:     DISCORD_WEBHOOK_URL   channel webhook (needed to post)
         SOLANA_RPC_URL        optional Helius/QuickNode endpoint (the public one is rate-limited)
Run:     python memecoin_bot.py                loop forever
         python memecoin_bot.py --once         one pass using state.json (GitHub Actions / cron)
         python memecoin_bot.py --test         send a fake sample embed
         python memecoin_bot.py --report       win rate by confidence + what the model has learned
         python memecoin_bot.py --probe MINT   show what the bot reads from RugCheck for one token
"""
import csv
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from typing import Optional

import requests

RPC_URL = os.getenv("SOLANA_RPC_URL") or "https://api.mainnet-beta.solana.com"
DEXSCREENER = "https://api.dexscreener.com"
RUGCHECK = "https://api.rugcheck.xyz/v1"  # public read endpoints; third-party, can change or rate-limit
DISCORD_WEBHOOK = os.getenv("DISCORD_WEBHOOK_URL")  # keep secret

GREEN, YELLOW, RED = 0x2ECC71, 0xF1C40F, 0xE74C3C
RETRY_SECONDS = 30 * 60          # re-check a rejected token after 30 min
POSTED_SECONDS = 30 * 86400      # never re-signal a posted token for 30 days


@dataclass
class Config:
    # hard gates (cheap pre-filter)
    gate_liquidity_usd: float = 10_000
    gate_volume_h1_usd: float = 5_000
    min_pair_age_min: float = 10        # skip brand-new launches (most dangerous)
    max_pair_age_min: float = 360
    # evidence thresholds (scored, not gates)
    min_liquidity_usd: float = 20_000
    min_volume_h1_usd: float = 10_000
    min_holders: int = 300
    max_top10_pct: float = 30.0
    max_insider_pct: float = 10.0
    min_lp_locked_pct: float = 80.0
    min_evidence_ratio: float = 0.65    # weighted share of checks that must pass to post a signal
    # paper trading
    position_size_usd: float = 25
    take_profit: float = 0.50
    stop_loss: float = 0.20
    max_hold_min: float = 360
    max_open_positions: int = 3
    max_daily_loss_usd: float = 50
    poll_seconds: int = 20
    # learning
    learn_min_samples: int = 30         # until then the model is mostly its starting guess
    # files
    log_file: str = "paper_trades.csv"
    signals_file: str = "signals.jsonl"
    state_file: str = "state.json"


# ---------- Evidence checks + learning model ----------
CHECK_WEIGHTS = {  # starting importance of each check (the model adjusts these as results come in)
    "liquidity": 1.0, "volume": 1.0, "liq_mcap": 0.8, "buy_pressure": 0.6, "not_overheated": 0.6,
    "authorities": 1.5, "holders": 1.0, "top10": 1.2, "insiders": 1.2, "lp_locked": 1.2,
    "rugcheck": 1.5, "socials": 0.4,
}
PRIOR_BIAS = -1.27   # hand-tuned guess: all checks pass ~60% win chance, all fail ~5%
PRIOR_SCALE = 0.135


@dataclass
class Check:
    key: str
    label: str
    weight: float
    ok: Optional[bool]  # True pass, False fail, None unknown (data unavailable)


class Model:
    """Tiny online logistic regression. Features: +1 check passed, -1 failed, 0 unknown."""
    LR = 0.05
    L2 = 0.01  # gently pulls weights back toward the starting priors

    def __init__(self):
        self.bias = PRIOR_BIAS
        self.w = {k: PRIOR_SCALE * wt for k, wt in CHECK_WEIGHTS.items()}
        self.n = 0

    def predict(self, x: dict) -> float:
        z = self.bias + sum(self.w.get(k, 0.0) * v for k, v in x.items())
        z = max(-30.0, min(30.0, z))
        return 1.0 / (1.0 + math.exp(-z))

    def update(self, x: dict, win: bool):
        err = (1.0 if win else 0.0) - self.predict(x)
        self.bias = max(-4.0, min(2.0, self.bias + self.LR * err))
        for k, v in x.items():
            prior = PRIOR_SCALE * CHECK_WEIGHTS.get(k, 0.0)
            w = self.w.get(k, prior)
            w += self.LR * err * v - self.L2 * (w - prior)
            self.w[k] = max(-1.5, min(1.5, w))
        self.n += 1

    def to_dict(self):
        return {"bias": self.bias, "w": self.w, "n": self.n}

    @classmethod
    def from_dict(cls, d):
        m = cls()
        if isinstance(d, dict):
            m.bias = d.get("bias", m.bias)
            m.w.update(d.get("w", {}))
            m.n = d.get("n", 0)
        return m


def evaluate(pair: dict, rc: dict, authorities_ok: Optional[bool], cfg: Config) -> list:
    liq = (pair.get("liquidity") or {}).get("usd") or 0
    vol_h1 = (pair.get("volume") or {}).get("h1") or 0
    mcap = pair.get("marketCap") or pair.get("fdv")
    h1 = (pair.get("txns") or {}).get("h1") or {}
    buys, sells = h1.get("buys", 0), h1.get("sells", 0)
    h1_chg = (pair.get("priceChange") or {}).get("h1")
    info = pair.get("info") or {}

    def known(v, fn):
        return None if v is None else bool(fn(v))

    ratio = (liq / mcap) if mcap else None
    rc_ok = None
    if rc.get("ok"):
        rc_ok = (not rc.get("rugged")) and (rc.get("danger_risks") or 0) == 0

    return [
        Check("liquidity", f"Liquidity ≥ {usd(cfg.min_liquidity_usd)}", CHECK_WEIGHTS["liquidity"], liq >= cfg.min_liquidity_usd),
        Check("volume", f"1h volume ≥ {usd(cfg.min_volume_h1_usd)}", CHECK_WEIGHTS["volume"], vol_h1 >= cfg.min_volume_h1_usd),
        Check("liq_mcap", "Liquidity ≥ 3% of market cap", CHECK_WEIGHTS["liq_mcap"], known(ratio, lambda r: r >= 0.03)),
        Check("buy_pressure", "1h buys ≥ sells", CHECK_WEIGHTS["buy_pressure"], (buys >= sells) if (buys + sells) > 0 else None),
        Check("not_overheated", "1h gain ≤ +200%", CHECK_WEIGHTS["not_overheated"], known(_num(h1_chg), lambda c: c <= 200)),
        Check("authorities", "Mint + freeze authority revoked", CHECK_WEIGHTS["authorities"], authorities_ok),
        Check("holders", f"Holders ≥ {cfg.min_holders}", CHECK_WEIGHTS["holders"], known(rc.get("holders"), lambda h: h >= cfg.min_holders)),
        Check("top10", f"Top 10 holders ≤ {cfg.max_top10_pct:.0f}%", CHECK_WEIGHTS["top10"], known(rc.get("top10_pct"), lambda p: p <= cfg.max_top10_pct)),
        Check("insiders", f"Insiders ≤ {cfg.max_insider_pct:.0f}%", CHECK_WEIGHTS["insiders"], known(rc.get("insider_pct"), lambda p: p <= cfg.max_insider_pct)),
        Check("lp_locked", f"LP burned/locked ≥ {cfg.min_lp_locked_pct:.0f}%", CHECK_WEIGHTS["lp_locked"], known(rc.get("lp_locked_pct"), lambda p: p >= cfg.min_lp_locked_pct)),
        Check("rugcheck", "RugCheck: no rug/danger flags", CHECK_WEIGHTS["rugcheck"], rc_ok),
        Check("socials", "Website or socials listed", CHECK_WEIGHTS["socials"], bool(info.get("websites") or info.get("socials"))),
    ]


def evidence_ratio(checks: list) -> float:
    total = sum(c.weight for c in checks)
    return sum(c.weight for c in checks if c.ok is True) / total if total else 0.0


def to_x(checks: list) -> dict:
    return {c.key: (0 if c.ok is None else (1 if c.ok else -1)) for c in checks}


# ---------- Formatting helpers ----------
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
    return "n/a" if n is None else f"{n:+.1f}%"


def price_str(p):
    return "$" + f"{float(p):.10f}".rstrip("0").rstrip(".")


def now_iso():
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


# ---------- Discord ----------
def notify(embed: dict, content: str = ""):
    """Post one embed (plus optional plain message text) via webhook. A Discord failure must never break the bot."""
    if not DISCORD_WEBHOOK:
        print("[warn] DISCORD_WEBHOOK_URL not set; skipping alert")
        return
    payload = {"embeds": [embed]}
    if content:
        payload["content"] = content  # regular message text, shown above the embed
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


def build_signal_embed(pair, rc, checks, p_win, model_n, cfg: Config) -> dict:
    base = pair.get("baseToken") or {}
    mint = base.get("address", "?")
    price = float(pair["priceUsd"])
    liq = (pair.get("liquidity") or {}).get("usd")
    vol = pair.get("volume") or {}
    chg = pair.get("priceChange") or {}
    h1 = (pair.get("txns") or {}).get("h1") or {}
    info = pair.get("info") or {}
    age_h = (time.time() * 1000 - (pair.get("pairCreatedAt") or 0)) / 3_600_000
    mcap = pair.get("marketCap") or pair.get("fdv")

    passed = sum(1 for c in checks if c.ok is True)
    unknown = sum(1 for c in checks if c.ok is None)
    ratio = evidence_ratio(checks)
    icon = {True: "✅", False: "❌", None: "❔"}
    evidence_lines = "\n".join(f"{icon[c.ok]} {c.label}" for c in checks)

    holders = rc.get("holders")
    top10 = rc.get("top10_pct")
    ins, nets = rc.get("insider_pct"), rc.get("insider_networks")
    if ins is not None:
        insiders = f"{ins:.1f}%" + (f" ({nets} network{'s' if nets != 1 else ''})" if nets else "")
    elif nets is not None:
        insiders = f"{nets} network(s), % n/a"
    else:
        insiders = "n/a"
    lp = rc.get("lp_locked_pct")

    if model_n >= cfg.learn_min_samples:
        learn = f"Model has learned from {model_n} paper outcomes."
    else:
        learn = f"Early estimate: learning {model_n}/{cfg.learn_min_samples} outcomes, still mostly the starting guess."

    links = []
    if pair.get("url"):
        links.append(f"[Chart]({pair['url']})")
    links.append(f"[RugCheck](https://rugcheck.xyz/tokens/{mint})")
    for w in info.get("websites") or []:
        if w.get("url"):
            links.append(f"[{w.get('label') or 'Website'}]({w['url']})")
    for s in info.get("socials") or []:
        if s.get("url"):
            links.append(f"[{(s.get('type') or 'link').title()}]({s['url']})")

    fields = [
        {"name": "Market cap", "value": usd(mcap), "inline": True},
        {"name": "Liquidity", "value": usd(liq), "inline": True},
        {"name": "Price", "value": price_str(price), "inline": True},
        {"name": "Volume", "value": f"5m {usd(vol.get('m5'))}\n1h {usd(vol.get('h1'))}\n24h {usd(vol.get('h24'))}", "inline": True},
        {"name": "Price change", "value": f"5m {pct(chg.get('m5'))}\n1h {pct(chg.get('h1'))}\n24h {pct(chg.get('h24'))}", "inline": True},
        {"name": "1h txns", "value": f"{h1.get('buys', 0)} buys / {h1.get('sells', 0)} sells", "inline": True},
        {"name": "Holders", "value": f"{holders:,}" if holders is not None else "n/a", "inline": True},
        {"name": "Top 10 holders", "value": f"{top10:.1f}% (pools excluded)" if top10 is not None else "n/a", "inline": True},
        {"name": "Insiders", "value": insiders, "inline": True},
        {"name": "LP burned/locked", "value": f"{lp:.0f}%" if lp is not None else "n/a", "inline": True},
        {"name": "Age / DEX", "value": f"{age_h:.1f}h on {pair.get('dexId', '?')}", "inline": True},
        {"name": "Est. win chance", "value": f"**~{p_win:.0%}**", "inline": True},
        {"name": f"Evidence: {passed}/{len(checks)} checks passed ({ratio:.0%} weighted, {unknown} unknown)", "value": evidence_lines, "inline": False},
        {"name": "Confidence note", "value": learn, "inline": False},
        {"name": "NOT checked", "value": "Honeypot (sell) simulation and bundled-launch detection beyond RugCheck's insider flags. Verify before buying.", "inline": False},
        {"name": "Paper plan", "value": f"TP +{cfg.take_profit:.0%} ({price_str(price * (1 + cfg.take_profit))}) / SL -{cfg.stop_loss:.0%} ({price_str(price * (1 - cfg.stop_loss))}) / max hold {cfg.max_hold_min / 60:.0f}h", "inline": False},
        {"name": "Links", "value": " • ".join(links)[:1000], "inline": False},
    ]
    embed = {
        "title": f"{base.get('symbol', '?')} — {base.get('name', '')}"[:256],
        "color": GREEN if ratio >= 0.8 else YELLOW,
        "fields": fields,
        "footer": {"text": "Paper signal. Not financial advice, do your own research."},
        "timestamp": now_iso(),
    }
    if pair.get("url"):
        embed["url"] = pair["url"]
    if info.get("imageUrl"):
        embed["thumbnail"] = {"url": info["imageUrl"]}
    return embed


def build_exit_embed(pos: "Position", price: float, pnl: float, reason: str, model_n: int) -> dict:
    change = price / pos.entry_price - 1 if pos.entry_price else 0
    return {
        "title": f"EXIT: {pos.symbol} ({reason.replace('_', ' ')})",
        "color": GREEN if pnl >= 0 else RED,
        "fields": [
            {"name": "Entry", "value": price_str(pos.entry_price), "inline": True},
            {"name": "Now", "value": price_str(price) if price else "no price (possible rug)", "inline": True},
            {"name": "Change", "value": f"{change:+.1%}", "inline": True},
            {"name": "Paper PnL", "value": f"${pnl:.2f} on ${pos.size_usd:.0f} (ignores fees/slippage)", "inline": False},
        ],
        "footer": {"text": f"Result logged. Model has now learned from {model_n} outcomes."},
        "timestamp": now_iso(),
    }


# ---------- Data ----------
def get_json(url, **kwargs):
    try:
        r = requests.get(url, timeout=10, **kwargs)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print(f"[warn] request failed: {url} ({e})")
        return None


def latest_solana_mints():
    data = get_json(f"{DEXSCREENER}/token-profiles/latest/v1") or []
    return [d["tokenAddress"] for d in data if d.get("chainId") == "solana"]


def best_pair(mint):
    data = get_json(f"{DEXSCREENER}/latest/dex/tokens/{mint}")
    pairs = [p for p in (data or {}).get("pairs") or [] if p.get("chainId") == "solana"]
    if not pairs:
        return None
    return max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)


def mint_authorities_revoked(mint) -> Optional[bool]:
    """True if mint and freeze authority are both revoked; None if the RPC call failed."""
    try:
        r = requests.post(RPC_URL, timeout=10, json={
            "jsonrpc": "2.0", "id": 1, "method": "getAccountInfo",
            "params": [mint, {"encoding": "jsonParsed"}],
        })
        info = r.json()["result"]["value"]["data"]["parsed"]["info"]
        return info.get("mintAuthority") is None and info.get("freezeAuthority") is None
    except Exception as e:
        print(f"[warn] authority check failed for {mint}: {e}")
        return None


def parse_rugcheck(rep) -> dict:
    """Defensive parse of a RugCheck report. Anything missing stays None ('unknown')."""
    rc = {"ok": False, "holders": None, "top10_pct": None, "insider_pct": None, "insider_networks": None,
          "lp_locked_pct": None, "rugged": None, "danger_risks": None, "risk_names": []}
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
        rc["top10_pct"] = sum(pcts[:10])
    if any("insider" in h for h in holders):
        rc["insider_pct"] = sum((_num(h.get("pct")) or 0.0) for h in holders if h.get("insider"))
    nets = rep.get("insiderNetworks")
    if isinstance(nets, list):
        rc["insider_networks"] = len(nets)

    lp = [v for v in (_num(((m or {}).get("lp") or {}).get("lpLockedPct")) for m in (rep.get("markets") or [])) if v is not None]
    if lp:
        rc["lp_locked_pct"] = max(lp)

    if rep.get("rugged") is not None:
        rc["rugged"] = bool(rep.get("rugged"))
    danger = [r.get("name") for r in (rep.get("risks") or []) if isinstance(r, dict) and str(r.get("level", "")).lower() == "danger"]
    rc["danger_risks"] = len(danger)
    rc["risk_names"] = danger
    return rc


def fetch_rugcheck(mint) -> dict:
    return parse_rugcheck(get_json(f"{RUGCHECK}/tokens/{mint}/report"))


def passes_gates(pair, cfg: Config) -> bool:
    liq = (pair.get("liquidity") or {}).get("usd") or 0
    vol_h1 = (pair.get("volume") or {}).get("h1") or 0
    created_ms = pair.get("pairCreatedAt")
    if not created_ms:
        return False
    age_min = (time.time() * 1000 - created_ms) / 60_000
    return (liq >= cfg.gate_liquidity_usd and vol_h1 >= cfg.gate_volume_h1_usd
            and cfg.min_pair_age_min <= age_min <= cfg.max_pair_age_min)


# ---------- Bot ----------
@dataclass
class Position:
    mint: str
    symbol: str
    entry_price: float
    size_usd: float
    url: str = ""
    opened_at: float = field(default_factory=time.time)
    x: dict = field(default_factory=dict)
    p_win: float = 0.0


class PaperBot:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        self.positions: dict = {}
        self.seen: dict = {}  # mint -> unix time it may be re-checked
        self.model = Model()
        self.realized_pnl_today = 0.0
        self.day = time.strftime("%F")
        if not os.path.exists(cfg.log_file):
            with open(cfg.log_file, "w", newline="") as f:
                csv.writer(f).writerow(["time", "action", "symbol", "mint", "price", "pnl_usd", "reason"])
        if not os.path.exists(cfg.signals_file):
            open(cfg.signals_file, "w").close()

    def log(self, action, pos, price, pnl=0.0, reason=""):
        with open(self.cfg.log_file, "a", newline="") as f:
            csv.writer(f).writerow([time.strftime("%F %T"), action, pos.symbol, pos.mint, price, round(pnl, 2), reason])
        print(f"[{action}] {pos.symbol} @ {price} pnl={pnl:.2f} {reason}")

    def jlog(self, record: dict):
        with open(self.cfg.signals_file, "a") as f:
            f.write(json.dumps(record) + "\n")

    def roll_day(self):
        today = time.strftime("%F")
        if today != self.day:
            self.day, self.realized_pnl_today = today, 0.0

    def scan(self):
        cfg = self.cfg
        if self.realized_pnl_today <= -cfg.max_daily_loss_usd:
            return  # daily loss limit hit: no new signals today
        now = time.time()
        for mint in latest_solana_mints():
            if len(self.positions) >= cfg.max_open_positions:
                break
            if mint in self.positions or (self.seen.get(mint) or 0) > now:
                continue
            self.seen[mint] = now + RETRY_SECONDS
            pair = best_pair(mint)
            if not pair or not passes_gates(pair, cfg):
                continue
            auth = mint_authorities_revoked(mint)
            if auth is not True:
                continue  # authorities live, or RPC failed: skip (retried later)
            rc = fetch_rugcheck(mint)
            if rc["rugged"]:
                continue
            checks = evaluate(pair, rc, auth, cfg)
            ratio = evidence_ratio(checks)
            if ratio < cfg.min_evidence_ratio:
                continue
            x = to_x(checks)
            p_win = self.model.predict(x)
            price = float(pair["priceUsd"])
            pos = Position(mint, pair["baseToken"]["symbol"], price, cfg.position_size_usd,
                           url=pair.get("url", ""), x=x, p_win=p_win)
            self.positions[mint] = pos
            self.seen[mint] = now + POSTED_SECONDS
            self.log("SIGNAL", pos, price)
            self.jlog({"type": "signal", "t": now_iso(), "mint": mint, "symbol": pos.symbol, "price": price,
                       "p_win": round(p_win, 4), "evidence_ratio": round(ratio, 4), "x": x,
                       "metrics": {"mcap": pair.get("marketCap") or pair.get("fdv"),
                                   "liquidity": (pair.get("liquidity") or {}).get("usd"),
                                   "holders": rc["holders"], "top10_pct": rc["top10_pct"],
                                   "insider_pct": rc["insider_pct"], "lp_locked_pct": rc["lp_locked_pct"]}})
            notify(build_signal_embed(pair, rc, checks, p_win, self.model.n, cfg), content=f"`{mint}`")  # CA

    def manage(self):
        cfg = self.cfg
        for mint, pos in list(self.positions.items()):
            age_min = (time.time() - pos.opened_at) / 60
            pair = best_pair(mint)
            if pair:
                price = float(pair["priceUsd"])
            elif age_min >= cfg.max_hold_min:
                price = 0.0  # pair vanished and we've waited long enough: treat as a rug
            else:
                continue
            change = price / pos.entry_price - 1
            reason = None
            if not pair:
                reason = "no_price"
            elif change >= cfg.take_profit:
                reason = "take_profit"
            elif change <= -cfg.stop_loss:
                reason = "stop_loss"
            elif age_min >= cfg.max_hold_min:
                reason = "timeout"
            if reason:
                pnl = pos.size_usd * change
                self.realized_pnl_today += pnl
                del self.positions[mint]
                if pos.x:
                    self.model.update(pos.x, pnl > 0)  # <- the bot learns from the result
                self.log("EXIT", pos, price, pnl, reason)
                self.jlog({"type": "outcome", "t": now_iso(), "mint": mint, "symbol": pos.symbol,
                           "pnl": round(pnl, 2), "reason": reason, "win": pnl > 0, "p_win": round(pos.p_win, 4)})
                notify(build_exit_embed(pos, price, pnl, reason, self.model.n), content=f"`{pos.mint}`")  # CA

    # ----- persistence -----
    def save_state(self):
        state = {
            "day": self.day,
            "realized_pnl_today": self.realized_pnl_today,
            "seen": dict(list(self.seen.items())[-3000:]),
            "positions": [vars(p) for p in self.positions.values()],
            "model": self.model.to_dict(),
        }
        with open(self.cfg.state_file, "w") as f:
            json.dump(state, f)

    def load_state(self):
        if not os.path.exists(self.cfg.state_file):
            return
        try:
            with open(self.cfg.state_file) as f:
                s = json.load(f)
            self.day = s.get("day", self.day)
            self.realized_pnl_today = s.get("realized_pnl_today", 0.0)
            seen = s.get("seen", {})
            self.seen = seen if isinstance(seen, dict) else dict.fromkeys(seen, time.time() + RETRY_SECONDS)
            self.positions = {p["mint"]: Position(**p) for p in s.get("positions", [])}
            self.model = Model.from_dict(s.get("model"))
        except Exception as e:
            print(f"[warn] could not load state, starting fresh: {e}")

    def run_once(self):
        """One pass for schedulers (GitHub Actions, cron). State persists in state.json."""
        self.load_state()
        self.roll_day()
        self.manage()
        self.scan()
        self.save_state()

    def run(self):
        print("Signal bot started (paper mode). Ctrl+C to stop.")
        self.load_state()
        while True:
            self.roll_day()
            self.manage()
            self.scan()
            self.save_state()
            time.sleep(self.cfg.poll_seconds)


# ---------- Tools ----------
def report(cfg: Config):
    outcomes = []
    if os.path.exists(cfg.signals_file):
        with open(cfg.signals_file) as f:
            for line in f:
                try:
                    rec = json.loads(line)
                except ValueError:
                    continue
                if rec.get("type") == "outcome":
                    outcomes.append(rec)
    n = len(outcomes)
    print(f"Resolved paper signals: {n}")
    if n:
        wins = sum(1 for o in outcomes if o["win"])
        print(f"Win rate: {wins / n:.0%}  |  total paper PnL: ${sum(o['pnl'] for o in outcomes):.2f}")
        for lo, hi, name in ((0, 0.35, "est. win chance < 35%"), (0.35, 0.5, "35-50%"), (0.5, 1.01, "> 50%")):
            b = [o for o in outcomes if lo <= o.get("p_win", 0) < hi]
            if b:
                print(f"  {name}: {len(b)} signals, actual win rate {sum(1 for o in b if o['win']) / len(b):.0%}")
    bot = PaperBot(cfg)
    bot.load_state()
    print(f"\nModel learned from {bot.model.n} outcomes (needs ~{cfg.learn_min_samples}+ to mean much).")
    print("Check weights (higher = trusted more; started at prior):")
    for k, w in sorted(bot.model.w.items(), key=lambda kv: -kv[1]):
        print(f"  {k:15s} {w:+.3f}   (prior {PRIOR_SCALE * CHECK_WEIGHTS[k]:+.3f})")


def probe(mint: str):
    rep = get_json(f"{RUGCHECK}/tokens/{mint}/report")
    if not rep:
        print("No data (RugCheck unreachable, rate-limited, or unknown token).")
        return
    print("top-level keys:", sorted(rep.keys()))
    print("totalHolders:", rep.get("totalHolders"))
    print("topHolders[:3]:", json.dumps((rep.get("topHolders") or [])[:3], indent=1)[:1500])
    print("insiderNetworks:", json.dumps(rep.get("insiderNetworks"))[:600])
    m0 = (rep.get("markets") or [{}])[0] or {}
    print("markets[0].lp:", json.dumps(m0.get("lp"))[:600])
    print("risks:", [(r.get("name"), r.get("level")) for r in rep.get("risks") or [] if isinstance(r, dict)])
    print("\nWhat the bot extracted:", parse_rugcheck(rep))


def send_test():
    """Fake token, just to check the webhook and how the embed looks on your phone."""
    cfg = Config()
    pair = {
        "baseToken": {"address": "TestMint1111111111111111111111111111111111", "symbol": "TEST", "name": "Test Coin"},
        "priceUsd": "0.00004213", "liquidity": {"usd": 48_500}, "marketCap": 4_213_000,
        "volume": {"m5": 3_200, "h1": 61_000, "h24": 410_000},
        "priceChange": {"m5": 2.1, "h1": 18.4, "h24": 96.0},
        "txns": {"h1": {"buys": 412, "sells": 301}},
        "pairCreatedAt": (time.time() - 3 * 3600) * 1000, "dexId": "raydium",
        "url": "https://dexscreener.com/solana",
        "info": {"websites": [{"url": "https://example.com", "label": "Website"}]},
    }
    rc = {"ok": True, "holders": 1840, "top10_pct": 18.2, "insider_pct": 4.1, "insider_networks": 1,
          "lp_locked_pct": 100.0, "rugged": False, "danger_risks": 0, "risk_names": []}
    checks = evaluate(pair, rc, True, cfg)
    model = Model()
    notify(build_signal_embed(pair, rc, checks, model.predict(to_x(checks)), model.n, cfg),
           content=f"`{pair['baseToken']['address']}`")  # CA
    print("Test embed sent (check your Discord channel).")


if __name__ == "__main__":
    if "--test" in sys.argv:
        send_test()
    elif "--report" in sys.argv:
        report(Config())
    elif "--probe" in sys.argv:
        i = sys.argv.index("--probe")
        probe(sys.argv[i + 1]) if len(sys.argv) > i + 1 else print("usage: --probe MINT")
    elif "--once" in sys.argv:
        PaperBot(Config()).run_once()
    else:
        PaperBot(Config()).run()
