"""
Spidey Bot — Solana memecoin signals for Discord (spec: "Spidey Bot Upgrade Spec", 10 Nov 2026).

What it posts (every coin post starts with TYPE • $TICKER • score, then the full CA):
  #signals      NEW (once per CA, ever), UPDATE / BREAKOUT (only on a material change, 10-min cooldown),
                2-hourly 24h tracker
  #wins         2X / 3X / 5X cards (once per level, from valid prints only)
  #graveyard    INVALIDATED / DEAD (once per CA, with CA + jump link), daily "why it died" at 9:05 PM PT
  #early-watch  lower-bar feed with a NOT A BUY CALL banner; promoted to #signals when it qualifies
  #recap        hourly recap 8 AM–10 PM PT, daily leaderboard 9 PM PT
  #bot          boot/recovery message, 15-min heartbeat, poll-gap and flapping warnings
A channel without its own webhook falls back to #signals (wins/graveyard fall back to the tracker webhook first;
#bot messages only go to the log without a #bot webhook).

Where coins come from: live pump.fun feed (PumpPortal: launches, trades, migrations), GeckoTerminal new/trending
pools, DexScreener profiles/boosts. Checks: Solana RPC (authorities, holders, curve, dev balance) + RugCheck.
Every setting lives in spidey/config.py and can be changed with config.json or SPIDEY_* env vars.

Webhooks (env): DISCORD_WEBHOOK_URL (#signals, required), DISCORD_WINS_WEBHOOK_URL, DISCORD_GRAVEYARD_WEBHOOK_URL,
                DISCORD_EARLY_WEBHOOK_URL, DISCORD_RECAP_WEBHOOK_URL, DISCORD_BOT_WEBHOOK_URL,
                DISCORD_TRACKER_WEBHOOK_URL (old setting: used for wins + graveyard if those aren't set)
Optional:       SOLANA_RPC_URL (free Helius/QuickNode key = faster, fewer failed holder checks)

Run:   pip install requests websocket-client
       python memecoin_bot.py                  run forever (use systemd/pm2/docker on a VPS — see DEPLOY.md)
       python memecoin_bot.py --max-minutes N  run N minutes, save, exit (GitHub Actions)
       python memecoin_bot.py --test           post one sample of every message type
       python memecoin_bot.py --probe MINT     run the checks on one token and print them
"""
import csv
import json
import os
import re
import signal as os_signal
import subprocess
import sys
import threading
import time
import traceback
from collections import Counter, defaultdict, deque
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Optional

import requests

from spidey import messages as M
from spidey import schedule
from spidey.catalyst import check_catalyst
from spidey.config import Config, load_config
from spidey.lifecycle import Lifecycle, Poster
from spidey.sources import (FAILURES, LIMITERS, PUMP_SUPPLY_RAW, STABLES, PumpFeed, curve_state, fetch_dex_all_pairs,
                            fetch_dex_feed, fetch_gecko_mints, fetch_rugcheck, fetch_sol_usd, holder_stats,
                            largest_holders, mint_info, num, token_balance)
from spidey.store import PT, Record, Store, fmt_pt, norm_ca

VERSION = "SPIDEY-7.0"
WEBHOOKS = {
    "signals": os.getenv("DISCORD_WEBHOOK_URL"),
    "wins": os.getenv("DISCORD_WINS_WEBHOOK_URL") or os.getenv("DISCORD_TRACKER_WEBHOOK_URL"),
    "graveyard": os.getenv("DISCORD_GRAVEYARD_WEBHOOK_URL") or os.getenv("DISCORD_TRACKER_WEBHOOK_URL"),
    "early-watch": os.getenv("DISCORD_EARLY_WEBHOOK_URL"),
    "recap": os.getenv("DISCORD_RECAP_WEBHOOK_URL"),
    "bot": os.getenv("DISCORD_BOT_WEBHOOK_URL"),
}


# ======================================================================= Discord
class DiscordPoster(Poster):
    """Webhook per channel. ?wait=true returns the message id, so posts can carry jump links."""

    def __init__(self, hooks: dict, dry_run: bool = False):
        self.hooks, self.dry = hooks, dry_run
        self.guilds = {}

    def url_for(self, channel: str) -> Optional[str]:
        if self.hooks.get(channel):
            return self.hooks[channel]
        return None if channel == "bot" else self.hooks.get("signals")

    def _guild(self, url: str) -> Optional[str]:
        if url not in self.guilds:
            try:
                r = requests.get(url, timeout=10)
                self.guilds[url] = (r.json() or {}).get("guild_id") if r.status_code == 200 else None
            except Exception:
                return None
        return self.guilds[url]

    def send(self, channel: str, embed: dict, content: Optional[str] = None) -> Optional[dict]:
        embed = {k: v for k, v in embed.items() if not k.startswith("_")}
        url = self.url_for(channel)
        if self.dry or not url:
            print(f"[{channel}] {(content + ' | ') if content else ''}{embed.get('title')}")
            return {"id": None, "jump": None} if (self.dry or channel == "bot") else None
        payload = {"embeds": [embed], "allowed_mentions": {"parse": []}}
        if content:
            payload["content"] = content
        for attempt in range(3):
            try:
                r = requests.post(url + ("&" if "?" in url else "?") + "wait=true", json=payload, timeout=15)
            except Exception as e:
                print(f"[warn] discord {channel}: {e}")
                time.sleep(2 * (attempt + 1))
                continue
            if r.status_code == 429:
                try:
                    time.sleep(min(float(r.json().get("retry_after", 1)), 10))
                except Exception:
                    time.sleep(1)
                continue
            if r.status_code >= 400:
                print(f"[warn] discord {channel} {r.status_code}: {r.text[:200]}")
                return None
            try:
                msg = r.json()
            except ValueError:
                return {"id": None, "jump": None}
            gid = self._guild(url)
            jump = f"https://discord.com/channels/{gid}/{msg.get('channel_id')}/{msg.get('id')}" if gid and msg.get("id") else None
            return {"id": msg.get("id"), "jump": jump}
        return None


# ======================================================================= helpers
def ramp(x, lo, hi):
    if x is None:
        return None
    if lo == hi:
        return 100.0 if x >= hi else 0.0
    return 100.0 * max(0.0, min(1.0, (x - lo) / (hi - lo)))


def wavg(parts):
    parts = [(v, w) for v, w in parts if v is not None]
    return sum(v * w for v, w in parts) / sum(w for _, w in parts) if parts else 0.0


def line(ok: Optional[bool], text: str) -> str:
    return f"{'✅' if ok else ('❔' if ok is None else '❌')} {text}"


def pfmt(x, d=1):
    return "n/a" if x is None else f"{x:.{d}f}%"


# ======================================================================= curve coins (live feed)
@dataclass
class Launch:
    mint: str
    name: str
    symbol: str
    creator: str
    curve: str
    t0: float
    mcap_sol: float = 0.0
    peak_sol: float = 0.0
    mc0: float = 0.0
    n_buys: int = 0
    n_sells: int = 0
    buyers: set = field(default_factory=set)
    balances: dict = field(default_factory=dict)       # wallet -> tokens (UI units)
    first_buy: dict = field(default_factory=dict)      # wallet -> seconds after launch of first buy
    flow: deque = field(default_factory=lambda: deque(maxlen=6000))
    hist: deque = field(default_factory=lambda: deque(maxlen=800))
    dev_sold: bool = False
    dev_max: float = 0.0
    rugged: bool = False
    last_trade: float = 0.0
    last_eval: float = 0.0
    last_deep: float = 0.0
    chain: dict = field(default_factory=dict)          # last on-chain check
    chain_t: float = 0.0

    def on_trade(self, t: float, m: dict) -> Optional[float]:
        """Returns tokens sold if this trade was a DEV SELL, else None."""
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
            if who == self.creator:
                self.dev_max = max(self.dev_max, bal)
        dev_sell = None
        if side == "buy":
            self.n_buys += 1
            if who and who != self.creator:
                self.buyers.add(who)
                self.first_buy.setdefault(who, t - self.t0)
        else:
            self.n_sells += 1
            if who == self.creator:
                self.dev_sold, dev_sell = True, tokens
        self.flow.append((t, side, sol))
        self.last_trade = t
        return dev_sell

    def mcap_at(self, t):
        best = None
        for ts, mc in self.hist:
            if ts <= t:
                best = mc
            else:
                break
        return best

    def flow_since(self, t0):
        b = s = 0.0
        for t, side, sol in reversed(self.flow):
            if t < t0:
                break
            if side == "buy":
                b += sol
            else:
                s += sol
        return b, s

    def group_pct(self, lo_s, hi_s) -> float:
        """% of supply still held by wallets whose first buy came lo_s..hi_s seconds after launch (dev excluded)."""
        tok = sum(self.balances.get(w, 0.0) for w, dt in self.first_buy.items() if lo_s <= dt <= hi_s)
        return tok / 1e9 * 100

    def holders(self) -> int:
        return sum(1 for w, b in self.balances.items() if b > 0 and w != self.curve)


def launch_rug_reason(l: Launch, cfg: Config) -> Optional[str]:
    if l.rugged or not l.dev_sold or l.dev_max <= 0 or not l.mc0 or not l.peak_sol:
        return None
    if (l.balances.get(l.creator, 0.0) <= l.dev_max * (1 - cfg.rug_dev_sold_share) and l.peak_sol >= l.mc0 * cfg.rug_min_pump
            and l.mcap_sol <= l.peak_sol * (1 - cfg.rug_price_drop) and len(l.buyers) >= cfg.rug_min_victims):
        return f"dev dumped, MC -{(1 - l.mcap_sol / l.peak_sol) * 100:.0f}% from peak"
    return None


def curve_observation(l: Launch, now: float, sol_usd: float, cfg: Config, blacklisted: bool, launches_24h: int) -> dict:
    """Score + checks for a bonding-curve coin (§5.3: liquidity / LP are n/a here, never a reason to reject)."""
    chain = l.chain or {}
    mi, hs, cs = chain.get("mint"), chain.get("holders") or {}, chain.get("curve") or {}
    mc = l.mcap_sol * sol_usd
    b5, s5 = l.flow_since(now - 300)
    bh, sh = l.flow_since(now - 3600)
    past5, past60 = l.mcap_at(now - 300), l.mcap_at(now - 3600) or l.mc0
    g5 = (l.mcap_sol / past5 - 1) * 100 if past5 else None
    g60 = (l.mcap_sol / past60 - 1) * 100 if past60 else None
    dev = hs.get("dev")
    if l.creator in l.balances:
        fd = l.balances[l.creator] / 1e9 * 100
        dev = fd if dev is None else max(dev, fd)
    bundler = l.group_pct(0, cfg.bundle_window_s)
    sniper = l.group_pct(cfg.bundle_window_s + 1e-9, cfg.sniper_window_s)
    holders, top10 = l.holders(), hs.get("top10")
    uniq = len(l.buyers) / l.n_buys if l.n_buys else None
    metrics = {"age_s": now - l.t0, "vol_m5": (b5 + s5) * sol_usd, "vol_h1": (bh + sh) * sol_usd, "chg_m5": g5, "chg_h1": g60,
               "buys": l.n_buys, "sells": l.n_sells, "buyers": len(l.buyers), "holders": holders, "top10": top10,
               "top10_warn": top10 is not None and cfg.top10_warn_pct < top10 <= cfg.max_top10_pct,
               "dev": dev, "insiders": None, "bundler": bundler, "sniper": sniper}
    # ---- hard safety (rejects even early-watch)
    hard = []
    if mi is None:
        hard.append("authority check n/a")
    elif not mi.get("revoked"):
        hard.append("mint/freeze authority on")
    elif mi.get("bad_ext"):
        hard.append("risky token settings")
    if blacklisted:
        hard.append("dev blacklisted (past rug)")
    if launches_24h > cfg.max_creator_launches_24h:
        hard.append(f"dev launched {launches_24h} coins in 24h")
    # ---- quality gates (§5.1 values that apply on the curve)
    gates = []
    if top10 is None:
        gates.append("top 10 n/a")
    elif top10 > cfg.max_top10_pct:
        gates.append(f"top 10 {top10:.0f}%")
    if dev is not None and dev > cfg.max_dev_holdings_pct:
        gates.append(f"dev holds {dev:.1f}%")
    if bundler > cfg.max_bundler_pct:
        gates.append(f"bundlers {bundler:.0f}%")
    if sniper > cfg.max_sniper_pct:
        gates.append(f"snipers {sniper:.0f}%")
    checks = [line(mi.get("revoked") if mi else None, "Mint & freeze revoked"),
              line((not mi.get("bad_ext")) if mi else None, "No risky token settings"),
              line(None if top10 is None else top10 <= cfg.max_top10_pct, f"Top 10 {pfmt(top10)}" + (" (30–40% warn)" if metrics["top10_warn"] else "")),
              line(None if dev is None else dev <= cfg.max_dev_holdings_pct, f"Dev holds {pfmt(dev)}" + (" (sold some)" if l.dev_sold else "")),
              line(bundler <= cfg.max_bundler_pct, f"Bundlers {pfmt(bundler)}"),
              line(sniper <= cfg.max_sniper_pct, f"Snipers {pfmt(sniper)}"),
              line(not blacklisted, "Dev not blacklisted"),
              "❔ Liquidity / LP: n/a on the bonding curve"]
    score = round(wavg([(ramp(len(l.buyers), 20, 150), 20), (ramp(uniq, 0.3, 0.8), 10),
                        (ramp(b5 / s5 if s5 else (3 if b5 else 0), 1.0, 3.0), 15), (ramp(g5, 0, 50), 15),
                        (ramp(-(top10 or 100), -40, -12), 15), (ramp(-(dev or 0), -8, 0), 10),
                        (ramp(-(bundler + sniper), -40, 0), 15)]))
    momentum = (cfg.curve_min_mcap <= mc <= cfg.curve_max_mcap and l.peak_sol and l.mcap_sol >= l.peak_sol * (1 - cfg.curve_max_drawdown)
                and b5 >= s5 * cfg.curve_min_buy_sell_5m and b5 > 0 and (now - l.t0) / 60 >= cfg.curve_min_age_min)
    breakout = bool(g5 is not None and g5 >= 30 and b5 >= 2 * s5 and momentum)
    reasons = [f"{len(l.buyers)} unique buyers ({uniq * 100:.0f}% unique)" if uniq else None,
               f"MC {g5:+.0f}% in 5m" if g5 is not None else None,
               f"5m flow {b5:.1f} SOL in / {s5:.1f} out",
               f"curve {cs['progress'] * 100:.0f}% · reserve {cs['reserve_sol']:.1f} SOL" if cs else None]
    return {"ca": l.mint, "ticker": l.symbol, "name": l.name, "source": "curve", "price": mc / 1e9 if mc else None,
            "price_sol": l.mcap_sol / 1e9, "mc": mc, "liq": None, "pool": l.curve, "pool_dex": "pumpfun", "curve_key": l.curve,
            "dev_wallet": l.creator, "dev_balance": l.balances.get(l.creator, 0.0) * 1e6, "metrics": metrics, "checks": checks,
            "reasons": [r for r in reasons if r], "hard_fails": hard, "gate_fails": gates, "raw_score": score,
            "bundle_known": True, "momentum": bool(momentum), "breakout": breakout,
            "breakout_reasons": [f"MC {g5:+.0f}% in 5m with {b5:.1f} SOL bought vs {s5:.1f} sold"] if breakout else [],
            "safety": {"fails": real_flags(hard + [g for g in gates if g.startswith(("bundlers", "snipers"))])}}


def real_flags(fails: list) -> list:
    """Only definite failures count as a 'new safety flag' (a lookup that returned n/a is not news)."""
    return [f for f in fails if "n/a" not in f and "unknown" not in f]


# ======================================================================= DEX pairs
@dataclass
class Pair:
    mint: str
    symbol: str = "?"
    name: str = ""
    pair: str = ""
    dex: str = ""
    created_ms: Optional[float] = None
    snap: dict = field(default_factory=dict)
    peak_price: float = 0.0
    misses: int = 0
    next_refresh: float = 0.0
    last_eval: float = 0.0
    rc: Optional[dict] = None
    rc_t: float = 0.0
    chain: dict = field(default_factory=dict)
    chain_t: float = 0.0
    creator: str = ""
    liq_hist: deque = field(default_factory=lambda: deque(maxlen=60))
    rugged: bool = False
    x_url: Optional[str] = None


def pair_snap(p: dict, t: float) -> dict:
    tx, vol, chg = p.get("txns") or {}, p.get("volume") or {}, p.get("priceChange") or {}
    h1, m5 = tx.get("h1") or {}, tx.get("m5") or {}
    return {"t": t, "price": num(p.get("priceUsd")), "mcap": num(p.get("marketCap") or p.get("fdv")),
            "liq": num((p.get("liquidity") or {}).get("usd")), "vol_m5": num(vol.get("m5")), "vol_h1": num(vol.get("h1")),
            "buys_h1": h1.get("buys") or 0, "sells_h1": h1.get("sells") or 0, "trades_m5": (m5.get("buys") or 0) + (m5.get("sells") or 0),
            "chg_m5": num(chg.get("m5")), "chg_h1": num(chg.get("h1"))}


def pump_migrated(mint: str, dex: str) -> bool:
    return mint.endswith("pump") and "pumpswap" in (dex or "").lower()


def breakout_trigger(p: Pair, now: float, cfg: Config) -> list:
    """Existing breakout trigger reasons ([] = not breaking out)."""
    s = p.snap
    mc, liq, vol = s.get("mcap") or 0, s.get("liq") or 0, s.get("vol_h1") or 0
    b, sl, m5, h1 = s.get("buys_h1") or 0, s.get("sells_h1") or 0, s.get("chg_m5"), s.get("chg_h1")
    age_min = (now * 1000 - p.created_ms) / 60000 if p.created_ms else None
    if not mc or age_min is None or age_min < 10 or m5 is None or h1 is None:
        return []
    if (liq / mc >= cfg.breakout_min_liq_mcap and vol / mc >= cfg.breakout_min_vol_mcap and b >= sl * cfg.breakout_min_buy_sell_h1
            and m5 >= cfg.breakout_min_chg_m5 and cfg.breakout_min_chg_h1 <= h1 <= cfg.breakout_max_chg_h1 and m5 * 12 >= h1):
        return [f"liquidity {liq / mc * 100:.0f}% of MC", f"1h volume {vol / mc:.2f}x MC", f"buys/sells {b}/{sl} in 1h",
                f"price {m5:+.0f}% in 5m, {h1:+.0f}% in 1h (accelerating)"]
    return []


def dex_observation(p: Pair, now: float, cfg: Config, blacklisted: bool, launch_info: Optional[dict]) -> dict:
    """§5.1 hard filters + score for a coin trading in a real pool."""
    s, rc, mi, hs = p.snap, p.rc or {}, p.chain.get("mint"), p.chain.get("holders") or {}
    mc, liq = s.get("mcap"), s.get("liq")
    burned = pump_migrated(p.mint, p.dex)
    lp = 100.0 if burned else rc.get("lp_locked")
    lp_eff = lp if lp is not None else 0.0                          # unknown lock = 0 (§5.1)
    holders = rc.get("holders")
    top10 = rc.get("top10") if rc.get("top10") is not None else hs.get("top10")
    dev = rc.get("dev") if rc.get("dev") is not None else hs.get("dev")
    insiders = rc.get("insiders")
    li = launch_info or {}
    bundler, sniper = li.get("bundler"), li.get("sniper")
    revoked = mi.get("revoked") if mi else rc.get("revoked")
    dangers = [d for d in rc.get("dangers") or [] if not (burned and "lp" in d.lower() and "unlock" in d.lower())]
    age_s = now - p.created_ms / 1000 if p.created_ms else None
    metrics = {"age_s": age_s, "vol_m5": s.get("vol_m5"), "vol_h1": s.get("vol_h1"), "chg_m5": s.get("chg_m5"), "chg_h1": s.get("chg_h1"),
               "buys": s.get("buys_h1"), "sells": s.get("sells_h1"), "buyers": li.get("buyers"), "holders": holders, "top10": top10,
               "top10_warn": top10 is not None and cfg.top10_warn_pct < top10 <= cfg.max_top10_pct, "dev": dev,
               "insiders": insiders, "bundler": bundler, "sniper": sniper}
    hard = []
    if revoked is None:
        hard.append("authority check n/a")
    elif not revoked:
        hard.append("mint/freeze authority on")
    if mi and mi.get("bad_ext"):
        hard.append("risky token settings")
    if rc.get("rugged"):
        hard.append("RugCheck: rugged")
    if dangers:
        hard.append("RugCheck danger: " + ", ".join(dangers[:2]))
    if blacklisted:
        hard.append("dev blacklisted (past rug)")
    gates = []
    if liq is None or liq < cfg.min_liquidity_usd:
        gates.append(f"liquidity {M.usd(liq)}")
    if lp_eff < cfg.min_lp_locked_or_burned_pct:
        gates.append("LP lock unknown" if lp is None else f"LP locked {lp:.0f}%")
    if top10 is None or top10 > cfg.max_top10_pct:
        gates.append("top 10 n/a" if top10 is None else f"top 10 {top10:.0f}%")
    if dev is not None and dev > cfg.max_dev_holdings_pct:
        gates.append(f"dev holds {dev:.1f}%")
    if bundler is not None and bundler > cfg.max_bundler_pct:
        gates.append(f"bundlers {bundler:.0f}%")
    if sniper is not None and sniper > cfg.max_sniper_pct:
        gates.append(f"snipers {sniper:.0f}%")
    if holders is None or holders < cfg.min_holders:
        gates.append("holders n/a" if holders is None else f"{holders} holders")
    checks = [line(None if not rc else (not rc.get("rugged") and not dangers), "RugCheck clean" if rc and not dangers else
                   ("RugCheck: " + ", ".join(dangers[:2]) if dangers else "RugCheck n/a")),
              line(revoked, "Mint & freeze revoked"),
              line((not mi.get("bad_ext")) if mi else None, "No risky token settings"),
              line(lp_eff >= cfg.min_lp_locked_or_burned_pct, "LP burned (pump.fun migration)" if burned else
                   (f"LP locked {lp:.0f}%" if lp is not None else "LP lock unknown (counted as 0%)")),
              line(None if liq is None else liq >= cfg.min_liquidity_usd, f"Liquidity {M.usd(liq)}"),
              line(None if holders is None else holders >= cfg.min_holders, f"{holders} holders" if holders is not None else "Holders n/a"),
              line(None if top10 is None else top10 <= cfg.max_top10_pct, f"Top 10 {pfmt(top10)}" + (" (30–40% warn)" if metrics["top10_warn"] else "")),
              line(True if dev is None else dev <= cfg.max_dev_holdings_pct, f"Dev holds {pfmt(dev)}" if dev is not None else "Dev n/a"),
              line(True if bundler is None else bundler <= cfg.max_bundler_pct, f"Bundlers {pfmt(bundler)}" if bundler is not None else "Bundlers n/a (score capped at 70)"),
              line(True if sniper is None else sniper <= cfg.max_sniper_pct, f"Snipers {pfmt(sniper)}" if sniper is not None else "Snipers n/a"),
              line(not blacklisted, "Dev not blacklisted")]
    vol, b, sl = s.get("vol_h1") or 0, s.get("buys_h1") or 0, s.get("sells_h1") or 0
    h1 = s.get("chg_h1")
    score = round(wavg([(ramp((liq or 0) / mc if mc else None, 0.03, 0.20), 15), (ramp(vol / mc if mc else None, 0.1, 1.5), 15),
                        (ramp(b / (b + sl) if b + sl else None, 0.48, 0.68), 15), (ramp(holders, 40, 800), 15),
                        (ramp(-(top10 if top10 is not None else 60), -45, -15), 15),
                        (None if h1 is None else (0.0 if h1 > 200 else ramp(h1, 0, 60)), 10), (ramp(s.get("chg_m5"), -5, 15), 5),
                        (ramp(-(dev or 0) - (insiders or 0), -10, 0), 10)]))
    trig = breakout_trigger(p, now, cfg)
    reasons = trig or [f"liquidity {liq / mc * 100:.0f}% of MC" if (liq and mc) else None,
                       f"1h volume {vol / mc:.2f}x MC" if mc else None, f"buys/sells {b}/{sl} in 1h",
                       f"price {M.pct(s.get('chg_m5'))} in 5m, {M.pct(h1)} in 1h"]
    return {"ca": p.mint, "ticker": p.symbol, "name": p.name, "source": "dex", "price": s.get("price"), "mc": mc, "liq": liq,
            "pool": p.pair, "pool_dex": p.dex, "dev_wallet": p.creator or None, "metrics": metrics, "checks": checks,
            "reasons": [r for r in reasons if r], "hard_fails": hard, "gate_fails": gates, "raw_score": score,
            "bundle_known": bundler is not None and sniper is not None, "momentum": b > sl and (h1 or 0) > 0,
            "breakout": bool(trig), "breakout_reasons": trig, "x_url": p.x_url,
            "safety": {"fails": real_flags(hard + [g for g in gates if g.startswith(("bundlers", "snipers", "LP locked"))]),
                       "lp": lp, "revoked": revoked}}


def finalize(obs: dict, cfg: Config, catalyst_tag: str, confluence: list, existing: Optional[Record]) -> dict:
    """Caps + boosts -> final score, then which channel it qualifies for (§5.1, §5.3, §5.4, §7, §8)."""
    raw = obs["raw_score"]
    parts, score = [str(raw)], raw
    if not obs.get("bundle_known") and score > cfg.score_cap_without_bundle_data:
        score = cfg.score_cap_without_bundle_data
        parts = [f"{raw} capped {score} (bundler n/a)"]
    if catalyst_tag == "REAL_CATALYST":
        score += cfg.catalyst_boost
        parts.append(f"+{cfg.catalyst_boost} catalyst")
    elif catalyst_tag == "HYPE_ONLY" and cfg.hype_score_penalty:
        score -= cfg.hype_score_penalty
        parts.append(f"-{cfg.hype_score_penalty} hype only")
    if confluence:
        boost = min(cfg.confluence_boost_cap, cfg.confluence_boost_per_source * len(confluence))
        score += boost
        parts.append(f"+{boost} confluence")
    score = max(0, min(100, int(round(score))))
    obs["score"] = score
    obs["score_parts"] = " ".join(parts) + f" = {score}" if len(parts) > 1 else None
    obs["catalyst_tag"], obs["confluence"] = catalyst_tag, confluence
    safe = not obs["hard_fails"]
    clean = safe and not obs["gate_fails"]
    if obs["source"] == "curve":
        buyers = obs["metrics"].get("buyers") or 0
        signals = clean and obs["momentum"] and score >= cfg.curve_signals_min_score and buyers >= cfg.curve_min_unique_buyers \
            and (obs["metrics"].get("holders") or 0) >= cfg.min_holders
        early = safe and obs["momentum"] and score >= cfg.early_min_score and (obs["metrics"].get("holders") or 0) >= cfg.early_min_holders
    else:
        signals = clean and obs["momentum"] and score >= cfg.signals_min_score and (obs["breakout"] or score >= cfg.promote_score)
        liq, holders = obs.get("liq") or 0, obs["metrics"].get("holders") or 0
        early = safe and obs["momentum"] and score >= cfg.early_min_score and liq >= cfg.early_min_liquidity_usd and holders >= cfg.early_min_holders
    obs["qualifies_signals"], obs["qualifies_early"] = bool(signals), bool(early) and not signals
    obs["qualifies_promotion"] = bool(signals) and score >= (cfg.curve_signals_min_score if obs["source"] == "curve" else cfg.promote_score)
    return obs


# ======================================================================= deep checks (worker threads)
def deep_curve(mint, curve, creator) -> dict:
    mi = mint_info(mint)
    supply = (mi or {}).get("supply") or PUMP_SUPPLY_RAW
    return {"mint": mi, "curve": curve_state(curve), "holders": holder_stats(largest_holders(mint, exclude_owners=[curve]), supply, creator)}


def deep_dex(mint, pair_addr, cached_rc, creator) -> dict:
    out = {"mint": mint_info(mint), "rc": cached_rc or fetch_rugcheck(mint), "rc_fresh": cached_rc is None}
    if not out["rc"] or out["rc"].get("top10") is None:
        supply = (out["mint"] or {}).get("supply")
        if supply:
            out["holders"] = holder_stats(largest_holders(mint, exclude_owners=[pair_addr]), supply, creator or None)
    return out


def safety_recheck(rec_ca, dev, full, is_dex) -> dict:
    out = {}
    if dev:
        out["dev_balance"] = token_balance(dev, rec_ca)
    if full:
        out["mint"] = mint_info(rec_ca)
        if is_dex:
            out["rc"] = fetch_rugcheck(rec_ca)
    return out


# ======================================================================= bot
class Bot:
    def __init__(self, cfg: Config, poster: Poster, feed: Optional[PumpFeed] = None, clock=time.time):
        self.cfg, self.clock, self.poster, self.feed = cfg, clock, poster, feed
        self.store = Store(cfg.state_file, cfg.backup_dir, cfg.backup_keep_days, clock=clock)
        self.store.load()
        self.lc = Lifecycle(self.store, cfg, poster, clock=clock, on_end=self.on_end)
        self.launches: dict = {}
        self.pairs: dict = {}
        self.launch_info: dict = {}          # mint -> {bundler, sniper, buyers, creator, t} (snapshot from the feed)
        self.mint_creator: dict = {}         # mint -> (dev, t)
        self.seen: dict = {}
        self.pending: dict = {}
        self.trending: dict = {}             # mint -> last time seen in GeckoTerminal trending (public confluence)
        self.pool = ThreadPoolExecutor(max_workers=5)
        self.sol_usd, self.sol_t = None, 0.0
        self.timers = defaultdict(float)
        self.blocks, self.counts = Counter(), Counter()
        self.status_lines = deque(maxlen=60)
        self.started = clock()
        self.last_tick = clock()
        self.last_poll_ok = self.store.meta.get("last_poll_ok")
        self.gap_warned = False
        self.flap_warned_at = 0.0
        self.creator_times = defaultdict(deque, {k: deque(v) for k, v in self.store.creator_times.items()})
        for r in self.store.records.values():                # resume UPDATE/BREAKOUT checks for live coins after a restart
            if r.status == "active" and (r.source == "dex" or r.migrated_at):
                self.add_pair(r.ca, clock(), force=True)
                self.pairs[r.ca].creator = r.dev_wallet or ""

    # ---------------- utils
    def every(self, key, seconds, now) -> bool:
        if now - self.timers[key] >= seconds:
            self.timers[key] = now
            return True
        return False

    def submit(self, key, kind, fn, *args):
        self.pending[key] = (kind, self.pool.submit(fn, *args))

    def log(self, event, symbol, mint, detail=""):
        new = not os.path.exists(self.cfg.log_file)
        with open(self.cfg.log_file, "a", newline="") as f:
            w = csv.writer(f)
            if new:
                w.writerow(["time", "event", "symbol", "mint", "price", "detail"])
            w.writerow([time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()), event, symbol, mint, "", detail])

    def jlog(self, rec: dict):
        with open(self.cfg.signals_file, "a") as f:
            f.write(json.dumps(rec, default=str) + "\n")

    def block(self, reason):
        self.blocks[re.sub(r"\$?\d[\d.,]*[KMB%]?", "N", reason)[:48]] += 1

    def creator_launches(self, creator, now) -> int:
        dq = self.creator_times.get(creator)
        if not dq:
            return 0
        while dq and dq[0] < now - 86400:
            dq.popleft()
        return len(dq)

    def blacklisted(self, dev) -> bool:
        return bool(dev) and dev in self.store.blacklist

    def blacklist_dev(self, dev, reason, mint, symbol):
        if not dev or dev in self.store.blacklist:
            return
        self.store.blacklist[dev] = [self.clock(), f"{symbol}: {reason}"[:80]]
        self.counts["blacklisted"] += 1
        self.log("BLACKLIST", symbol, mint, f"{dev} | {reason}")
        # any live signal from this dev is now invalid
        for rec in list(self.store.records.values()):
            if rec.dev_wallet == dev and not rec.ended and rec.status == "active" and rec.ca != mint:
                self.lc.invalidate(rec.ca, "dev_blacklisted", f"Dev rugged {symbol} ({reason}).")

    def on_end(self, rec: Record, reason: str, detail: str):
        self.log(f"{'INVALIDATED' if rec.status == 'invalidated' else 'DEAD'}", rec.ticker, rec.ca, f"{reason} {detail}".strip())
        self.jlog({"type": "outcome", "t": self.clock(), "ca": rec.ca, "ticker": rec.ticker, "official": rec.official,
                   "reason": reason, "peak_x": rec.peak_multiple})
        if reason in ("dev_sold", "liquidity_pulled", "safety_fail") and rec.dev_wallet:
            self.blacklist_dev(rec.dev_wallet, DEATH_TEXT.get(reason, reason), rec.ca, rec.ticker)
        if self.feed and rec.ca in self.launches:
            self.feed.unwatch([rec.ca])

    def to_bot(self, text: str, title: str = "Spidey Bot"):
        self.poster.send("bot", {"title": title, "description": text[:4000], "color": 0x747F8D})

    # ---------------- feed (curve coins)
    def on_feed(self, events):
        subs, unsubs = [], []
        for t, kind, mint, m in events:
            if kind == "launch":
                creator = m.get("traderPublicKey") or ""
                if creator:
                    self.creator_times[creator].append(t)
                    self.mint_creator[mint] = (creator, t)
                if mint in self.launches or len(self.launches) >= self.cfg.max_trade_subs:
                    continue
                l = Launch(mint=mint, name=str(m.get("name") or "?")[:60], symbol=str(m.get("symbol") or "?")[:20],
                           creator=creator, curve=m.get("bondingCurveKey") or "", t0=t, last_trade=t)
                mc = num(m.get("marketCapSol")) or 0.0
                l.mcap_sol = l.peak_sol = l.mc0 = mc
                l.hist.append((t, mc))
                init = num(m.get("initialBuy")) or 0.0
                if creator and init > 0:
                    l.balances[creator] = l.dev_max = init
                self.launches[mint] = l
                self.counts["launches"] += 1
                subs.append(mint)
            elif kind == "trade":
                l = self.launches.get(mint)
                if not l:
                    continue
                sold = l.on_trade(t, m)
                rec = self.store.get(mint)
                if sold and rec and not rec.ended and rec.status == "active" and self.cfg.dev_sell_invalidates:
                    pct = sold / 1e9 * 100
                    if pct >= self.cfg.dev_sell_dust_pct:
                        self.lc.invalidate(mint, "dev_sold", f"Dev wallet sold {pct:.2f}% of supply on the curve.")
            elif kind == "migration":
                self.counts["migrations"] += 1
                l = self.launches.pop(mint, None)
                if l:
                    self.snapshot_launch(l, t)
                    unsubs.append(mint)
                self.add_pair(mint, t, force=True)
        if self.feed:
            self.feed.watch(subs)
            self.feed.unwatch(unsubs)

    def snapshot_launch(self, l: Launch, t: float):
        self.launch_info[l.mint] = {"bundler": l.group_pct(0, self.cfg.bundle_window_s),
                                    "sniper": l.group_pct(self.cfg.bundle_window_s + 1e-9, self.cfg.sniper_window_s),
                                    "buyers": len(l.buyers), "creator": l.creator, "t": t}

    def prune(self, now):
        for mint, l in self.launches.items():
            why = launch_rug_reason(l, self.cfg)
            if why:
                l.rugged = True
                self.blacklist_dev(l.creator, why, mint, l.symbol)
        if self.every("prune_maps", 600, now):
            self.mint_creator = {m: v for m, v in self.mint_creator.items() if v[1] >= now - 86400}
            self.launch_info = {m: v for m, v in self.launch_info.items() if v["t"] >= now - 86400}
            self.trending = {m: t for m, t in self.trending.items() if t >= now - 86400}
            self.seen = {m: t for m, t in self.seen.items() if t > now}
        drop = []
        for mint, l in self.launches.items():
            rec = self.store.get(mint)
            if (rec and rec.status == "active") or mint in self.pending:
                continue                                              # keep watching trades of live signals (dev sells)
            age = (now - l.t0) / 60
            mc = l.mcap_sol * (self.sol_usd or 0)
            if (age > self.cfg.curve_max_age_min or now - l.last_trade > 600
                    or (age > 5 and (len(l.buyers) < 8 or (self.sol_usd and mc < 5_000)))):
                drop.append(mint)
        for m in drop:
            l = self.launches.pop(m)
            if len(l.buyers) >= 20:
                self.snapshot_launch(l, now)
        if self.feed and drop:
            self.feed.unwatch(drop)

    # ---------------- DEX pairs
    def add_pair(self, mint, now, force=False):
        if mint in STABLES or mint in self.pairs:
            return
        rec = self.store.get(mint)
        if rec and (rec.ended or rec.status == "archived"):
            return
        if not force and self.seen.get(mint, 0) > now:
            return
        if len(self.pairs) >= self.cfg.max_pairs:
            return
        self.pairs[mint] = Pair(mint=mint, next_refresh=now, creator=(self.mint_creator.get(mint) or ("", 0))[0])

    def drop_pair(self, mint, now, ttl):
        self.pairs.pop(mint, None)
        self.seen[mint] = now + ttl

    def discover(self, now):
        if self.every("gecko_new", 30, now):
            for m in fetch_gecko_mints("new_pools"):
                self.add_pair(m, now)
        if self.every("gecko_trend", 120, now):
            for m in fetch_gecko_mints("trending_pools"):
                self.trending[m] = now
                self.add_pair(m, now)
        if self.every("dex_profiles", 30, now):
            for m in fetch_dex_feed("/token-profiles/latest/v1"):
                self.add_pair(m, now)
        if self.every("dex_boosts", 60, now):
            for m in fetch_dex_feed("/token-boosts/latest/v1"):
                self.add_pair(m, now)

    def refresh_pairs(self, now):
        due = sorted((p for p in self.pairs.values() if p.next_refresh <= now), key=lambda p: p.next_refresh)[:120]
        for i in range(0, len(due), 30):
            batch = due[i:i + 30]
            res = fetch_dex_all_pairs([p.mint for p in batch])
            if res is None:
                return
            self.poll_ok(now)
            for p in batch:
                p.next_refresh = now + self.cfg.pair_refresh_s
                pairs = res.get(p.mint) or []
                real = [d for d in pairs if (d.get("dexId") or "").lower() != "pumpfun"]
                if not pairs:
                    p.misses += 1
                    if p.misses >= 4:
                        self.drop_pair(p.mint, now, 1800)
                    continue
                if not real:                                          # still on the curve: feed lane's job
                    self.drop_pair(p.mint, now, 900)
                    continue
                d = next((x for x in real if x.get("pairAddress") == p.pair), None) or \
                    max(real, key=lambda x: num((x.get("liquidity") or {}).get("usd")) or 0)
                p.misses = 0
                p.symbol = (d.get("baseToken") or {}).get("symbol") or p.symbol
                p.name = (d.get("baseToken") or {}).get("name") or p.name
                p.pair, p.dex = d.get("pairAddress") or p.pair, d.get("dexId") or p.dex
                p.created_ms = num(d.get("pairCreatedAt")) or p.created_ms
                socials = ((d.get("info") or {}).get("socials") or [])
                p.x_url = next((s.get("url") for s in socials if s.get("type") == "twitter"), p.x_url)
                p.snap = pair_snap(d, now)
                p.liq_hist.append((now, p.snap.get("liq") or 0.0))
                rec = self.store.get(p.mint)
                if not rec and not p.rugged:
                    why = self.liq_rug(p, now)
                    if why:
                        p.rugged = True
                        self.pair_rugged(p, why, now)
                        continue
                p.peak_price = max(p.peak_price, p.snap.get("price") or 0)
                age_h = (now * 1000 - p.created_ms) / 3.6e6 if p.created_ms else 0
                if not rec and (age_h > self.cfg.max_pair_age_h or (p.snap.get("liq") or 0) < 1_000
                                or (p.snap.get("mcap") or 0) > self.cfg.max_mcap_usd):
                    self.drop_pair(p.mint, now, 6 * 3600)

    def liq_rug(self, p: Pair, now) -> Optional[str]:
        recent = [liq for t, liq in p.liq_hist if t >= now - self.cfg.rug_liq_window_s]
        if len(recent) >= 2 and max(recent) >= self.cfg.rug_min_liq and recent[-1] <= max(recent) * (1 - self.cfg.rug_liq_drop):
            return f"liquidity drained ({M.usd(max(recent))} -> {M.usd(recent[-1])})"
        return None

    def pair_rugged(self, p: Pair, why, now):
        dev = p.creator or (p.rc or {}).get("creator")
        if dev:
            self.blacklist_dev(dev, why, p.mint, p.symbol)
            self.drop_pair(p.mint, now, 86400)
        elif p.mint not in self.pending:
            self.submit(p.mint, "rugdev", fetch_rugcheck, p.mint)

    # ---------------- evaluation
    def catalyst_for(self, rec: Optional[Record]) -> str:
        return rec.catalyst_tag if rec else "UNCHECKED"

    def confluence_for(self, ca, now, rec: Optional[Record]) -> list:
        t = self.trending.get(ca)
        start = (rec.first_alert_at or rec.early_alert_at) if rec else now
        if t and t >= (start or now) - self.cfg.confluence_lookback_hours * 3600:
            return ["GeckoTerminal trending"]
        return list(rec.confluence) if rec else []

    def evaluate(self, now):
        sol = self.sol_usd
        if sol:
            for l in list(self.launches.values()):
                if l.mint in self.pending or now - l.last_eval < 20:
                    continue
                rec = self.store.get(l.mint)
                if rec and (rec.ended or rec.status == "archived"):
                    continue
                l.last_eval = now
                age = (now - l.t0) / 60
                mc = l.mcap_sol * sol
                if age < self.cfg.curve_min_age_min or not (self.cfg.curve_min_mcap <= mc <= self.cfg.curve_max_mcap * 1.5):
                    continue
                if len(l.buyers) < 15 and not rec:
                    continue
                if l.curve and now - l.chain_t >= (self.cfg.safety_recheck_s if rec else 60):
                    l.chain_t = now
                    self.submit(l.mint, "curve", deep_curve, l.mint, l.curve, l.creator)
                    continue
                if l.chain:
                    self.consider_curve(l, now)
        for p in list(self.pairs.values()):
            if p.mint in self.pending or not p.snap or now - p.last_eval < 60:
                continue
            rec = self.store.get(p.mint)
            if rec and (rec.ended or rec.status == "archived"):
                self.pairs.pop(p.mint, None)
                continue
            p.last_eval = now
            s = p.snap
            cheap_ok = (s.get("liq") or 0) >= self.cfg.early_min_liquidity_usd and (s.get("buys_h1") or 0) > (s.get("sells_h1") or 0) \
                and (s.get("chg_h1") or 0) > 0 and (s.get("vol_h1") or 0) >= 5_000
            if not cheap_ok and not rec:
                continue
            fresh_rc = p.rc if (p.rc is not None and now - p.rc_t <= 180) else None
            if fresh_rc is None or now - p.chain_t > 180:
                p.chain_t = now
                self.submit(p.mint, "dex", deep_dex, p.mint, p.pair, fresh_rc, p.creator)
                continue
            self.consider_dex(p, now)

    def consider_curve(self, l: Launch, now):
        rec = self.store.get(l.mint)
        obs = curve_observation(l, now, self.sol_usd, self.cfg, self.blacklisted(l.creator), self.creator_launches(l.creator, now))
        self.decide(obs, rec, now)

    def consider_dex(self, p: Pair, now):
        rec = self.store.get(p.mint)
        if not p.creator:
            p.creator = (p.rc or {}).get("creator") or (self.mint_creator.get(p.mint) or ("", 0))[0]
        obs = dex_observation(p, now, self.cfg, self.blacklisted(p.creator), self.launch_info.get(p.mint))
        self.decide(obs, rec, now)

    def decide(self, obs, rec, now):
        obs = finalize(obs, self.cfg, self.catalyst_for(rec), self.confluence_for(obs["ca"], now, rec), rec)
        if rec is None and (obs["qualifies_signals"] or obs["qualifies_early"]) and obs["ca"] not in self.pending:
            # first sighting worth posting: do the catalyst search first (fail-soft: UNCHECKED if it errors)
            self.submit(obs["ca"], "catalyst_new", lambda o=obs: (o, check_catalyst(o.get("name") or "", o.get("ticker") or "")))
            return
        if rec is None:
            if obs["hard_fails"]:
                self.block(obs["hard_fails"][0])
            elif obs["gate_fails"]:
                self.block(obs["gate_fails"][0])
            return
        self.post_obs(obs, rec, now)

    def post_obs(self, obs, rec, now):
        typ = self.lc.on_candidate(obs)
        if typ:
            self.log(typ, obs["ticker"], obs["ca"], f"score {obs['score']}")
            self.jlog({"type": typ, "t": now, "ca": obs["ca"], "ticker": obs["ticker"], "score": obs["score"],
                       "official": bool(self.store.get(obs["ca"]) and self.store.get(obs["ca"]).official),
                       "mc": obs.get("mc"), "liq": obs.get("liq"), "checks": obs.get("checks")})
            rec = self.store.get(obs["ca"])
            if typ in ("UPDATE", "BREAKOUT") and rec and not rec.catalyst_rerun_done and rec.catalyst_tag != "REAL_CATALYST":
                rec.catalyst_rerun_done = True
                self.submit(f"cat:{rec.ca}", "catalyst", lambda o=obs: (o, check_catalyst(o.get("name") or "", o.get("ticker") or "")))

    def collect(self, now):
        for key, (kind, fut) in list(self.pending.items()):
            if not fut.done():
                continue
            del self.pending[key]
            try:
                res = fut.result()
            except Exception as e:
                print(f"[warn] {kind} job failed for {key}: {e}")
                continue
            if kind == "curve":
                l = self.launches.get(key)
                if l:
                    l.chain = res
                    if not (res.get("curve") or {}).get("complete"):
                        self.consider_curve(l, now)
            elif kind == "dex":
                p = self.pairs.get(key)
                if not p:
                    continue
                p.chain = res
                if res.get("rc_fresh") and res.get("rc") is not None:
                    p.rc, p.rc_t = res["rc"], now
                if (p.rc or {}).get("rugged"):
                    self.blacklist_dev(p.creator or p.rc.get("creator"), "RugCheck: rugged", p.mint, p.symbol)
                self.consider_dex(p, now)
            elif kind == "rugdev":
                p = self.pairs.get(key)
                if p and res and res.get("creator"):
                    self.blacklist_dev(res["creator"], "liquidity drained", key, p.symbol)
                self.drop_pair(key, now, 86400)
            elif kind == "catalyst_new":
                obs, (tag, url, summ) = res
                rec = self.store.get(obs["ca"])
                obs = finalize(obs, self.cfg, tag or "UNCHECKED", obs.get("confluence") or [], rec)
                obs["catalyst_url"], obs["catalyst_summary"] = url, summ
                if rec is None and not (obs["qualifies_signals"] or obs["qualifies_early"]):
                    continue
                self.post_obs(obs, rec, now)
            elif kind == "catalyst":
                obs, (tag, url, summ) = res
                obs["catalyst_tag"] = tag or obs.get("catalyst_tag")
                if self.lc.on_catalyst(obs["ca"], tag, url, summ, obs):
                    self.log("UPDATE", obs["ticker"], obs["ca"], f"catalyst {tag}")
            elif kind == "safety":
                self.apply_safety(key.split(":", 1)[1], res, now)

    # ---------------- monitoring live signals (§5.2)
    def monitor(self, now):
        live = [r for r in self.store.records.values() if r.status == "active" and not r.ended]
        due = [r for r in live if now - (r.last_polled_at or 0) >= (self.cfg.active_poll_s if r.official else self.cfg.early_poll_s)]
        for i in range(0, len(due), 30):
            batch = due[i:i + 30]
            res = fetch_dex_all_pairs([r.ca for r in batch])
            if res is None:
                break
            self.poll_ok(now)
            for r in batch:
                q = self.pick_quote(r, res.get(r.ca) or [], now)
                typ = self.lc.on_quote(r.ca, q)
                if typ:
                    self.log(typ, r.ticker, r.ca)
        for r in live:
            if r.dev_wallet and self.blacklisted(r.dev_wallet):
                self.lc.invalidate(r.ca, "dev_blacklisted", "Dev wallet is on the rug blacklist.")
                continue
            key = f"safety:{r.ca}"
            full = now - (r.safety_checked_at or 0) >= self.cfg.safety_recheck_s
            if key not in self.pending and (full or self.every(f"dev:{r.ca}", self.cfg.dev_check_s, now)):
                if full:
                    r.safety_checked_at = now
                self.submit(key, "safety", safety_recheck, r.ca, r.dev_wallet, full, bool(r.migrated_at or r.source == "dex"))
        # catalyst retries for UNCHECKED (60 min window, every 10 min)
        for r in live:
            if r.catalyst_tag == "UNCHECKED" and r.catalyst_first_try_at and now - r.catalyst_first_try_at <= self.cfg.catalyst_retry_minutes * 60:
                key = f"cat:{r.ca}"
                if key not in self.pending and self.every(key, 600, now):
                    o = {"ca": r.ca, "ticker": r.ticker, "name": r.name}
                    self.submit(key, "catalyst", lambda o=o: (o, check_catalyst(o["name"], o["ticker"])))

    def pick_quote(self, r: Record, pairs: list, now) -> Optional[dict]:
        def mk(d):
            dex = (d.get("dexId") or "").lower()
            s = pair_snap(d, now)
            return {"price": s["price"], "mc": s["mcap"], "liq": None if dex == "pumpfun" else s["liq"],
                    "pool": d.get("pairAddress"), "dex": dex, "traded": (s["trades_m5"] or 0) > 0}
        by = {d.get("pairAddress"): d for d in pairs}
        on_curve = (r.pool_dex or "").lower() == "pumpfun" and not r.migrated_at
        if r.pool in by and not (on_curve and any((d.get("dexId") or "").lower() != "pumpfun" for d in pairs)):
            return mk(by[r.pool])
        real = [d for d in pairs if (d.get("dexId") or "").lower() != "pumpfun"]
        if on_curve and real:                                        # migrated: switch the primary pool once
            return mk(max(real, key=lambda d: num((d.get("liquidity") or {}).get("usd")) or 0))
        if r.pool is None and pairs:
            return mk(max(pairs, key=lambda d: num((d.get("liquidity") or {}).get("usd")) or 0))
        l = self.launches.get(r.ca)
        if on_curve and l and self.sol_usd:                            # curve price straight from the live feed
            mc = l.mcap_sol * self.sol_usd
            return {"price": mc / 1e9, "mc": mc, "liq": None, "pool": r.pool, "dex": "pumpfun", "traded": now - l.last_trade < 120}
        if on_curve:
            return {"price": r.last_valid_price, "mc": r.last_valid_mc, "liq": None, "pool": r.pool, "dex": "pumpfun", "traded": False} \
                if r.last_valid_price else None
        return None

    def apply_safety(self, ca, res, now):
        rec = self.store.get(ca)
        if not rec or rec.ended or rec.status != "active":
            return
        mi, rc, bal = res.get("mint"), res.get("rc"), res.get("dev_balance")
        if mi is not None and (not mi.get("revoked") or mi.get("bad_ext")):
            self.lc.invalidate(ca, "authority_revived", "Mint/freeze authority or a risky token setting appeared after the alert.")
            return
        if rc:
            if rc.get("rugged"):
                self.lc.kill(ca, "safety_fail", "RugCheck marks this token as rugged.")
                return
            burned = pump_migrated(ca, rec.pool_dex or "")
            lp = rc.get("lp_locked")
            if not burned and lp is not None and lp < self.cfg.min_lp_locked_or_burned_pct and \
                    (rec.safety_snapshot.get("lp") or 0) >= self.cfg.min_lp_locked_or_burned_pct:
                self.lc.invalidate(ca, "lp_unlocked", f"LP lock fell to {lp:.0f}%.")
                return
        if bal is not None and rec.dev_wallet and self.cfg.dev_sell_invalidates:
            prev = rec.dev_balance
            supply = (mi or {}).get("supply") or PUMP_SUPPLY_RAW
            if prev is not None and bal < prev and (prev - bal) / supply * 100 >= self.cfg.dev_sell_dust_pct:
                self.lc.invalidate(ca, "dev_sold", f"Dev wallet balance dropped {(prev - bal) / supply * 100:.2f}% of supply.")
                return
            if prev is None or bal > prev:
                rec.dev_balance = bal                 # baseline; small sells accumulate until they pass the dust limit

    # ---------------- ops (§6)
    def poll_ok(self, now):
        self.last_poll_ok = now
        self.store.meta["last_poll_ok"] = now
        if self.gap_warned:
            self.gap_warned = False
            self.to_bot("Market polling recovered.")

    def ops(self, now):
        # poll gap
        since = self.last_poll_ok or self.started                 # never polled yet counts from boot
        if now - since > self.cfg.poll_gap_alert_minutes * 60 and not self.gap_warned:
            self.gap_warned = True
            self.to_bot(f"⚠️ No successful market poll for {M.dur(now - since)} (data source errors; retrying with backoff).")
        # flapping
        win = self.cfg.scanner_fail_warn_minutes * 60
        fails = [h for t, h in list(FAILURES) if t >= now - win]
        if len(fails) >= self.cfg.scanner_fail_warn_count and now - self.flap_warned_at >= win:
            self.flap_warned_at = now
            top = ", ".join(f"{h} ({n})" for h, n in Counter(fails).most_common(3))
            self.to_bot(f"⚠️ {len(fails)} data-source errors in {self.cfg.scanner_fail_warn_minutes:.0f} min: {top}. Still running, retrying with backoff.")
        # heartbeat
        last_hb = self.store.meta.get("last_heartbeat")
        if last_hb is None or now - last_hb >= self.cfg.heartbeat_minutes * 60:
            gap = (now - last_hb) if last_hb else None
            s = M.tracker_stats(self.store.records.values(), now)
            active = sum(1 for r in self.store.records.values() if r.status == "active" and r.official)
            poll = f"{now - self.last_poll_ok:.0f}s ago" if self.last_poll_ok else "n/a"
            txt = f"ok • poll {poll} • active {active} • tracker 24h alerts {s['alerts']}"
            if gap and gap > self.cfg.heartbeat_minutes * 60 * 1.5:
                txt += f" • recovered after {M.dur(gap)}"
            self.poster.send("bot", {"title": "heartbeat", "description": txt, "color": 0x3BA55D})
            self.store.meta["last_heartbeat"] = now
        # scheduled posts
        recs = list(self.store.records.values())
        for job, channel, build in (("tracker", "signals", M.tracker_embed), ("recap", "recap", M.recap_embed),
                                    ("leaderboard", "recap", M.leaderboard_embed), ("postmortem", "graveyard", M.postmortem_embed)):
            d = schedule.due(job, now, self.cfg, self.store.jobs)
            if not d:
                continue
            emb = build(recs, now, late=d["late"])
            if job == "recap" and emb.get("_empty"):
                h = datetime.fromtimestamp(d["slot"], PT).hour
                if not (self.cfg.recap_start_hour <= h <= self.cfg.recap_end_hour) and self.cfg.recap_skip_if_empty_overnight:
                    self.store.jobs[job] = d["slot"]
                    continue
            if self.poster.send(channel, emb) is not None:
                self.store.jobs[job] = d["slot"]
                self.store.save()

    def status(self, now):
        top = ", ".join(f"{k} ({v})" for k, v in self.blocks.most_common(4)) or "none"
        s = M.tracker_stats(self.store.records.values(), now)
        active = sum(1 for r in self.store.records.values() if r.status == "active")
        line_ = (f"[status] {fmt_pt(now, True)} feed={self.feed.status() if self.feed else 'off'} sol=${self.sol_usd or 0:.0f} "
                 f"launches={self.counts['launches']} watching={len(self.launches)} pairs={len(self.pairs)} live={active} "
                 f"checking={len(self.pending)} blacklist={len(self.store.blacklist)} | 24h: {s['alerts']} alerts, "
                 f"{s['hit2']} hit 2x, {s['dead']} dead, {s['invalidated']} invalidated | top blocks: {top} | "
                 f"api/min dex={LIMITERS['dex'].per_min()} rpc={LIMITERS['rpc'].per_min()} rugcheck={LIMITERS['rugcheck'].per_min()}")
        print(line_)
        self.status_lines.append(line_)
        self.blocks.clear()
        with open(self.cfg.status_file, "w") as f:
            f.write(f"{VERSION} | updated {fmt_pt(now, True)} | up {M.dur(now - self.started)}\n"
                    f"channels: " + ", ".join(f"{k}={'own' if v else 'fallback'}" for k, v in WEBHOOKS.items()) +
                    f" | rpc={'custom' if os.getenv('SOLANA_RPC_URL') else 'public'}\n\n" + "\n".join(self.status_lines) + "\n")

    def save(self):
        self.store.creator_times = {c: list(dq) for c, dq in self.creator_times.items() if len(dq) >= 2}
        self.store.prune(self.clock(), self.cfg.records_keep_days, self.cfg.blacklist_days, self.cfg.blacklist_max)
        self.store.save()

    def git_sync(self):
        """GitHub Actions only: commit + push state mid-run so a runner loss costs minutes, not hours."""
        files = [self.cfg.state_file, self.cfg.log_file, self.cfg.signals_file, self.cfg.status_file, self.cfg.backup_dir]
        try:
            subprocess.run(["git", "add", "-A", "--"] + [f for f in files if os.path.exists(f)], check=True, capture_output=True, timeout=30)
            if subprocess.run(["git", "diff", "--cached", "--quiet"], timeout=30).returncode == 0:
                return
            subprocess.run(["git", "-c", "user.name=signal-bot", "-c", "user.email=signal-bot@users.noreply.github.com",
                            "commit", "-q", "-m", "update state"], check=True, capture_output=True, timeout=30)
            subprocess.run(["git", "pull", "-q", "--rebase", "-X", "theirs", "origin", "main"], capture_output=True, timeout=60)
            subprocess.run(["git", "push", "-q", "origin", "HEAD:main"], capture_output=True, timeout=60)
        except Exception as e:
            print(f"[warn] git sync failed: {e}")

    # ---------------- main loop
    def boot_message(self):
        st, now = self.store, self.clock()
        s = M.tracker_stats(st.records.values(), now)
        active = sum(1 for r in st.records.values() if r.status == "active" and r.official)
        last = st.meta.get("last_poll_ok")
        state = "ok" if st.loaded else f"STARTED EMPTY ({st.load_error})"
        self.to_bot(f"booted {VERSION} • state {state} • recovered {active} active signals • last tracker: {s['alerts']} alerts, "
                    f"{s['hit2']} hit 2x, {s['dead']} dead • last successful poll {fmt_pt(last, True) if last else 'n/a'}", "restart")

    def tick(self, now):
        self.last_tick = now
        if self.sol_usd is None or now - self.sol_t > 120:
            v = fetch_sol_usd()
            if v:
                self.sol_usd, self.sol_t = v, now
        if self.feed:
            self.on_feed(self.feed.drain())
        if self.every("prune", 15, now):
            self.prune(now)
        self.discover(now)
        self.refresh_pairs(now)
        self.evaluate(now)
        self.collect(now)
        self.monitor(now)
        self.collect(now)
        self.ops(now)
        if self.every("status", 60, now):
            self.status(now)
        if self.every("save", 60, now):
            self.save()
        if self.cfg.git_sync_minutes and self.every("git", self.cfg.git_sync_minutes * 60, now) and now - self.started > 60:
            self.git_sync()


DEATH_TEXT = {"dev_sold": "dev sold after alert", "liquidity_pulled": "liquidity pulled", "safety_fail": "safety fail after alert"}


def start_health_server(bot: Bot, port: int):
    class H(BaseHTTPRequestHandler):
        def do_GET(self):
            now = time.time()
            ok = (now - bot.last_tick < 120 and os.path.exists(bot.cfg.state_file)
                  and bot.last_poll_ok is not None and now - bot.last_poll_ok <= bot.cfg.health_poll_stale_sec)
            body = json.dumps({"ok": ok, "last_poll_age_s": None if bot.last_poll_ok is None else round(now - bot.last_poll_ok),
                               "active": sum(1 for r in bot.store.records.values() if r.status == "active")}).encode()
            self.send_response(200 if ok else 503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *a):
            pass
    srv = HTTPServer(("0.0.0.0", port), H)
    threading.Thread(target=srv.serve_forever, daemon=True, name="health").start()
    print(f"[health] http://0.0.0.0:{port}/")


def run(bot: Bot, max_minutes: Optional[float] = None):
    stop = {"flag": False}

    def handler(*_):
        stop["flag"] = True
    os_signal.signal(os_signal.SIGTERM, handler)
    os_signal.signal(os_signal.SIGINT, handler)

    def watchdog():                                       # deadlocked loop -> exit so the process manager restarts us
        while not stop["flag"]:
            time.sleep(30)
            if time.time() - bot.last_tick > 300:
                print("[watchdog] main loop stuck for 5 min -> exiting for restart")
                try:
                    bot.save()
                finally:
                    os._exit(3)
    threading.Thread(target=watchdog, daemon=True, name="watchdog").start()
    if bot.cfg.health_port:
        start_health_server(bot, bot.cfg.health_port)
    if bot.feed:
        bot.feed.start()
    bot.boot_message()
    end = bot.started + max_minutes * 60 if max_minutes else None
    print(f"[start] {VERSION} rpc={'custom' if os.getenv('SOLANA_RPC_URL') else 'public'}")
    try:
        while not stop["flag"] and (end is None or bot.clock() < end):
            try:
                bot.tick(bot.clock())
            except Exception:
                traceback.print_exc()                     # recoverable: log and keep going, never restart for this
            time.sleep(1.0)
    finally:
        if bot.feed:
            bot.feed.stop = True
        bot.save()
        bot.status(bot.clock())
        bot.pool.shutdown(wait=False, cancel_futures=True)
        print("[stop] saved")


# ======================================================================= test + probe
def send_test(poster: Poster):
    now, cfg = time.time(), Config()
    ca = "SampLe1111111111111111111111111111111111pump"
    obs = {"ca": ca, "ticker": "SAMPLE", "name": "Sample (test post)", "price": 6e-5, "mc": 59_678, "liq": 17_604, "pool": "SamplePool",
           "score": 79, "score_parts": "71 +8 catalyst = 79", "reasons": ["liquidity 29% of MC", "1h volume 1.57x MC"],
           "metrics": {"age_s": 2500, "vol_m5": 9000, "vol_h1": 93_641, "chg_m5": 12, "chg_h1": 64, "buys": 410, "sells": 180,
                       "buyers": 233, "holders": 612, "top10": 18.5, "dev": 0.0, "insiders": 0.0, "bundler": 4.0, "sniper": 9.0},
           "checks": ["✅ RugCheck clean", "✅ Mint & freeze revoked", "✅ LP burned (pump.fun migration)"],
           "catalyst_tag": "REAL_CATALYST", "catalyst_url": "https://example.com/news", "catalyst_summary": "Example: event coverage (test)"}
    rec = Record(ca=ca, ticker="SAMPLE", official=True, first_alert_at=now - 3000, first_alert_price=6e-5, first_alert_mc_usd=59_678,
                 peak_price=1.5e-4, peak_mc_usd=150_000, peak_at=now - 600, last_valid_price=1.3e-4, last_valid_mc=130_000,
                 last_valid_at=now - 60, max_drawdown_pct=-12)
    poster.send("signals", M.signal_embed("NEW", obs))
    poster.send("early-watch", M.signal_embed("NEW", obs), M.EARLY_BANNER)
    poster.send("wins", M.multiplier_embed(2, rec, 1.3e-4, 130_000, now))
    poster.send("graveyard", M.ending_embed("DEAD", rec, "liquidity_pulled", "Liquidity $40K → $300.", now))
    poster.send("signals", M.tracker_embed([rec], now))
    poster.send("bot", {"title": "heartbeat", "description": "ok • test post"})
    print("[test] sent one sample of each message type")


def probe(mint: str):
    cfg, now = load_config(), time.time()
    pairs = (fetch_dex_all_pairs([mint]) or {}).get(mint) or []
    real = [d for d in pairs if (d.get("dexId") or "").lower() != "pumpfun"]
    print(f"pairs: {[(d.get('dexId'), d.get('pairAddress')) for d in pairs]}")
    if not real:
        print("still on the bonding curve (or unknown to DexScreener); curve coins are judged from the live feed")
        return
    d = max(real, key=lambda x: num((x.get("liquidity") or {}).get("usd")) or 0)
    p = Pair(mint=mint, pair=d.get("pairAddress"), dex=d.get("dexId"), symbol=(d.get("baseToken") or {}).get("symbol"),
             created_ms=num(d.get("pairCreatedAt")), snap=pair_snap(d, now))
    res = deep_dex(mint, p.pair, None, None)
    p.rc, p.chain = res.get("rc"), res
    obs = finalize(dex_observation(p, now, cfg, False, None), cfg, "UNCHECKED", [], None)
    print("\n".join(obs["checks"]))
    print(f"hard fails: {obs['hard_fails'] or 'none'} | gate fails: {obs['gate_fails'] or 'none'}")
    print(f"score {obs['score']} ({obs['score_parts'] or obs['raw_score']}) | breakout: {obs['breakout_reasons'] or 'no'}")
    print(f"=> #signals: {obs['qualifies_signals']} | #early-watch: {obs['qualifies_early']}")


def main(argv):
    cfg = load_config()
    poster = DiscordPoster(WEBHOOKS)
    if "--test" in argv:
        send_test(poster)
        return
    if "--probe" in argv:
        probe(norm_ca(argv[argv.index("--probe") + 1]))
        return
    mm = float(argv[argv.index("--max-minutes") + 1]) if "--max-minutes" in argv else None
    run(Bot(cfg, poster, feed=PumpFeed()), max_minutes=mm)


if __name__ == "__main__":
    main(sys.argv[1:])
