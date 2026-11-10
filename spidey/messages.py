"""Discord embeds for every post type (§4). Every coin post: header line with TYPE, then the full CA."""
from statistics import median
from typing import Optional

from .store import RUG_REASONS, STAGES, Record, fmt_pt, stage_of

FOOTER_TEXT = "Research only, not financial advice."
EARLY_BANNER = "NOT A BUY CALL — early watch only."
COLORS = {"NEW": 0x3BA55D, "UPDATE": 0x5865F2, "BREAKOUT": 0xF0A020, "2X": 0x57F287, "3X": 0x57F287, "5X": 0x57F287,
          "INVALIDATED": 0xED4245, "DEAD": 0x99AAB5, "CORRECTION": 0xFEE75C}
STAGE_LABEL = {"new_pairs": "New Pairs", "final_stretch": "Final Stretch", "migrated": "Migrated"}
DEATH_LABEL = {"price_collapse": "price collapsed", "lp_unlocked": "LP unlocked", "authority_revived": "authority revived",
               "liquidity_pulled": "liquidity pulled", "dev_sold": "dev sold", "dev_blacklisted": "dev blacklisted",
               "no_trades": "no trades (quiet fade)", "safety_fail": "safety fail", "manual": "manual"}
LESSON = {
    "price_collapse": "Price fell 90%+ from its high. A dump this deep is treated as a rug even if some liquidity is left.",
    "lp_unlocked": "LP was not actually locked/burned. The real RugCheck lock % is now required.",
    "authority_revived": "Mint/freeze authority came back after the alert.",
    "dev_sold": "Dev wallet sold after the alert. Any dev sell now ends the signal immediately.",
    "dev_blacklisted": "Dev had rugged before. Repeat rug wallets are blocked for 30 days.",
    "liquidity_pulled": "LP was pulled or drained. Prefer burned/locked LP and deeper pools.",
    "no_trades": "Volume dried up and price faded. Momentum without a catalyst rarely holds.",
    "safety_fail": "A hard safety check failed after the alert (authority revived, LP unlocked, flagged rug).",
    "manual": "Closed by hand.",
}


def usd(n) -> str:
    if n is None:
        return "n/a"
    n = float(n)
    for div, suf in ((1e9, "B"), (1e6, "M"), (1e3, "K")):
        if abs(n) >= div:
            return f"${n / div:.1f}{suf}" if n / div < 100 else f"${n / div:.0f}{suf}"
    return f"${n:,.0f}"


def price_str(p) -> str:
    if not p:
        return "n/a"
    return f"${p:.3g}" if p >= 0.001 else f"${p:.3e}"


def mult(x: Optional[float]) -> str:
    return "n/a" if x is None else f"{x:.2f}x"


def pct(n, signed=True) -> str:
    if n is None:
        return "n/a"
    return f"{n:+.0f}%" if signed else f"{n:.0f}%"


def dur(seconds: Optional[float]) -> str:
    if seconds is None:
        return "n/a"
    m = max(0, int(seconds // 60))
    return f"{m}m" if m < 60 else (f"{m // 60}h {m % 60}m" if m < 1440 else f"{m // 1440}d {m % 1440 // 60}h")


def header(typ: str, ticker: str, tail: str) -> str:
    return f"{typ}  •  ${ticker}  •  {tail}"[:256]


def ca_line(ca: str) -> str:
    return f"```{ca}```"


def footer() -> dict:
    return {"text": FOOTER_TEXT}


def links(ca: str, pool: Optional[str], extra: bool = True, x_url: Optional[str] = None) -> str:
    out = [f"[Axiom](https://axiom.trade/meme/{pool or ca})", f"[DexScreener](https://dexscreener.com/solana/{ca})",
           f"[Bubblemaps](https://app.bubblemaps.io/sol/token/{ca})", f"[RugCheck](https://rugcheck.xyz/tokens/{ca})",
           f"[X]({x_url})" if x_url else f"[X](https://x.com/search?q={ca})"]
    if extra:
        out += [f"[Solscan](https://solscan.io/token/{ca})"]
        if ca.endswith("pump"):
            out.append(f"[pump.fun](https://pump.fun/coin/{ca})")
    return " · ".join(out)


def catalyst_lines(tag: str, url: Optional[str], summary: Optional[str]) -> str:
    if tag == "REAL_CATALYST" and url:
        return f"**REAL CATALYST** — {summary or 'outside coverage found'}\nSource: {url}"
    if tag == "HYPE_ONLY":
        return "**HYPE ONLY** — no credible outside coverage found"
    return "UNCHECKED — news search pending"


def signal_embed(typ: str, obs: dict, rec: Optional[Record] = None, changed: Optional[list] = None) -> dict:
    """NEW / UPDATE / BREAKOUT. obs = the scanner's observation of the coin (see bot.Observation)."""
    m = obs.get("metrics") or {}
    score_line = f"score {obs['score']}/100"
    lines = [ca_line(obs["ca"])]
    if obs.get("stage"):
        bp = obs.get("bonding_pct")
        lines.append(f"Stage: **{STAGE_LABEL.get(obs['stage'], obs['stage'])}**" + (f" · bonding {bp:.0f}%" if bp is not None else ""))
    if changed:
        lines.append("**What changed:** " + "; ".join(changed))
    if obs.get("score_parts"):
        lines.append(f"Score: {obs['score_parts']}")
    fired = obs.get("breakout_reasons") if typ == "BREAKOUT" and obs.get("breakout_reasons") else obs.get("reasons")
    fields = []
    if fired:
        fields.append({"name": "Breakout trigger" if typ == "BREAKOUT" else "Why it fired",
                       "value": "\n".join(f"• {r}" for r in fired)[:1024], "inline": False})
    fields.append({"name": "Market", "value": (
        f"MC **{usd(obs.get('mc'))}** · Liq **{usd(obs.get('liq')) if obs.get('liq') is not None else 'n/a'}** · Age **{dur(m.get('age_s'))}**\n"
        f"Vol 5m **{usd(m.get('vol_m5'))}** ({pct(m.get('chg_m5'))}) · Vol 1h **{usd(m.get('vol_h1'))}** ({pct(m.get('chg_h1'))})\n"
        f"Buys/Sells **{m.get('buys', 'n/a')}/{m.get('sells', 'n/a')}** · Unique buyers **{m.get('buyers', 'n/a')}**"), "inline": False})
    hold = (f"Holders **{m.get('holders', 'n/a')}** · Top 10 **{pct(m.get('top10'), False)}**"
            + (" ⚠️" if m.get("top10_warn") else "") + f" · Dev **{pct(m.get('dev'), False) if m.get('dev') is not None else 'n/a'}**"
            + f" · Insiders **{pct(m.get('insiders'), False) if m.get('insiders') is not None else 'n/a'}**\n"
            + f"Bundler **{pct(m.get('bundler'), False) if m.get('bundler') is not None else 'n/a'}** · "
            + f"Sniper **{pct(m.get('sniper'), False) if m.get('sniper') is not None else 'n/a'}** · Fresh wallets **n/a**")
    fields.append({"name": "Holders", "value": hold, "inline": False})
    if obs.get("checks"):
        fields.append({"name": "Safety checks", "value": "\n".join(obs["checks"])[:1024], "inline": False})
    tag = rec.catalyst_tag if rec else obs.get("catalyst_tag", "UNCHECKED")
    url = rec.catalyst_url if rec else obs.get("catalyst_url")
    summ = rec.catalyst_summary if rec else obs.get("catalyst_summary")
    fields.append({"name": "Catalyst", "value": catalyst_lines(tag, url, summ)[:1024], "inline": False})
    conf = (rec.confluence if rec else None) or obs.get("confluence")
    if conf:
        fields.append({"name": "Also seen on", "value": ", ".join(conf), "inline": False})
    if rec and typ != "NEW" and rec.first_alert_price:
        fields.append({"name": "Since alert", "value": (
            f"Alert {usd(rec.first_alert_mc_usd)} at {fmt_pt(rec.first_alert_at)} · now **{mult(rec.multiple(rec.last_valid_price))}**"
            + (f" · [original]({rec.first_alert_jump})" if rec.first_alert_jump else "")), "inline": False})
    fields.append({"name": "Links", "value": links(obs["ca"], obs.get("pool"), extra=True, x_url=obs.get("x_url")), "inline": False})
    return {"title": header(typ, obs.get("ticker", "?"), score_line), "color": COLORS.get(typ, 0x5865F2),
            "description": "\n".join(lines)[:4096], "fields": fields, "footer": footer()}


def multiplier_embed(level: int, rec: Record, now_price: float, now_mc: Optional[float], now_ts: float) -> dict:
    m = rec.multiple(now_price)
    dd = rec.max_drawdown_pct
    body = [ca_line(rec.ca),
            f"Alert: {price_str(rec.first_alert_price)} / {usd(rec.first_alert_mc_usd)} at {fmt_pt(rec.first_alert_at)}",
            f"Now:   {price_str(now_price)} / {usd(now_mc)} at {fmt_pt(now_ts)}",
            f"Peak:  {mult(rec.peak_multiple)}  •  Max DD: {dd:.0f}%"]
    if rec.first_alert_jump:
        body.append(f"Original: {rec.first_alert_jump}")
    return {"title": header(f"{level}X", rec.ticker, f"{mult(m)} from alert"), "color": COLORS["2X"],
            "description": "\n".join(body), "footer": footer()}


def ending_embed(typ: str, rec: Record, reason: str, detail: str, now_ts: float) -> dict:
    """INVALIDATED / DEAD (§4.3). Multiples only from the last VALID trade."""
    label = DEATH_LABEL.get(reason, reason.replace("_", " "))
    if rec.official and rec.first_alert_price:
        base_ts, base_price, base_mc = rec.first_alert_at, rec.first_alert_price, rec.first_alert_mc_usd
    else:
        base_ts, base_price, base_mc = rec.early_alert_at or rec.first_alert_at, rec.early_alert_price or rec.first_alert_price, \
            rec.early_alert_mc_usd or rec.first_alert_mc_usd
    m = (lambda p: (p / base_price) if (p and base_price) else None)
    body = [ca_line(rec.ca)]
    if detail:
        body.append(detail)
    if not rec.official:
        body.append("_Early-watch coin (not an official signal)._")
    body += [f"Alert: {usd(base_mc)}, {price_str(base_price)}, {fmt_pt(base_ts)}",
             f"Peak:  {usd(rec.peak_mc_usd)} ({mult(m(rec.peak_price))}) at {fmt_pt(rec.peak_at)}",
             f"Last valid: {usd(rec.last_valid_mc)}, {price_str(rec.last_valid_price)}, {mult(m(rec.last_valid_price))}, {fmt_pt(rec.last_valid_at)}"]
    if reason == "liquidity_pulled":
        body.append("LP pulled — last valid multiple, quoted MC after pull may be stale.")
    body.append(f"Time alive: {dur(now_ts - base_ts) if base_ts else 'n/a'}")
    jump = rec.first_alert_jump or rec.early_jump
    if jump:
        body.append(f"Original: {jump}")
    return {"title": header(typ, rec.ticker, label), "color": COLORS[typ], "description": "\n".join(body), "footer": footer()}


def correction_embed(rec: Record, what: str) -> dict:
    return {"title": header("CORRECTION", rec.ticker, "fixes the earlier post"), "color": COLORS["CORRECTION"],
            "description": f"{ca_line(rec.ca)}\n{what}", "footer": footer()}


# ======================================================================= reports
def _med(vals):
    vals = [v for v in vals if v is not None]
    return median(vals) if vals else None


def tracker_stats(records, now: float) -> dict:
    """§2.6 / §5.5 — recomputed from the records every time."""
    win = [r for r in records if r.official and r.first_alert_at and now - 86400 <= r.first_alert_at <= now]
    pk = [r.peak_multiple or 0 for r in win]
    t = {k: _med([r.multiple(r.price_at.get(k)) for r in win]) for k in ("t+15m", "t+30m", "t+60m")}
    return {"rugs": sum(r.ended and r.death_reason in RUG_REASONS for r in win),
            "fades": sum(r.ended and r.death_reason not in RUG_REASONS for r in win),
            "alerts": len(win), "hit2": sum(p >= 2 for p in pk), "hit3": sum(p >= 3 for p in pk), "hit5": sum(p >= 5 for p in pk),
            "dead": sum(r.status == "dead" for r in win), "invalidated": sum(r.status == "invalidated" for r in win),
            "open": sum(not r.ended for r in win), "median_dd": _med([r.max_drawdown_pct for r in win]),
            "t15": t["t+15m"], "t30": t["t+30m"], "t60": t["t+60m"]}


def _start(r: Record):
    return r.first_alert_at if r.official else r.early_alert_at


def _peak_x(r: Record) -> float:
    base = r.first_alert_price if r.official else r.early_alert_price
    return (r.peak_price / base) if (base and r.peak_price) else 0.0


def stage_stats(records, now: float) -> dict:
    """Per stage, every coin posted in the last 24h (official + early-watch): posts, 2x, 5x, rugs, quiet fades."""
    out = {st: {"posts": 0, "official": 0, "hit2": 0, "hit5": 0, "rugs": 0, "fades": 0} for st in STAGES}
    for r in records:
        t = _start(r)
        if not t or not (now - 86400 <= t <= now):
            continue
        d = out[stage_of(r)]
        d["posts"] += 1
        d["official"] += bool(r.official)
        d["hit2"] += _peak_x(r) >= 2
        d["hit5"] += _peak_x(r) >= 5
        if r.ended:
            d["rugs" if r.death_reason in RUG_REASONS else "fades"] += 1
    return out


def stage_lines(records, now: float) -> list:
    out = []
    for st, d in stage_stats(records, now).items():
        n = d["posts"]
        rate = (lambda k: f"{d[k] / n * 100:.0f}%") if n else (lambda k: "n/a")
        out.append(f"{STAGE_LABEL[st]}: {n} posts ({d['official']} official) · 2x {d['hit2']} ({rate('hit2')}) · "
                   f"5x {d['hit5']} ({rate('hit5')}) · rugged {d['rugs']} · faded {d['fades']}")
    return out


def tracker_embed(records, now: float, late: bool = False) -> dict:
    s = tracker_stats(records, now)
    body = [f"Alerts: {s['alerts']} unique CAs in #signals",
            f"Hit 2x / 3x / 5x (peak, valid prints): {s['hit2']} / {s['hit3']} / {s['hit5']}",
            f"Rugged: {s['rugs']}  •  Quiet fades: {s['fades']}  •  Still open: {s['open']}",
            "Median max drawdown: " + ("n/a" if s["median_dd"] is None else f"{s['median_dd']:.0f}%"),
            f"Price vs alert (median multiple): T+15 {mult(s['t15'])}  •  T+30 {mult(s['t30'])}  •  T+60 {mult(s['t60'])}",
            "", "**By stage (all posts, incl. early-watch)**"] + stage_lines(records, now) + [
            "_Early-watch coins count from their promotion price once promoted._"]
    title = f"24h tracker  •  {fmt_pt(now - 86400, True)} – {fmt_pt(now, True)}" + ("  (late)" if late else "")
    return {"title": title, "color": 0x5865F2, "description": "\n".join(body), "footer": footer()}


def recap_embed(records, now: float, late: bool = False) -> Optional[dict]:
    hour = [r for r in records if r.official and r.first_alert_at and r.first_alert_at >= now - 3600]
    early = [r for r in records if not r.official and r.early_alert_at and r.early_alert_at >= now - 3600]
    deaths = [r for r in records if r.ended and r.death_at and r.death_at >= now - 3600]
    live = [r for r in records if r.official and not r.ended and r.status == "active" and r.multiple(r.last_valid_price)]
    live.sort(key=lambda r: r.multiple(r.last_valid_price), reverse=True)
    if not (hour or early or deaths):
        lines = ["Quiet hour — no new qualifying coins."]
    else:
        lines = [f"New signals: {len(hour)}" + (" — " + ", ".join(f"${r.ticker}" for r in hour) if hour else ""),
                 f"New early-watch: {len(early)}", f"Deaths / invalidations: {len(deaths)}"
                 + (" — " + ", ".join(f"${r.ticker} ({DEATH_LABEL.get(r.death_reason or r.invalidation_reason or '', 'ended')})" for r in deaths) if deaths else "")]
    lines.append(f"Open signals: {len(live)}")
    if live:
        b, w = live[0], live[-1]
        lines.append(f"Best vs alert: ${b.ticker} {mult(b.multiple(b.last_valid_price))}  •  Worst: ${w.ticker} {mult(w.multiple(w.last_valid_price))}")
    return {"title": f"Hourly recap  •  {fmt_pt(now - 3600)}–{fmt_pt(now)}" + ("  (late)" if late else ""), "color": 0x5865F2,
            "description": "\n".join(lines), "footer": footer(), "_empty": not (hour or early or deaths)}


def leaderboard_embed(records, now: float, late: bool = False) -> dict:
    win = [r for r in records if r.official and r.first_alert_at and r.first_alert_at >= now - 86400]
    win.sort(key=lambda r: r.peak_multiple or 0, reverse=True)

    def row(r):
        return (f"**${r.ticker}** `{r.ca}`\n  peak {mult(r.peak_multiple)} · max DD {r.max_drawdown_pct:.0f}% · "
                f"T+30 {mult(r.multiple(r.price_at.get('t+30m')))} · {r.catalyst_tag.replace('_', ' ')} · "
                f"{'dead' if r.ended else 'alive'}")
    best, worst = win[:5], [r for r in win[::-1][:3] if r not in win[:5]]
    desc = ("**Best**\n" + "\n".join(row(r) for r in best)) if best else "No official signals in the last 24h."
    if worst:
        desc += "\n\n**Worst**\n" + "\n".join(row(r) for r in worst)
    desc = desc[:3400] + "\n\n**Hit rate by stage (24h, all posts)**\n" + "\n".join(stage_lines(records, now))
    return {"title": "Daily leaderboard" + ("  (late)" if late else ""), "color": 0xF0A020, "description": desc[:4096], "footer": footer()}


def postmortem_embed(records, now: float, late: bool = False) -> dict:
    ended = [r for r in records if r.ended and r.death_at and r.death_at >= now - 86400]
    groups = {}
    for r in ended:
        groups.setdefault(r.death_reason or "manual", []).append(r)
    if not groups:
        desc = "Nothing died or got invalidated in the last 24h."
    else:
        parts = []
        for reason, rs in sorted(groups.items(), key=lambda kv: -len(kv[1])):
            parts.append(f"**{DEATH_LABEL.get(reason, reason)} ({len(rs)})** — {LESSON.get(reason, '')}\n"
                         + "\n".join(f"• ${r.ticker} `{r.ca}`" for r in rs))
        desc = "\n\n".join(parts)
    return {"title": "Why it died — last 24h" + ("  (late)" if late else ""), "color": 0x99AAB5, "description": desc[:4096], "footer": footer()}
