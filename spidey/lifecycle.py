"""Signal lifecycle (§2.4, §2.5, §3): one NEW per CA, labeled follow-ups only on material change,
valid-price multiples, one ending post per CA. Independent of the scanners, so it can be tested with fixtures.

Every Discord send follows reserve -> save -> send -> roll back on failure, so a crash can never cause a
second NEW / multiplier / death for the same CA, and a failed send is retried on the next poll.
"""
import time
from typing import Callable, Optional

from . import messages as M
from .config import Config
from .store import Record, Store, norm_ca

T_MARKS = (("t+5m", 300), ("t+15m", 900), ("t+30m", 1800), ("t+60m", 3600))


class Poster:
    """Interface: send(channel, embed, content=None) -> {"id": str, "jump": str|None} or None if it failed."""

    def send(self, channel: str, embed: dict, content: Optional[str] = None) -> Optional[dict]:
        raise NotImplementedError


class Lifecycle:
    def __init__(self, store: Store, cfg: Config, poster: Poster, clock=time.time,
                 on_end: Optional[Callable[[Record, str, str], None]] = None):
        self.store, self.cfg, self.poster, self.clock = store, cfg, poster, clock
        self.on_end = on_end                      # hook: (record, reason, detail) -> e.g. blacklist the dev
        self.new_times = []

    # ------------------------------------------------------------------ helpers
    def _send(self, channel, embed, content=None, reserve=None, rollback=None) -> Optional[dict]:
        if reserve:
            reserve()
            self.store.save()
        res = self.poster.send(channel, embed, content)
        if res is None and rollback:
            rollback()
            self.store.save()
        elif res is not None:
            self.store.save()
        return res

    def _new_allowed(self, now) -> bool:
        self.new_times = [t for t in self.new_times if t > now - 3600]
        return len(self.new_times) < self.cfg.max_new_per_hour

    # ------------------------------------------------------------------ candidates (scanner output)
    def on_candidate(self, obs: dict) -> Optional[str]:
        """Feed one scanner observation. Returns the post type that went out (or None)."""
        now = self.clock()
        ca = norm_ca(obs["ca"])
        obs["ca"] = ca
        rec = self.store.get(ca)
        if rec is None:
            return self._maybe_new(obs, now)
        if rec.ended or rec.status == "archived":
            return None                                            # never resurrect, never re-post
        self._refresh_latest(rec, obs, now)
        if not rec.official and obs.get("qualifies_promotion"):
            return self._promote(rec, obs, now)
        return self._maybe_update(rec, obs, now)

    def _refresh_latest(self, rec: Record, obs: dict, now: float):
        rec.ticker, rec.name = obs.get("ticker") or rec.ticker, obs.get("name") or rec.name
        rec.last_score, rec.last_mc_usd, rec.last_liq_usd = obs.get("score"), obs.get("mc"), obs.get("liq")
        rec.last_polled_at = now
        rec.dev_wallet = rec.dev_wallet or obs.get("dev_wallet")
        if obs.get("pool") and not rec.pool:
            rec.pool, rec.pool_dex = obs["pool"], obs.get("pool_dex")

    def _maybe_new(self, obs: dict, now: float) -> Optional[str]:
        channel = "signals" if obs.get("qualifies_signals") else ("early-watch" if obs.get("qualifies_early") else None)
        if channel is None:
            return None
        if not self._new_allowed(now):
            return None
        official = channel == "signals"
        rec = Record(ca=obs["ca"], ticker=obs.get("ticker") or "?", name=obs.get("name") or "", official=official,
                     source=obs.get("source", "dex"), first_alert_channel=channel, dev_wallet=obs.get("dev_wallet"),
                     pool=obs.get("pool"), pool_dex=obs.get("pool_dex"), curve_key=obs.get("curve_key"),
                     catalyst_tag=obs.get("catalyst_tag") or "UNCHECKED", catalyst_url=obs.get("catalyst_url"),
                     catalyst_summary=obs.get("catalyst_summary"), catalyst_first_try_at=now,
                     confluence=list(obs.get("confluence") or []), safety_snapshot=dict(obs.get("safety") or {}),
                     dev_balance=obs.get("dev_balance"), dev_peak_balance=obs.get("dev_balance"))
        self._set_baseline(rec, obs, now, official)
        rec.last_score = rec.last_posted_score = obs.get("score")
        rec.last_update_at, rec.last_posted_price, rec.last_polled_at = now, obs.get("price"), now
        rec.breakout_active = bool(obs.get("breakout"))
        if obs.get("liq") is not None:
            rec.liq_hist = [[now, obs["liq"]]]

        def reserve():
            self.store.records[rec.ca] = rec

        def rollback():
            self.store.records.pop(rec.ca, None)

        res = self._send(channel, M.signal_embed("NEW", obs, rec), M.EARLY_BANNER if not official else None, reserve, rollback)
        if res is None:
            return None
        if official:
            rec.first_alert_message_id, rec.first_alert_jump = res.get("id"), res.get("jump")
        else:
            rec.early_jump = res.get("jump")
            rec.first_alert_message_id = res.get("id")
        self.new_times.append(now)
        self.store.save()
        return "NEW"

    def _set_baseline(self, rec: Record, obs: dict, now: float, official: bool):
        price = obs.get("price")
        rec.last_price = rec.last_valid_price = price
        rec.last_mc_usd = rec.last_valid_mc = obs.get("mc")
        rec.last_liq_usd = obs.get("liq")
        rec.last_valid_at = rec.last_trade_at = now
        rec.peak_price, rec.peak_mc_usd, rec.peak_at = price, obs.get("mc"), now
        rec.max_drawdown_pct = 0.0
        rec.price_at = {}
        if official:
            rec.first_alert_at, rec.first_alert_price = now, price
            rec.first_alert_price_sol = obs.get("price_sol")
            rec.first_alert_mc_usd, rec.first_alert_liq_usd = obs.get("mc"), obs.get("liq")
        else:
            rec.early_alert_at, rec.early_alert_price, rec.early_alert_mc_usd = now, price, obs.get("mc")

    def _promote(self, rec: Record, obs: dict, now: float) -> Optional[str]:
        """Early-watch -> #signals. Official baseline = promotion price (documented in the tracker footer)."""
        before = (rec.official, rec.first_alert_at, rec.first_alert_price, rec.first_alert_mc_usd, rec.first_alert_liq_usd,
                  rec.promoted_to_signals_at, rec.peak_price, rec.peak_mc_usd, rec.peak_at, rec.max_drawdown_pct,
                  dict(rec.price_at), rec.last_update_at, rec.last_posted_price, rec.last_posted_score)

        def reserve():
            rec.official, rec.promoted_to_signals_at = True, now
            self._set_baseline(rec, obs, now, official=True)
            rec.last_update_at, rec.last_posted_price, rec.last_posted_score = now, obs.get("price"), obs.get("score")

        def rollback():
            (rec.official, rec.first_alert_at, rec.first_alert_price, rec.first_alert_mc_usd, rec.first_alert_liq_usd,
             rec.promoted_to_signals_at, rec.peak_price, rec.peak_mc_usd, rec.peak_at, rec.max_drawdown_pct,
             rec.price_at, rec.last_update_at, rec.last_posted_price, rec.last_posted_score) = before

        changed = [f"Promoted from #early-watch (flagged {M.fmt_pt(rec.early_alert_at)})"]
        if rec.early_alert_price and obs.get("price"):
            changed.append(f"{M.mult(obs['price'] / rec.early_alert_price)} since early-watch")
        res = self._send("signals", M.signal_embed("UPDATE", obs, rec, changed), None, reserve, rollback)
        if res is None:
            return None
        rec.first_alert_message_id, rec.first_alert_jump = res.get("id"), res.get("jump")
        self.store.save()
        return "UPDATE"

    def material_changes(self, rec: Record, obs: dict, now: float) -> tuple:
        """([what changed], urgent, is_breakout)."""
        changed, urgent = [], False
        sc, last_sc = obs.get("score"), rec.last_posted_score
        if sc is not None and last_sc is not None and abs(sc - last_sc) >= self.cfg.material_score_delta:
            changed.append(f"Score {last_sc} → {sc}")
        p, lp = obs.get("price"), rec.last_posted_price
        if p and lp and abs(p / lp - 1) * 100 >= self.cfg.material_move_pct:
            mc_then = (obs.get("mc") or 0) * lp / p
            changed.append(f"MC {M.usd(mc_then)} → {M.usd(obs.get('mc'))} ({(p / lp - 1) * 100:+.0f}%)")
        old_fail = set(rec.safety_snapshot.get("fails") or [])
        new_fail = [f for f in (obs.get("safety") or {}).get("fails") or [] if f not in old_fail]
        if new_fail:
            changed.append("New safety flag: " + ", ".join(new_fail))
            urgent = True
        breakout = bool(obs.get("breakout")) and not rec.breakout_active
        return changed, urgent, breakout

    def _maybe_update(self, rec: Record, obs: dict, now: float) -> Optional[str]:
        changed, urgent, breakout = self.material_changes(rec, obs, now)
        if not obs.get("breakout"):
            rec.breakout_active = False
        snap = obs.get("safety")
        if not changed and not breakout:
            if snap is not None:
                rec.safety_snapshot = dict(snap)
            return None                                           # silent state update
        cooldown = (now - (rec.last_update_at or 0)) < self.cfg.update_cooldown_minutes * 60
        if cooldown and not urgent:
            return None                                           # keep the edge: it can fire after cooldown
        typ = "BREAKOUT" if breakout else "UPDATE"
        before = (rec.last_update_at, rec.last_posted_price, rec.last_posted_score, rec.breakout_active, dict(rec.safety_snapshot))

        def reserve():
            rec.last_update_at, rec.last_posted_price, rec.last_posted_score = now, obs.get("price"), obs.get("score")
            rec.breakout_active = rec.breakout_active or breakout
            if snap is not None:
                rec.safety_snapshot = dict(snap)

        def rollback():
            rec.last_update_at, rec.last_posted_price, rec.last_posted_score, rec.breakout_active, rec.safety_snapshot = before

        channel = "signals" if rec.official else "early-watch"
        res = self._send(channel, M.signal_embed(typ, obs, rec, changed), None if rec.official else M.EARLY_BANNER, reserve, rollback)
        return typ if res else None

    # ------------------------------------------------------------------ catalyst results (§5.4)
    def on_catalyst(self, ca: str, tag: Optional[str], url: Optional[str], summary: Optional[str], obs: Optional[dict] = None) -> Optional[str]:
        """tag None = the search failed (stays UNCHECKED, retried by the caller for catalyst_retry_minutes)."""
        rec = self.store.get(ca)
        if rec is None or tag is None:
            return None
        if tag == "REAL_CATALYST" and not url:
            tag = "HYPE_ONLY"                                     # never REAL without a URL
        old = (rec.catalyst_tag, rec.catalyst_url)
        rec.catalyst_tag, rec.catalyst_url, rec.catalyst_summary = tag, url, summary
        quiet_fill = old[0] == "UNCHECKED" and tag == "HYPE_ONLY"      # filling in "nothing found" isn't news
        cooldown = (self.clock() - (rec.last_update_at or 0)) < self.cfg.update_cooldown_minutes * 60
        if old == (tag, url) or quiet_fill or rec.ended or rec.status == "archived" or obs is None \
                or (cooldown and tag != "REAL_CATALYST"):
            self.store.save()
            return None
        before = (rec.last_update_at,)

        def reserve():
            rec.last_update_at = self.clock()

        def rollback():
            rec.last_update_at = before[0]

        changed = [f"Catalyst {old[0].replace('_', ' ')} → {tag.replace('_', ' ')}"]
        channel = "signals" if rec.official else "early-watch"
        res = self._send(channel, M.signal_embed("UPDATE", obs, rec, changed), None if rec.official else M.EARLY_BANNER, reserve, rollback)
        return "UPDATE" if res else None

    # ------------------------------------------------------------------ monitoring (prices)
    def on_quote(self, ca: str, quote: Optional[dict]) -> Optional[str]:
        """quote for the record's PRIMARY pool (None = the pool is gone from the data source).
        quote = {price, mc, liq (None on curve), pool, dex, traded (bool: trades since last poll)}"""
        now = self.clock()
        rec = self.store.get(ca)
        if rec is None or rec.ended or rec.status == "archived":
            return None
        rec.last_polled_at = now
        start = rec.first_alert_at if rec.official else rec.early_alert_at
        if start and now - start > self.cfg.track_hours * 3600:
            rec.status = "archived"
            return None
        if quote is None:
            rec.quote_state = "STALE"
            if rec.last_liq_usd and rec.last_liq_usd >= self.cfg.dead_liq_usd:   # a real pool that disappeared
                rec.missing_since = rec.missing_since or now
                if now - rec.missing_since >= self.cfg.pool_missing_minutes * 60:
                    return self.end(rec, "DEAD", "liquidity_pulled", "Pool vanished from the market data.")
            return None
        rec.missing_since = None
        # migration: switch the primary pool ONCE (curve -> real pool); baseline never changes
        if quote.get("pool") and rec.pool and quote["pool"] != rec.pool:
            if ((rec.pool_dex or "").lower() == "pumpfun" or rec.source == "curve") and not rec.migrated_at:
                rec.pool, rec.pool_dex, rec.migrated_at = quote["pool"], quote.get("dex"), now
                rec.liq_hist = []
            else:
                return None                                       # not our pool: never price off a residual pool
        elif quote.get("pool") and not rec.pool:
            rec.pool, rec.pool_dex = quote["pool"], quote.get("dex")
        price, mc, liq = quote.get("price"), quote.get("mc"), quote.get("liq")
        rec.last_price, rec.last_mc_usd = price, mc
        on_curve = liq is None
        if not on_curve:
            rec.last_liq_usd = liq
            hist = [h for h in rec.liq_hist if h[0] >= now - self.cfg.lp_pull_window_minutes * 60]
            top = max([h[1] for h in hist], default=0)
            rec.liq_hist = (rec.liq_hist + [[now, liq]])[-40:]
            pulled = top >= self.cfg.dead_liq_usd and liq <= top * (1 - self.cfg.lp_pull_drop_pct / 100)
            if liq < self.cfg.dead_liq_usd or pulled:
                rec.quote_state = "INVALID_POOL"
                return self.end(rec, "DEAD", "liquidity_pulled",
                                f"Liquidity {M.usd(top or rec.first_alert_liq_usd)} → {M.usd(liq)}.")
            if mc and liq / mc < 0.002:                           # dust LP implying a huge MC: don't trust it
                rec.quote_state = "INVALID_POOL"
                return None
        if not price:
            return None
        rec.quote_state = "OK"
        # ---- valid print
        rec.last_valid_price, rec.last_valid_mc, rec.last_valid_at = price, mc, now
        if quote.get("traded", True):
            rec.last_trade_at = now
        if rec.peak_price is None or price > rec.peak_price:
            rec.peak_price, rec.peak_mc_usd, rec.peak_at = price, mc, now
        base = rec.first_alert_price if rec.official else rec.early_alert_price
        if base:
            rec.max_drawdown_pct = min(rec.max_drawdown_pct, (price / base - 1) * 100)
            for key, secs in T_MARKS:
                if key not in rec.price_at and start and now - start >= secs:
                    rec.price_at[key] = price
        # ---- quiet fade
        quiet = now - (rec.last_trade_at or now) >= self.cfg.dead_quiet_minutes * 60
        down = base and price <= base * (1 - self.cfg.dead_quiet_drop_pct / 100)
        if quiet and (down or (liq is not None and liq < self.cfg.dead_liq_usd * 2)):
            return self.end(rec, "DEAD", "no_trades", f"No trades for {self.cfg.dead_quiet_minutes:.0f}m.")
        return self._multipliers(rec, price, mc, now)

    def _multipliers(self, rec: Record, price: float, mc, now: float) -> Optional[str]:
        if not rec.official:
            return None
        m = rec.multiple(price)
        if m is None:
            return None
        due = [x for x in self.cfg.multipliers if m >= x and x not in rec.multipliers_posted]
        if not due:
            return None
        level = max(due)

        def reserve():
            rec.multipliers_posted = sorted(set(rec.multipliers_posted) | set(due))
            rec.last_posted_price = price                         # a win card also resets the "big move" baseline

        before = (list(rec.multipliers_posted), rec.last_posted_price)

        def rollback():
            rec.multipliers_posted, rec.last_posted_price = before

        res = self._send("wins", M.multiplier_embed(level, rec, price, mc, now), None, reserve, rollback)
        return f"{level}X" if res else None

    # ------------------------------------------------------------------ endings (§3.4)
    def end(self, rec: Record, typ: str, reason: str, detail: str = "") -> Optional[str]:
        """typ: INVALIDATED | DEAD. One shot per CA."""
        if rec.ended or rec.death_posted:
            return None
        now = self.clock()
        before = (rec.status, rec.death_posted, rec.death_reason, rec.death_at, rec.invalidation_reason)

        def reserve():
            rec.status = "invalidated" if typ == "INVALIDATED" else "dead"
            rec.death_posted, rec.death_reason, rec.death_at = True, reason, now
            if typ == "INVALIDATED":
                rec.invalidation_reason = reason

        def rollback():
            rec.status, rec.death_posted, rec.death_reason, rec.death_at, rec.invalidation_reason = before

        res = self._send("graveyard", M.ending_embed(typ, rec, reason, detail, now), None, reserve, rollback)
        if res and self.on_end:
            self.on_end(rec, reason, detail)
        return typ if res else None

    def invalidate(self, ca: str, reason: str, detail: str = "") -> Optional[str]:
        rec = self.store.get(ca)
        return self.end(rec, "INVALIDATED", reason, detail) if rec else None

    def kill(self, ca: str, reason: str, detail: str = "") -> Optional[str]:
        rec = self.store.get(ca)
        return self.end(rec, "DEAD", reason, detail) if rec else None

    def correct(self, ca: str, what: str) -> Optional[str]:
        """One-time CORRECTION for an ended coin (e.g. the first death post lacked the CA)."""
        rec = self.store.get(ca)
        if rec is None or "correction" in rec.corrections:
            return None
        res = self._send("graveyard", M.correction_embed(rec, what), None,
                         lambda: rec.corrections.append("correction"), lambda: rec.corrections.remove("correction"))
        return "CORRECTION" if res else None
