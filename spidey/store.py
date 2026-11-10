"""Durable bot state: one record per CA (mint address), plus blacklist, repeat-launcher counts and job clocks.

Saved as JSON with an atomic write (temp file -> fsync -> rename), so a crash mid-write can't corrupt it.
A dated copy is kept in backups/ once a day (7 days kept). Counts are always recomputed from the records.
"""
import glob
import json
import os
import shutil
import time
from dataclasses import asdict, dataclass, field, fields
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

PT = ZoneInfo("America/Los_Angeles")
STORE_VERSION = 7

STATUSES = ("active", "invalidated", "dead", "archived")
DEATH_REASONS = ("liquidity_pulled", "price_collapse", "dev_sold", "dev_blacklisted", "lp_unlocked", "authority_revived",
                 "no_trades", "safety_fail", "manual")
RUG_REASONS = ("liquidity_pulled", "price_collapse", "dev_sold", "dev_blacklisted", "lp_unlocked", "authority_revived", "safety_fail")
STAGES = ("new_pairs", "final_stretch", "migrated")


def norm_ca(ca: str) -> str:
    """Base58 is case-sensitive: trim whitespace only."""
    return (ca or "").strip()


def fmt_pt(ts: Optional[float], with_day: bool = False) -> str:
    if not ts:
        return "n/a"
    d = datetime.fromtimestamp(ts, PT)
    s = d.strftime("%-I:%M %p PT")
    return (d.strftime("%b %-d ") + s) if with_day else s


@dataclass
class Record:
    ca: str
    ticker: str = "?"
    name: str = ""
    status: str = "active"
    official: bool = False                  # True once it is (or was promoted to) a #signals alert
    source: str = "dex"                     # "curve" (pump.fun bonding curve) or "dex"
    stage: Optional[str] = None             # stage at the FIRST post: new_pairs | final_stretch | migrated
    bonding_pct: Optional[float] = None     # bonding-curve progress at the first post (None for migrated)
    official_stage: Optional[str] = None    # stage when it became an official #signals alert
    official_bonding_pct: Optional[float] = None
    # ---- official baseline (multiplier math uses ONLY these)
    first_alert_at: Optional[float] = None
    first_alert_price: Optional[float] = None       # USD
    first_alert_price_sol: Optional[float] = None
    first_alert_mc_usd: Optional[float] = None
    first_alert_liq_usd: Optional[float] = None     # None on the bonding curve
    first_alert_message_id: Optional[str] = None
    first_alert_jump: Optional[str] = None
    first_alert_channel: str = "signals"            # "signals" | "early-watch"
    promoted_to_signals_at: Optional[float] = None
    # ---- early-watch baseline (kept for reference after promotion)
    early_alert_at: Optional[float] = None
    early_alert_price: Optional[float] = None
    early_alert_mc_usd: Optional[float] = None
    early_jump: Optional[str] = None
    # ---- latest
    last_score: Optional[int] = None
    last_mc_usd: Optional[float] = None
    last_price: Optional[float] = None
    last_liq_usd: Optional[float] = None
    peak_liq_usd: Optional[float] = None            # highest liquidity of the primary pool since the alert
    last_update_at: Optional[float] = None          # last POSTED update (not last poll)
    last_posted_price: Optional[float] = None
    last_posted_score: Optional[int] = None
    last_polled_at: Optional[float] = None
    last_trade_at: Optional[float] = None
    # ---- valid-price tracking (never from a drained / inconsistent pool)
    last_valid_price: Optional[float] = None
    last_valid_mc: Optional[float] = None
    last_valid_at: Optional[float] = None
    quote_state: str = "OK"                         # OK | STALE | INVALID_POOL
    missing_since: Optional[float] = None
    peak_price: Optional[float] = None
    peak_mc_usd: Optional[float] = None
    peak_at: Optional[float] = None
    max_drawdown_pct: float = 0.0
    price_at: dict = field(default_factory=dict)    # "t+5m" -> price, ...
    multipliers_posted: list = field(default_factory=list)
    breakout_active: bool = False
    # ---- endings
    death_posted: bool = False
    death_reason: Optional[str] = None
    death_at: Optional[float] = None
    invalidation_reason: Optional[str] = None
    corrections: list = field(default_factory=list)
    # ---- catalyst / confluence
    catalyst_tag: str = "UNCHECKED"                 # REAL_CATALYST | HYPE_ONLY | UNCHECKED
    catalyst_url: Optional[str] = None
    catalyst_summary: Optional[str] = None
    catalyst_first_try_at: Optional[float] = None
    catalyst_rerun_done: bool = False
    confluence: list = field(default_factory=list)
    # ---- chain
    dev_wallet: Optional[str] = None
    dev_balance: Optional[float] = None             # raw units, last seen
    dev_peak_balance: Optional[float] = None
    pool: Optional[str] = None                      # primary pool / pair address
    pool_dex: Optional[str] = None
    curve_key: Optional[str] = None
    migrated_at: Optional[float] = None
    liq_hist: list = field(default_factory=list)    # [[t, liq_usd], ...] last ~40
    safety_snapshot: dict = field(default_factory=dict)
    safety_checked_at: Optional[float] = None

    def multiple(self, price: Optional[float]) -> Optional[float]:
        if not price or not self.first_alert_price:
            return None
        return price / self.first_alert_price

    @property
    def peak_multiple(self) -> Optional[float]:
        return self.multiple(self.peak_price)

    @property
    def ended(self) -> bool:
        return self.status in ("dead", "invalidated")


def stage_of(r: Record) -> str:
    """Stage a record counts under in the stats (older records without a stored stage are inferred)."""
    st = (r.official_stage if r.official else None) or r.stage
    if st:
        return st
    if r.source != "curve":
        return "migrated"
    return "final_stretch" if (r.first_alert_mc_usd or r.early_alert_mc_usd or 0) >= 25_000 else "new_pairs"


RECORD_FIELDS = {f.name for f in fields(Record)}


def record_from_dict(d: dict) -> Optional[Record]:
    if not isinstance(d, dict) or not d.get("ca"):
        return None
    return Record(**{k: v for k, v in d.items() if k in RECORD_FIELDS})


class Store:
    def __init__(self, path: str = "state.json", backup_dir: str = "backups", backup_keep_days: int = 7, clock=time.time):
        self.path, self.backup_dir, self.keep, self.clock = path, backup_dir, backup_keep_days, clock
        self.records: dict = {}
        self.blacklist: dict = {}           # dev wallet -> [time, reason]
        self.creator_times: dict = {}       # dev wallet -> [launch times] (repeat launchers only)
        self.jobs: dict = {}                # job name -> last slot time it ran for
        self.meta: dict = {}
        self.load_error: Optional[str] = None
        self.loaded = False

    # ---------------- records
    def get(self, ca: str) -> Optional[Record]:
        return self.records.get(norm_ca(ca))

    def create(self, ca: str, **kw) -> Record:
        ca = norm_ca(ca)
        if ca in self.records:
            raise ValueError(f"record exists: {ca}")
        r = Record(ca=ca, **kw)
        self.records[ca] = r
        return r

    def official(self):
        return [r for r in self.records.values() if r.official]

    # ---------------- persistence
    def load(self) -> bool:
        if not os.path.exists(self.path):
            self.load_error = "no state file (first run)"
            return False
        try:
            with open(self.path) as f:
                st = json.load(f)
        except Exception as e:
            self.load_error = f"state unreadable: {e}"
            bak = sorted(glob.glob(os.path.join(self.backup_dir, "state-*.json")))
            if bak:                                  # fall back to the newest daily backup
                try:
                    with open(bak[-1]) as f:
                        st = json.load(f)
                    self.load_error += f" -> restored {os.path.basename(bak[-1])}"
                except Exception:
                    return False
            else:
                return False
        if not isinstance(st, dict):
            self.load_error = "state not an object"
            return False
        if st.get("store_version") == STORE_VERSION:
            for d in (st.get("records") or {}).values():
                r = record_from_dict(d)
                if r:
                    self.records[r.ca] = r
            self.jobs = dict(st.get("jobs") or {})
            self.meta = dict(st.get("meta") or {})
        else:
            self._migrate_v6(st)
        bl = st.get("blacklist") or {}
        self.blacklist = {k: [self.clock(), "old"] for k in bl} if isinstance(bl, list) else dict(bl)
        self.creator_times = {k: list(v) for k, v in (st.get("creator_times") or {}).items()}
        self.loaded = True
        return True

    def _migrate_v6(self, st: dict):
        """SCANNER-6.0 kept 'calls'; turn them into records so they are never re-posted as NEW."""
        for c in st.get("calls") or []:
            if not isinstance(c, dict) or not c.get("mint"):
                continue
            dead = bool(c.get("dead"))
            r = Record(ca=norm_ca(c["mint"]), ticker=c.get("symbol") or "?", official=True,
                       source="curve" if c.get("kind") == "early" else "dex",
                       status="dead" if dead else ("archived" if c.get("closed") else "active"),
                       first_alert_at=c.get("t0"), first_alert_price=c.get("price0"),
                       last_price=c.get("last_price"), last_valid_price=c.get("last_price"), last_valid_at=c.get("last_seen"),
                       peak_price=c.get("peak"), dev_wallet=c.get("creator") or None,
                       multipliers_posted=[x for x in (c.get("hit") or []) if x in (2, 3, 5)],
                       death_posted=dead, death_reason="liquidity_pulled" if dead and "liquid" in (c.get("why_dead") or "") else (
                           "manual" if dead else None), death_at=c.get("last_seen") if dead else None)
            self.records[r.ca] = r

    def save(self):
        st = {"store_version": STORE_VERSION, "saved_at": self.clock(),
              "records": {ca: asdict(r) for ca, r in self.records.items()},
              "blacklist": self.blacklist, "creator_times": self.creator_times, "jobs": self.jobs, "meta": self.meta}
        d = os.path.dirname(os.path.abspath(self.path))
        tmp = self.path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(st, f, indent=0, separators=(",", ":"), default=str)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self.path)
        try:
            fd = os.open(d, os.O_RDONLY)
            os.fsync(fd)
            os.close(fd)
        except OSError:
            pass
        self._backup()

    def _backup(self):
        day = datetime.fromtimestamp(self.clock(), PT).strftime("%Y-%m-%d")
        os.makedirs(self.backup_dir, exist_ok=True)
        target = os.path.join(self.backup_dir, f"state-{day}.json")
        if not os.path.exists(target):
            shutil.copyfile(self.path, target)
        for old in sorted(glob.glob(os.path.join(self.backup_dir, "state-*.json")))[:-self.keep]:
            os.remove(old)

    def prune(self, now: float, records_keep_days: float, blacklist_days: float, blacklist_max: int):
        cut = now - records_keep_days * 86400
        for ca in [ca for ca, r in self.records.items() if (r.first_alert_at or r.early_alert_at or now) < cut]:
            del self.records[ca]
        bl_cut = now - blacklist_days * 86400
        bl = sorted(((k, v) for k, v in self.blacklist.items() if v[0] >= bl_cut), key=lambda kv: kv[1][0])
        self.blacklist = dict(bl[-blacklist_max:])
