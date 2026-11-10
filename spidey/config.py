"""All tunables in one place. Every value is a default you can change WITHOUT touching code:

  1. config.json in the repo root:      {"min_liquidity_usd": 10000, "update_cooldown_minutes": 15}
  2. or an environment variable:        SPIDEY_MIN_LIQUIDITY_USD=10000
     (env wins over config.json; booleans accept true/false/1/0)
"""
import json
import os
from dataclasses import dataclass, fields


@dataclass
class Config:
    # ---------------- §12 spec defaults
    min_liquidity_usd: float = 8000
    early_min_liquidity_usd: float = 2000
    dead_liq_usd: float = 500
    min_lp_locked_or_burned_pct: float = 95
    max_top10_pct: float = 40
    top10_warn_pct: float = 30
    max_dev_holdings_pct: float = 8
    max_bundler_pct: float = 25
    max_sniper_pct: float = 35
    max_fresh_wallet_pct: float = 60            # not computable on free data yet -> shown n/a, gate skipped
    fresh_wallet_age_hours: float = 24
    min_holders: int = 40
    early_min_holders: int = 20
    early_min_score: int = 50
    signals_min_score: int = 60                 # NEW in #signals for migrated coins
    promote_score: int = 70
    curve_signals_min_score: int = 75           # Final Stretch -> official #signals
    curve_min_unique_buyers: int = 30
    score_cap_without_bundle_data: int = 70
    dev_sell_invalidates: bool = True
    dev_sell_dust_pct: float = 0.1
    material_score_delta: int = 10
    material_move_pct: float = 50
    update_cooldown_minutes: float = 10
    dead_quiet_minutes: float = 180
    dead_quiet_drop_pct: float = 70
    death_liq_drop_pct: float = 80              # liquidity down this much from its alert/peak level, at any time -> dead
    death_price_drop_pct: float = 90            # price down this much from the alert or the peak -> dead
    pool_missing_minutes: float = 10            # primary pool gone from DexScreener this long = liquidity_pulled
    health_poll_stale_sec: float = 120
    health_port: int = 0                        # 0 = no HTTP health endpoint (set e.g. 8080 on a VPS)
    heartbeat_minutes: float = 60
    feed_down_warn_s: float = 90               # pump.fun feed disconnected this long -> warn #spidey-status
    poll_gap_alert_minutes: float = 5
    tracker_every_hours: int = 2
    recap_start_hour: int = 8
    recap_end_hour: int = 22
    recap_overnight: bool = False
    recap_skip_if_empty_overnight: bool = True
    leaderboard_time: str = "21:00"
    postmortem_time: str = "21:05"
    catalyst_boost: int = 8
    hype_score_penalty: int = 0
    catalyst_retry_minutes: float = 60
    confluence_boost_per_source: int = 10
    confluence_boost_cap: int = 20
    confluence_lookback_hours: float = 3
    scanner_fail_warn_count: int = 10
    scanner_fail_warn_minutes: float = 10
    # ---------------- stage filters (Axiom Pulse stages)
    final_stretch_min_bonding_pct: float = 70   # curve coin at/above this bonding % = Final Stretch, below = New Pairs
    np_min_age_min: float = 8                   # New Pairs: early-watch only, never official
    np_min_mcap: float = 13000
    np_min_buyers: int = 25
    np_max_bundler_pct: float = 22
    np_max_dev_pct: float = 4
    np_max_top10_pct: float = 38
    migrated_max_dev_pct: float = 3             # dev holding more than this at alert time blocks a migrated coin
    migrated_young_minutes: float = 30          # first minutes after migration = high risk
    migrated_young_min_holders: int = 450
    migrated_young_max_top10_pct: float = 40
    # ---------------- monitoring cadence
    active_poll_s: float = 30
    early_poll_s: float = 60
    safety_recheck_s: float = 300               # RugCheck / authority re-check for live coins
    dev_check_s: float = 60                     # dev wallet balance check for live coins (dev-sell invalidation)
    track_hours: float = 24                     # after this a live coin is archived (tracker keeps its outcome)
    records_keep_days: float = 30               # a CA is never re-posted as NEW while its record exists
    multipliers: tuple = (2, 3, 5)
    # ---------------- discovery (curve coins, live pump.fun feed)
    curve_min_age_min: float = 5
    curve_max_age_min: float = 120
    curve_min_mcap: float = 8000
    curve_max_mcap: float = 150000
    curve_min_buy_sell_5m: float = 1.1
    curve_max_drawdown: float = 0.30
    bundle_window_s: float = 1.5                # bought ~same block as the launch
    sniper_window_s: float = 15
    max_creator_launches_24h: int = 3
    max_trade_subs: int = 400
    # ---------------- discovery (DEX pairs)
    max_pair_age_h: float = 24
    max_mcap_usd: float = 5_000_000
    max_pairs: int = 800
    pair_refresh_s: float = 30
    breakout_min_vol_mcap: float = 0.4
    breakout_min_liq_mcap: float = 0.08
    breakout_min_buy_sell_h1: float = 1.3
    breakout_min_chg_m5: float = 3
    breakout_min_chg_h1: float = 10
    breakout_max_chg_h1: float = 200
    # ---------------- rug-dev blacklist (any watched coin)
    rug_dev_sold_share: float = 0.80
    rug_price_drop: float = 0.50
    rug_min_pump: float = 1.30
    rug_min_victims: int = 5
    rug_liq_drop: float = 0.70
    rug_liq_window_s: float = 900
    rug_min_liq: float = 5000
    blacklist_days: float = 30
    blacklist_max: int = 30000
    # ---------------- posting
    post_endings: bool = True                   # DEAD / INVALIDATED post for rugs, in the channel of the original alert
    post_fade_endings: bool = False             # also post quiet fades (no trades for hours); off = tracked silently
    post_postmortem: bool = False               # daily "why it died" post
    max_new_per_hour: int = 8
    # ---------------- files
    state_file: str = "state.json"
    backup_dir: str = "backups"
    backup_keep_days: int = 7
    log_file: str = "radar_log.csv"
    signals_file: str = "signals.jsonl"
    status_file: str = "last_status.txt"
    git_sync_minutes: float = 0                 # >0: commit + push state every N minutes (GitHub Actions)


def _coerce(typ, raw):
    """Convert by the DECLARED field type (a float setting with a whole-number default must stay a float)."""
    typ = typ if isinstance(typ, str) else getattr(typ, "__name__", str(typ))
    if typ == "bool":
        return raw if isinstance(raw, bool) else str(raw).strip().lower() in ("1", "true", "yes", "on")
    if typ == "int":
        return int(float(raw))
    if typ == "float":
        return float(raw)
    if typ == "tuple":
        return tuple(int(x) for x in (raw if isinstance(raw, (list, tuple)) else str(raw).split(",")))
    return str(raw)


def load_config(path: str = "config.json", env=os.environ) -> Config:
    cfg = Config()
    overrides = {}
    if path and os.path.exists(path):
        with open(path) as f:
            overrides.update(json.load(f))
    for f in fields(Config):
        key = f"SPIDEY_{f.name.upper()}"
        if key in env:
            overrides[f.name] = env[key]
    types = {f.name: f.type for f in fields(Config)}
    for name, raw in overrides.items():
        if name in types:
            setattr(cfg, name, _coerce(types[name], raw))
        else:
            print(f"[config] unknown setting ignored: {name}")
    return cfg
