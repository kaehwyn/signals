"""Bot-level tests with every network call mocked (spec §11 P1/P2 items + ops)."""
import os
import re
import sys
import time

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)
import memecoin_bot as B  # noqa: E402
from spidey import catalyst, schedule  # noqa: E402
from spidey.config import Config, load_config  # noqa: E402
from spidey.store import PT  # noqa: E402

from test_lifecycle import FakePoster  # noqa: E402

PUMP = B.PUMP_SUPPLY_RAW


def wait(bot):
    for _ in range(200):
        if all(f.done() for _, f in bot.pending.values()):
            return
        time.sleep(0.005)


class World:
    def __init__(self, tmp_path, monkeypatch):
        os.chdir(tmp_path)
        self.t = [1_800_000_000.0]
        self.pairs = {}                 # mint -> list of dexscreener pair dicts
        self.rc = {}
        self.dev_bal = {}
        self.news = ("HYPE_ONLY", None, None)
        mp = monkeypatch
        mp.setattr(B, "fetch_sol_usd", lambda: 160.0)
        mp.setattr(B, "fetch_gecko_mints", lambda kind: [])
        mp.setattr(B, "fetch_dex_feed", lambda path: [])
        mp.setattr(B, "fetch_dex_all_pairs", lambda mints: {m: self.pairs[m] for m in mints if m in self.pairs})
        mp.setattr(B, "mint_info", lambda m: {"revoked": True, "supply": PUMP, "decimals": 6, "bad_ext": []})
        mp.setattr(B, "curve_state", lambda c: {"progress": 0.4, "reserve_sol": 25.0, "complete": False, "mcap_sol": 100})
        mp.setattr(B, "largest_holders", lambda m, exclude_owners=(): [(f"h{i}", 10_000_000 * 10 ** 6) for i in range(10)])
        mp.setattr(B, "fetch_rugcheck", lambda m: self.rc.get(m))
        mp.setattr(B, "token_balance", lambda owner, mint: self.dev_bal.get((owner, mint)))
        mp.setattr(B, "check_catalyst", lambda name, ticker: self.news)
        self.poster = FakePoster()
        self.cfg = Config()
        self.cfg.max_new_per_hour = 99
        self.bot = B.Bot(self.cfg, self.poster, feed=None, clock=lambda: self.t[0])

    def tick(self, n=1, step=31):
        for _ in range(n):
            self.t[0] += step
            self.bot.tick(self.t[0])
            wait(self.bot)
            self.bot.collect(self.t[0])
            wait(self.bot)
            self.bot.collect(self.t[0])

    def posts(self, channel=None):
        return [(c, e["title"]) for c, e, _ in self.poster.posts if channel is None or c == channel]


def dex_pair(mint, liq=40_000, mc=200_000, price=2e-4, dex="pumpswap", addr="POOL", m5=6, h1=40, created_min=90):
    return {"chainId": "solana", "baseToken": {"address": mint, "symbol": mint[:4], "name": mint[:4] + " coin"},
            "pairAddress": addr, "dexId": dex, "priceUsd": str(price), "marketCap": mc, "liquidity": {"usd": liq},
            "volume": {"m5": 20_000, "h1": 150_000}, "txns": {"m5": {"buys": 30, "sells": 10}, "h1": {"buys": 600, "sells": 300}},
            "priceChange": {"m5": m5, "h1": h1}, "pairCreatedAt": (1_800_000_000 - created_min * 60) * 1000}


def clean_rc(lp=None, holders=900, top10=18.0, creator="DEVX"):
    return {"holders": holders, "top1": 3.0, "top10": top10, "insiders": 0.0, "lp_locked": lp, "rugged": False, "dangers": [],
            "creator": creator, "dev": 0.5, "revoked": True}


@pytest.fixture
def w(tmp_path, monkeypatch):
    return World(tmp_path, monkeypatch)


def test_11_liquidity_gate_5k_goes_to_early_watch_only(w):
    m = "LIQ5K1111111111111111111111111111111111pump"
    w.pairs[m] = [dex_pair(m, liq=5_000, mc=40_000)]
    w.rc[m] = clean_rc()
    w.bot.add_pair(m, w.t[0])
    w.tick(4)
    assert not [p for p in w.posts("signals") if "NEW" in p[1]]
    assert [p for p in w.posts("early-watch") if p[1].startswith("NEW")]
    assert [c for ch, e, c in w.poster.posts if ch == "early-watch"][0].startswith("NOT A BUY CALL")


def test_12_unknown_lp_lock_rejected(w):
    m = "LPUNKNOWN111111111111111111111111111111111"             # not a pump.fun migration -> LP lock must be known
    w.pairs[m] = [dex_pair(m, dex="raydium")]
    w.rc[m] = clean_rc(lp=None)
    w.bot.add_pair(m, w.t[0])
    w.tick(4)
    assert not [p for p in w.posts("signals") if p[1].startswith("NEW")]


def test_clean_migrated_breakout_posts_signal_and_dev_sell_invalidates(w):
    m = "CLEAN1111111111111111111111111111111111pump"
    w.pairs[m] = [dex_pair(m)]
    w.rc[m] = clean_rc(lp=100.0, creator="DEVX")
    w.dev_bal[("DEVX", m)] = 5_000_000 * 10 ** 6
    w.news = ("REAL_CATALYST", "https://news.example/a", "Example: event coverage")
    w.bot.add_pair(m, w.t[0])
    w.tick(4)
    new = [e for c, e, _ in w.poster.posts if c == "signals" and e["title"].startswith("NEW")]
    assert len(new) == 1 and m in new[0]["description"] and "catalyst" in new[0]["description"]
    rec = w.bot.store.get(m)
    assert rec.official and rec.catalyst_tag == "REAL_CATALYST" and rec.dev_wallet == "DEVX"
    w.tick(3, step=61)                                        # baseline dev balance recorded
    w.dev_bal[("DEVX", m)] = 1_000_000 * 10 ** 6              # dev sells 0.4% of supply
    w.tick(3, step=61)
    assert w.bot.store.get(m).status == "invalidated"
    assert "DEVX" in w.bot.store.blacklist
    n = len(w.poster.posts)
    w.pairs[m] = [dex_pair(m, price=9e-4)]                     # 4.5x later: nothing more for an invalidated coin
    w.tick(4)
    assert all(not t[1][0].isdigit() for t in w.posts()[n:]) and not [p for p in w.posts()[n:] if m in str(p)]


def curve_launch(w, mint, creator="CDEV"):
    t = w.t[0]
    ev = [(t, "launch", mint, {"txType": "create", "mint": mint, "name": "Curve Coin", "symbol": "CURVE",
                               "traderPublicKey": creator, "bondingCurveKey": "CURVEKEY", "initialBuy": 10_000_000, "marketCapSol": 40})]
    mc = 60.0
    for i in range(120):
        ts = t + 3 + i * 5
        mc *= 1.004
        ev.append((ts, "trade", mint, {"txType": "buy", "mint": mint, "traderPublicKey": f"b{i}", "solAmount": 0.5,
                                       "tokenAmount": 1_000_000, "newTokenBalance": 1_000_000, "marketCapSol": mc}))
    w.bot.on_feed(ev)
    w.t[0] = t + 600
    return mc


def test_curve_coin_early_watch_then_dev_sell_invalidates(w):
    m = "CURVE111111111111111111111111111111111pump"
    mc = curve_launch(w, m)
    w.tick(3, step=21)
    first = [(c, t) for c, t in w.posts() if t.startswith("NEW")]
    assert first and first[0][0] in ("early-watch", "signals")
    w.bot.on_feed([(w.t[0], "trade", m, {"txType": "sell", "mint": m, "traderPublicKey": "CDEV", "solAmount": 3,
                                          "tokenAmount": 10_000_000, "newTokenBalance": 0, "marketCapSol": mc * 0.9})])
    assert w.bot.store.get(m).status == "invalidated"
    assert w.posts()[-1][1].startswith("INVALIDATED") and w.posts()[-1][0] == first[0][0]   # posted where the original is


def test_scheduled_tracker_once_per_slot_and_late_catchup(w):
    from datetime import datetime
    slot = datetime(2027, 1, 5, 10, 0, tzinfo=PT).timestamp()   # 10:00 AM PT
    w.t[0] = slot + 30
    w.bot.store.jobs["tracker"] = slot - 7200
    w.bot.ops(w.t[0])
    w.bot.ops(w.t[0] + 60)
    assert [t for c, t in w.posts("signals") if t.startswith("24h tracker")] == [w.posts("signals")[0][1]]
    w.t[0] = slot + 2 * 7200 + 3 * 3600                           # process was down across several slots
    w.bot.ops(w.t[0])
    trackers = [t for c, t in w.posts("signals") if t.startswith("24h tracker")]
    assert len(trackers) == 2 and trackers[-1].endswith("(late)")


def test_heartbeat_and_boot_go_to_bot_channel_only(w):
    w.bot.boot_message()
    w.bot.ops(w.t[0])
    assert {c for c, _ in w.posts()} <= {"status", "signals", "recap"}
    hb = [e for c, e, _ in w.poster.posts if e["title"] == "heartbeat"][0]["description"]
    assert "feed:" in hb and "launches seen" in hb and "watching" in hb and "New Pairs 0" in hb and "Final Stretch 0" in hb
    beats = lambda: len([1 for c, t in w.posts("status") if t == "heartbeat"])
    w.bot.ops(w.t[0] + 59 * 60)
    assert beats() == 1                                      # hourly, not every 15 min
    w.bot.ops(w.t[0] + 61 * 60)
    assert beats() == 2
    assert any(t == "heartbeat" for c, t in w.posts("status")) and any(t == "restart" for c, t in w.posts("status"))


def test_schedule_slots_pt():
    from datetime import datetime
    cfg = Config()
    t = datetime(2027, 1, 5, 21, 7, tzinfo=PT).timestamp()
    assert datetime.fromtimestamp(schedule.last_slot("leaderboard", t, cfg), PT).strftime("%H:%M") == "21:00"
    assert datetime.fromtimestamp(schedule.last_slot("postmortem", t, cfg), PT).strftime("%H:%M") == "21:05"
    assert datetime.fromtimestamp(schedule.last_slot("tracker", t, cfg), PT).strftime("%H:%M") == "20:00"
    night = datetime(2027, 1, 6, 3, 30, tzinfo=PT).timestamp()
    assert datetime.fromtimestamp(schedule.last_slot("recap", night, cfg), PT).strftime("%H:%M") == "22:00"


def test_catalyst_judge():
    items = [{"title": "Kitty Cat memecoin surges 900% - CoinBlog", "link": "https://coinblog.example/a", "source": "CoinBlog",
              "source_url": "https://coinblog.example", "pub": None},
             {"title": "GTA VI pre-load opens on PlayStation - Polygon", "link": "https://polygon.example/gta", "source": "Polygon",
              "source_url": "https://www.polygon.com", "pub": None}]
    assert catalyst.judge(items, ["GTA"])[0] == "REAL_CATALYST"
    assert catalyst.judge(items, ["Kitty"])[0] == "HYPE_ONLY"     # coin-pump coverage doesn't count
    assert catalyst.keywords("The Cat Coin", "CAT") == []          # nothing specific to search


def test_config_env_and_file_override(tmp_path):
    p = tmp_path / "config.json"
    p.write_text('{"min_liquidity_usd": 12000, "dev_sell_invalidates": false}')
    cfg = load_config(str(p), env={"SPIDEY_UPDATE_COOLDOWN_MINUTES": "15", "SPIDEY_MIN_LIQUIDITY_USD": "9000"})
    assert cfg.min_liquidity_usd == 9000 and cfg.update_cooldown_minutes == 15 and cfg.dev_sell_invalidates is False
    assert load_config(None, env={"SPIDEY_HEARTBEAT_MINUTES": "0.5"}).heartbeat_minutes == 0.5      # float, not int


def test_20_no_user_token_code_path():
    bad = re.compile(r"users/@me|/guilds/\d+/messages|Authorization['\"]?\s*[:=]\s*['\"]?(?!Bot)|discord\.com/api/v\d+/channels/\d+/messages", re.I)
    for root, _, files in os.walk(ROOT):
        if ".git" in root or "tests" in root:
            continue
        for f in files:
            if f.endswith(".py"):
                assert not bad.search(open(os.path.join(root, f)).read()), f


# ================================================================ v8
def test_pumpswap_with_rugcheck_lp_0_is_rejected(w):
    m = "TROLL1111111111111111111111111111111111pump"            # ...pump mint on PumpSwap is NOT assumed burned
    w.pairs[m] = [dex_pair(m)]
    w.rc[m] = clean_rc(lp=0.0)
    w.news = ("REAL_CATALYST", "https://news.example/a", "x")
    w.bot.add_pair(m, w.t[0])
    w.tick(5)
    assert not [p for p in w.posts() if p[1].startswith("NEW")]   # not official, not early-watch
    assert w.bot.store.get(m) is None


def test_live_signal_is_invalidated_when_lp_turns_out_unlocked(w):
    m = "LPFLIP111111111111111111111111111111111pump"
    w.pairs[m] = [dex_pair(m)]
    w.rc[m] = clean_rc(lp=100.0)
    w.news = ("REAL_CATALYST", "https://news.example/a", "x")
    w.bot.add_pair(m, w.t[0])
    w.tick(4)
    assert w.bot.store.get(m).official
    w.rc[m] = clean_rc(lp=0.0)
    w.tick(12)
    rec = w.bot.store.get(m)
    assert rec.status == "invalidated" and rec.invalidation_reason == "lp_unlocked"
    assert w.posts("signals")[-1][1].startswith("INVALIDATED")


def test_curve_signal_rechecks_lp_right_after_migration(w):
    m = "CURVE111111111111111111111111111111111pump"
    curve_launch(w, m)
    w.tick(3, step=21)
    rec = w.bot.store.get(m)
    assert rec and rec.stage in ("new_pairs", "final_stretch") and rec.bonding_pct is not None
    w.rc[m] = clean_rc(lp=0.0, creator="CDEV")
    w.pairs[m] = [dex_pair(m, price=rec.last_valid_price, mc=rec.last_valid_mc, addr="NEWPOOL")]
    w.tick(3, step=61)                                        # migrated -> LP lock checked immediately -> 0% -> out
    rec = w.bot.store.get(m)
    assert rec.migrated_at and rec.status == "invalidated" and rec.invalidation_reason == "lp_unlocked"


def gate(cfg, **kw):
    """Minimal observation for stage_gates."""
    o = {"stage": "migrated", "hard_fails": [], "gate_fails": [], "momentum": True, "score": 90, "breakout": True,
         "bundle_known": True, "mc": 30_000, "liq": 30_000, "lp_locked": 100.0, "pool_age_min": 120, "bonding_pct": None,
         "buy_sell_5m": 2.0,
         "metrics": {"age_s": 900, "buyers": 60, "holders": 600, "top10": 20.0, "dev": 1.0, "bundler": 5.0, "sniper": 5.0}}
    m = kw.pop("metrics", {})
    o.update(kw)
    o["metrics"].update(m)
    return B.stage_gates(o, cfg)


def test_stage_new_pairs_never_official_and_early_watch_thresholds():
    cfg = Config()
    ok = dict(stage="new_pairs", bonding_pct=30.0, liq=None, lp_locked=None)
    official, early = gate(cfg, **ok)
    assert official and not early                              # early-watch only, even with a 90 score
    for bad in (dict(metrics={"age_s": 200}), dict(mc=9_000), dict(metrics={"buyers": 15}), dict(metrics={"bundler": 30.0}),
                dict(bundle_known=False), dict(metrics={"dev": 6.0}), dict(metrics={"top10": 44.0}), dict(metrics={"top10": None})):
        assert gate(cfg, **{**ok, **bad})[1], bad


def test_stage_final_stretch_needs_bonding_buyers_and_score():
    cfg = Config()
    ok = dict(stage="final_stretch", bonding_pct=82.0, liq=None, lp_locked=None)
    assert gate(cfg, **ok) == ([], [])
    assert gate(cfg, **{**ok, "score": 70})[0] and not gate(cfg, **{**ok, "score": 70})[1]    # early-watch, not official
    assert gate(cfg, **{**ok, "metrics": {"buyers": 20}})[0]
    assert gate(cfg, **{**ok, "buy_sell_5m": 0.9})[0]
    assert gate(cfg, **{**ok, "metrics": {"top10": 44.0}})[1]                                  # early-watch enforces top 10 too
    assert B.curve_stage(69.0, cfg) == "new_pairs" and B.curve_stage(70.0, cfg) == "final_stretch"
    assert B.curve_stage(None, cfg) == "new_pairs"                                             # unknown = the stricter lane


def test_stage_migrated_rules():
    cfg = Config()
    assert gate(cfg) == ([], [])
    off, early = gate(cfg, pool_age_min=12, metrics={"holders": 300})                          # first 30 min: needs 450 holders
    assert off and not early
    assert gate(cfg, pool_age_min=12, metrics={"holders": 500}) == ([], [])
    assert gate(cfg, bundle_known=False, score=70, metrics={"bundler": None})[0]               # bundler n/a and "just 70"
    assert not gate(cfg, bundle_known=False, score=78, metrics={"bundler": None})[0]
    off, early = gate(cfg, metrics={"dev": 3.3}, gate_fails=["dev holds 3.3%"])                # FRONK
    assert off and early
    assert gate(cfg, metrics={"top10": 44.0})[1]                                               # TROLL in early-watch
    off, early = gate(cfg, lp_locked=None, gate_fails=["LP lock unknown"])
    assert off and not early                                                                   # unknown LP: early-watch at most
    assert gate(cfg, lp_locked=0.0, gate_fails=["LP locked 0%"])[1]                            # known-unlocked: nowhere


def test_bonding_progress_from_feed_and_stage_on_observation(w):
    l = B.Launch(mint="X" * 40 + "pump", name="n", symbol="S", creator="D", curve="C", t0=w.t[0])
    assert l.bonding_pct() is None
    l.on_trade(w.t[0] + 1, {"txType": "buy", "traderPublicKey": "a", "vTokensInBondingCurve": 279_900_000 + 793_100_000 * 0.25})
    assert l.bonding_pct() == pytest.approx(75.0)
    l.chain = {"curve": {"progress": 0.4, "reserve_sol": 1.0}}
    assert l.bonding_pct() == pytest.approx(75.0)              # live feed wins over the older RPC read
    o = B.curve_observation(l, w.t[0] + 2, 160.0, w.cfg, False, 0)
    assert o["stage"] == "final_stretch" and o["bonding_pct"] == pytest.approx(75.0)


class FeedStub:
    connected, err = True, ""

    def status(self):
        return "connected" if self.connected else "reconnecting (boom)"

    def drain(self):
        return []

    def watch(self, m):
        pass

    unwatch = watch


def test_feed_drop_warns_status_channel_once_and_reports_recovery(w):
    w.bot.feed = FeedStub()
    w.bot.store.meta["last_heartbeat"] = w.t[0]
    w.bot.ops(w.t[0])
    w.bot.feed.connected = False
    for dt in (10, 60, 100, 200, 300):
        w.bot.ops(w.t[0] + dt)
    warn = [e["description"] for c, e, _ in w.poster.posts if c == "status" and "feed" in e["description"]]
    assert len(warn) == 1 and "DOWN" in warn[0] and "only migrated" in warn[0]
    w.bot.feed.connected = True
    w.bot.ops(w.t[0] + 400)
    assert sum("reconnected" in e["description"] for c, e, _ in w.poster.posts if c == "status") == 1


def test_early_watch_coin_is_polled_and_dies_on_collapse(w):
    m = "LIQ5K1111111111111111111111111111111111pump"
    w.pairs[m] = [dex_pair(m, liq=5_000, mc=40_000)]
    w.rc[m] = clean_rc()
    w.bot.add_pair(m, w.t[0])
    w.tick(4)
    assert w.bot.store.get(m) and not w.bot.store.get(m).official
    w.pairs[m] = [dex_pair(m, liq=1_500, mc=400, price=2e-6)]  # fomo: -99%, $1.5K liquidity left
    w.tick(3)
    assert w.bot.store.get(m).status == "dead"
    assert w.posts()[-1][1].startswith("DEAD")
