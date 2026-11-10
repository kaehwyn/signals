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
    w.rc[m] = clean_rc(creator="DEVX")
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
    assert not w.posts("graveyard")                          # endings are silent by default


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
