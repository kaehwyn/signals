"""Spec §11 acceptance tests (P0 + lifecycle parts of P1/P2). Run: python -m pytest -q"""
import os
import sys

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from spidey import messages as M  # noqa: E402
from spidey.config import Config  # noqa: E402
from spidey.lifecycle import Lifecycle, Poster  # noqa: E402
from spidey.store import Store  # noqa: E402

CA = "DaKiTTy8xQ1111111111111111111111111111pump"


class FakePoster(Poster):
    def __init__(self):
        self.posts, self.fail, self.n = [], False, 0

    def send(self, channel, embed, content=None):
        if self.fail:
            return None
        self.n += 1
        self.posts.append((channel, embed, content))
        return {"id": str(self.n), "jump": f"https://discord.com/channels/1/2/{self.n}"}

    def types(self):
        return [e["title"].split("  •  ")[0] for _, e, _ in self.posts]


class Env:
    def __init__(self, tmp_path):
        self.t = [1_800_000_000.0]
        self.path = str(tmp_path / "state.json")
        self.poster = FakePoster()
        self.cfg = Config()
        self.boot()

    def boot(self):
        """(Re)start the 'process': fresh objects, state loaded from disk."""
        self.store = Store(self.path, backup_dir=os.path.dirname(self.path) + "/bk", clock=self.clock)
        self.store.load()
        self.lc = Lifecycle(self.store, self.cfg, self.poster, clock=self.clock)

    def clock(self):
        return self.t[0]

    def advance(self, minutes):
        self.t[0] += minutes * 60


def obs(price=1.0, score=70, mc=100_000, liq=20_000, breakout=False, signals=True, early=False, ca=CA, **kw):
    o = {"ca": ca, "ticker": "DAKITTY", "name": "Da Kitty", "price": price, "mc": mc, "liq": liq, "score": score,
         "qualifies_signals": signals, "qualifies_early": early, "breakout": breakout, "pool": "POOL1", "pool_dex": "pumpswap",
         "metrics": {}, "checks": ["✅ Mint & freeze revoked"], "reasons": ["volume 1.2x MC"], "safety": {"fails": []}}
    o.update(kw)
    return o


def q(price, liq=20_000, mc=None, pool="POOL1", dex="pumpswap", traded=True):
    return {"price": price, "mc": mc if mc is not None else price * 100_000, "liq": liq, "pool": pool, "dex": dex, "traded": traded}


@pytest.fixture
def env(tmp_path):
    return Env(tmp_path)


# ---------------------------------------------------------------- P0
def test_1_dedupe_across_restarts(env):
    assert env.lc.on_candidate(obs()) == "NEW"
    env.advance(40)
    env.boot()
    assert env.lc.on_candidate(obs(price=1.1)) is None            # no material change -> silent
    env.boot()
    assert env.lc.on_candidate(obs(price=1.6)) == "UPDATE"        # +60%
    assert env.poster.types() == ["NEW", "UPDATE"]
    assert "What changed" in env.poster.posts[-1][1]["description"]


def test_2_dakitty_breakout_labeled(env):
    env.lc.on_candidate(obs())
    env.advance(42)
    assert env.lc.on_candidate(obs(price=1.05)) is None
    env.advance(5)
    assert env.lc.on_candidate(obs(price=1.1, breakout=True, breakout_reasons=["vol/MC 1.6x"])) == "BREAKOUT"
    env.advance(1)
    assert env.lc.on_candidate(obs(price=1.1, breakout=True)) is None    # trigger still on: no repeat
    assert env.poster.types() == ["NEW", "BREAKOUT"]


def test_3_mubbo_identical_payloads(env):
    env.lc.on_candidate(obs())
    env.lc.on_candidate(obs())
    assert env.poster.types() == ["NEW"]


def test_4_gpu_single_death_with_ca_and_jump(env):
    env.lc.on_candidate(obs())
    env.lc.kill(CA, "dev_sold", "Dev sold.")
    env.lc.kill(CA, "dev_sold", "Dev sold.")
    env.boot()
    env.lc.kill(CA, "dev_sold", "Dev sold.")
    deaths = [e for _, e, _ in env.poster.posts if e["title"].startswith("DEAD")]
    assert len(deaths) == 1
    assert CA in deaths[0]["description"] and "discord.com/channels" in deaths[0]["description"]
    assert env.poster.posts[-1][0] == "graveyard"


def test_5_kawaene_lp_pull_uses_last_valid(env):
    env.lc.on_candidate(obs(price=1.0))
    env.advance(5)
    env.lc.on_quote(CA, q(1.80, liq=25_000))
    env.advance(1)
    out = env.lc.on_quote(CA, q(0.91, liq=300))                   # drained pool implies 0.91x
    assert out == "DEAD"
    d = env.poster.posts[-1][1]["description"]
    assert "1.80x" in d and "0.91x" not in d and "may be stale" in d
    assert env.store.get(CA).death_reason == "liquidity_pulled"


def test_5b_lp_drain_above_dust_also_counts_as_pull(env):
    env.lc.on_candidate(obs(price=1.0))
    env.lc.on_quote(CA, q(1.5, liq=40_000))
    env.advance(2)
    assert env.lc.on_quote(CA, q(0.2, liq=2_000)) == "DEAD"      # -95% liquidity in 2 min


def test_6_zero_baseline(env):
    env.lc.on_candidate(obs(price=None))
    env.advance(5)
    assert env.lc.on_quote(CA, q(2.0)) is None                    # no multiple, no crash
    rec = env.store.get(CA)
    assert rec.multiple(2.0) is None
    assert "n/a" in M.ending_embed("DEAD", rec, "manual", "", env.clock())["description"]


def test_7_tracker_survives_restart(env):
    for i in range(28):
        env.lc.cfg.max_new_per_hour = 999
        env.lc.on_candidate(obs(ca=f"CA{i:040d}"))
        env.advance(10)
    before = M.tracker_stats(env.store.records.values(), env.clock())["alerts"]
    env.boot()
    after = M.tracker_stats(env.store.records.values(), env.clock())["alerts"]
    assert before == after == 28
    env.advance(30)
    env.boot()
    assert M.tracker_stats(env.store.records.values(), env.clock())["alerts"] == 28


def test_8_multiplier_once(env):
    env.lc.on_candidate(obs(price=1.0))
    env.advance(1)
    assert env.lc.on_quote(CA, q(2.1)) == "2X"
    env.advance(1)
    env.lc.on_quote(CA, q(1.5))
    env.advance(1)
    assert env.lc.on_quote(CA, q(2.2)) is None
    env.boot()
    env.advance(1)
    assert env.lc.on_quote(CA, q(2.3)) is None
    assert env.lc.on_quote(CA, q(3.1)) == "3X"
    assert env.poster.types().count("2X") == 1 and env.poster.types().count("3X") == 1
    assert all(ch == "wins" for ch, e, _ in env.poster.posts if e["title"][0].isdigit())


def test_failed_send_is_retried_not_lost(env):
    env.poster.fail = True
    assert env.lc.on_candidate(obs()) is None
    assert env.store.get(CA) is None
    env.poster.fail = False
    assert env.lc.on_candidate(obs()) == "NEW"


def test_no_resurrect_after_death(env):
    env.lc.on_candidate(obs())
    env.lc.kill(CA, "liquidity_pulled")
    env.advance(60)
    assert env.lc.on_candidate(obs(price=5, breakout=True)) is None
    assert env.lc.on_quote(CA, q(5)) is None
    assert env.poster.types() == ["NEW", "DEAD"]


def test_cooldown_and_urgent_safety(env):
    env.lc.on_candidate(obs(price=1.0))
    env.advance(3)
    assert env.lc.on_candidate(obs(price=1.7)) is None            # material but inside 10-min cooldown
    assert env.lc.on_candidate(obs(price=1.7, safety={"fails": ["LP unlocked"]})) == "UPDATE"   # urgent


def test_no_trades_death(env):
    env.lc.on_candidate(obs(price=1.0))
    env.lc.on_quote(CA, q(0.9))
    env.advance(181)
    assert env.lc.on_quote(CA, q(0.25, traded=False)) == "DEAD"
    assert env.store.get(CA).death_reason == "no_trades"


def test_pool_vanished(env):
    env.lc.on_candidate(obs(price=1.0))
    env.lc.on_quote(CA, q(1.2))
    env.lc.on_quote(CA, None)
    env.advance(11)
    assert env.lc.on_quote(CA, None) == "DEAD"


def test_residual_pool_is_never_priced(env):
    env.lc.on_candidate(obs(price=1.0))
    assert env.lc.on_quote(CA, q(0.5, pool="OTHER")) is None
    assert env.store.get(CA).last_valid_price == 1.0


# ---------------------------------------------------------------- P1 lifecycle parts
def test_13_dev_sell_invalidates_and_stops(env):
    env.lc.on_candidate(obs(price=1.0))
    env.advance(11)
    assert env.lc.invalidate(CA, "dev_sold", "Dev wallet sold 3.1% of supply.") == "INVALIDATED"
    env.advance(5)
    assert env.lc.on_candidate(obs(price=1.2, breakout=True)) is None
    assert env.lc.on_quote(CA, q(2.5)) is None
    assert env.poster.types() == ["NEW", "INVALIDATED"]


def test_14_catalyst_outage_then_fill(env):
    env.lc.on_candidate(obs())
    assert env.store.get(CA).catalyst_tag == "UNCHECKED"
    assert env.lc.on_catalyst(CA, None, None, None, obs()) is None          # outage: stays UNCHECKED
    assert env.lc.on_catalyst(CA, "REAL_CATALYST", "https://news.example/x", "Event coverage", obs()) == "UPDATE"
    assert env.store.get(CA).catalyst_tag == "REAL_CATALYST"
    assert env.lc.on_catalyst(CA, "REAL_CATALYST", None, None, obs()) is None   # never REAL without a URL
    assert env.poster.types().count("NEW") == 1


def test_15_realizable_marks(env):
    env.lc.on_candidate(obs(price=1.0))
    for minute, price in ((15, 0.7), (15, 1.1), (10, 2.4), (25, 1.3)):
        env.advance(minute)
        env.lc.on_quote(CA, q(price))
    rec = env.store.get(CA)
    assert rec.price_at["t+15m"] == 0.7 and rec.price_at["t+30m"] == 1.1
    s = M.tracker_stats(env.store.records.values(), env.clock())
    assert s["hit2"] == 1 and s["t15"] == pytest.approx(0.7) and s["t30"] == pytest.approx(1.1)
    assert rec.max_drawdown_pct == pytest.approx(-30)


# ---------------------------------------------------------------- P2 lifecycle parts
def test_17_early_watch_banner_and_promotion(env):
    assert env.lc.on_candidate(obs(price=1.0, signals=False, early=True)) == "NEW"
    ch, e, content = env.poster.posts[-1]
    assert ch == "early-watch" and content.startswith("NOT A BUY CALL")
    assert M.tracker_stats(env.store.records.values(), env.clock())["alerts"] == 0
    env.advance(20)
    assert env.lc.on_candidate(obs(price=1.5, signals=True, qualifies_promotion=True)) == "UPDATE"
    rec = env.store.get(CA)
    assert rec.official and rec.first_alert_price == 1.5 and rec.early_alert_price == 1.0
    assert env.poster.posts[-1][0] == "signals"
    assert M.tracker_stats(env.store.records.values(), env.clock())["alerts"] == 1


def test_18_why_it_died_groups(env):
    env.lc.cfg.max_new_per_hour = 99
    for i, reason in enumerate(("dev_sold", "dev_sold", "liquidity_pulled")):
        ca = f"X{i:043d}"
        env.lc.on_candidate(obs(ca=ca))
        env.lc.kill(ca, reason)
    d = M.postmortem_embed(env.store.records.values(), env.clock())["description"]
    assert "dev sold (2)" in d and "liquidity pulled (1)" in d and f"X{0:043d}" in d


def test_store_atomic_and_corrupt_fallback(env, tmp_path):
    env.lc.on_candidate(obs())
    assert os.path.exists(str(tmp_path / "bk"))
    with open(env.path, "w") as f:
        f.write("{broken")
    env.boot()
    assert env.store.get(CA) is not None                          # restored from the daily backup
    assert "restored" in env.store.load_error
