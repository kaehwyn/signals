"""Clock-based jobs in America/Los_Angeles (§6.4). Each job remembers the last slot it ran for (in the state
file), so a restart never resets anything and a missed slot is caught up ONCE, labeled late (no flood)."""
from datetime import datetime, timedelta
from typing import Optional

from .config import Config
from .store import PT

LATE_AFTER_S = 600


def _hhmm(s: str) -> tuple:
    h, m = s.split(":")
    return int(h), int(m)


def last_slot(job: str, now: float, cfg: Config) -> Optional[float]:
    """Most recent scheduled time <= now for the job (epoch), or None if the job has no slot today yet."""
    d = datetime.fromtimestamp(now, PT)
    if job == "tracker":
        h = d.hour - d.hour % max(1, cfg.tracker_every_hours)
        return d.replace(hour=h, minute=0, second=0, microsecond=0).timestamp()
    if job == "recap":
        top = d.replace(minute=0, second=0, microsecond=0)
        for back in range(0, 25):
            t = top - timedelta(hours=back)
            if cfg.recap_overnight or cfg.recap_start_hour <= t.hour <= cfg.recap_end_hour:
                return t.timestamp()
        return None
    if job in ("leaderboard", "postmortem"):
        h, m = _hhmm(cfg.leaderboard_time if job == "leaderboard" else cfg.postmortem_time)
        t = d.replace(hour=h, minute=m, second=0, microsecond=0)
        if t > d:
            t -= timedelta(days=1)
        return t.timestamp()
    raise ValueError(job)


def due(job: str, now: float, cfg: Config, jobs_state: dict) -> Optional[dict]:
    """{"slot": t, "late": bool} if the job should run now, else None. Caller stores jobs_state[job] = slot after."""
    slot = last_slot(job, now, cfg)
    if slot is None:
        return None
    prev = jobs_state.get(job)
    if prev is not None and prev >= slot:
        return None
    late = now - slot > LATE_AFTER_S
    if prev is None and late:                      # first ever boot: don't post an old slot, just start the clock
        jobs_state[job] = slot
        return None
    return {"slot": slot, "late": late}
