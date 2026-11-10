# Running Spidey Bot

## Option A — GitHub Actions (free, what runs today)

Works with no server. Each run lasts ~5h40m, saves, and starts the next run itself, so there's only ~1 min of
downtime between runs (cron is just a backup). State is pushed to the repo every 30 min and at the end of each run,
so a restart never resets the tracker. Limits: GitHub can occasionally delay or drop a run, and there's no real
process manager — the `#bot` heartbeat will show any gap.

Secrets (repo → Settings → Secrets and variables → Actions):

| Secret | Channel | Required |
|---|---|---|
| `DISCORD_WEBHOOK_URL` | #signals | yes |
| `DISCORD_WINS_WEBHOOK_URL` | #wins | no (falls back to the tracker webhook, then #signals) |
| `DISCORD_EARLY_WEBHOOK_URL` | #early-watch | no (falls back to #signals, still with the NOT A BUY CALL banner) |
| `DISCORD_RECAP_WEBHOOK_URL` | #recap | no (falls back to #signals) |
| `DISCORD_STATUS_WEBHOOK_URL` | #spidey-status | no (hourly heartbeat, restart, feed-down and health warnings only go to the log without it) |
| `SOLANA_RPC_URL` | — | no, but a free Helius key makes holder/dev checks much more reliable |

Make a webhook: channel → Edit Channel → Integrations → Webhooks → New Webhook → Copy URL.

## Option B — always-on VPS (recommended for 24/7, §6.1)

Any small Linux VPS works (Oracle Cloud "Always Free" ARM VM costs nothing; Hetzner/DigitalOcean ~$4–6/mo).

```bash
sudo useradd -m spidey && sudo mkdir -p /opt/spidey && sudo chown spidey /opt/spidey
sudo -u spidey git clone https://github.com/kaehwyn/signals /opt/spidey/signals
sudo apt install -y python3-requests python3-websocket
# put the same webhook/RPC values in /opt/spidey/spidey.env, one per line:  DISCORD_WEBHOOK_URL=https://...
sudo cp /opt/spidey/signals/deploy/spidey-bot.service /etc/systemd/system/
sudo systemctl daemon-reload && sudo systemctl enable --now spidey-bot
curl -s localhost:8080/      # health: 200 only if the loop is alive, state is readable, last poll <= 120s ago
```

Or with Docker: `docker compose -f deploy/docker-compose.yml up -d --build` (restart: unless-stopped, log rotation,
healthcheck built in; state lives in `./data`). Copy `state.json` and `backups/` from the repo into the state
folder first so history carries over. Then disable the GitHub workflow so two copies don't post.

## What changed in v8

- A coin is marked dead when price is −90% from the alert or its peak, or liquidity is −80% from its alert/peak level
  (any time). Rugs get one DEAD/INVALIDATED post in the channel of the original alert with a link back to it
  (webhooks cannot make a true Discord "reply"). Quiet fades are tracked silently (`post_fade_endings`).
- LP lock is RugCheck's real number. A `...pump` mint on PumpSwap is no longer assumed burned; unknown = not official.
- Every record stores its stage (New Pairs / Final Stretch / Migrated) and bonding % at alert time, and each stage
  has its own filters (see the "stage filters" block in `spidey/config.py`).
- Heartbeat is hourly and shows feed status, launches seen, watching counts and posts by stage. A dropped pump.fun
  feed posts a warning (and a recovery note) to #spidey-status.
- Tracker and leaderboard report hit rates by stage and count rugs separately from quiet fades.

GitHub Actions still has ~1 min between runs and can be delayed by GitHub; Option B below is the real 24/7 fix.

## Changing settings

Every threshold is in `spidey/config.py`. Override without code changes via `config.json` in the working folder
(`{"min_liquidity_usd": 10000}`) or env vars (`SPIDEY_MIN_LIQUIDITY_USD=10000`; on Actions add them under `env:`
in `.github/workflows/signals.yml`).

## Tests

`pip install pytest && python -m pytest -q tests` — covers the spec §11 acceptance tests (dedupe across restarts,
DAKITTY/MUBBO/GPU/KAWAENE, tracker persistence, multiplier-once, dev-sell invalidation, gates, catalyst, schedules).
