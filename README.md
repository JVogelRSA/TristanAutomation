# Tristan Automation (weekly-automation)

Automated business reporting for Daylight Computer. Three weekly bots gather data
from Brex/Mercury/Rippling, Snowflake (Shopify), and DCL inventory emails, run an
LLM analysis pass, and ship one unified Gmail-optimized HTML email every Monday.
A separate monthly job reports month-end finished-unit counts to Zeni (accounting).

## The reports

| Script | What it does | Data sources | LLM |
| --- | --- | --- | --- |
| `weekly_report.py` | **The main entry point.** Runs all three sections below and sends ONE unified email with KPI cards + DOCX/CSV attachments | everything below | — |
| `sales_bot.py` | Weekly sales vs last week, kids deep-dive, daily breakdown | Snowflake (`DAYLIGHT_SALES.CONNECTORS.SHOPIFY`) | claude-opus-4-8 |
| `spend_bot.py` | CFO-style spend analysis, recurring-subscription detection, cash runway | Brex + Mercury (+ Rippling stub) | claude-opus-4-8 |
| `inventory_bot.py` | Burn rates, stockout ETAs, reorder flags from DCL CSV snapshots | IMAP (DCL emails) | gpt-4o |
| `monthly_zeni_report.py` | Month-end fully-assembled unit count for the accountants | IMAP (DCL emails) | — |

Each bot also runs standalone (sends its own email). The unified report reuses
their data/report functions without their solo send paths.

## Running

```bash
source venv/bin/activate

# Build the full weekly report but write it to ./out/ instead of emailing.
# Saves no history snapshots — safe to run any time.
python weekly_report.py --dry-run

# Real send (what cron runs every Monday)
python weekly_report.py

# Real send with [TEST] subject prefix
python weekly_report.py --test

# Monthly Zeni report (cron runs daily on the 1st–7th; it no-ops except the
# day the first new-month DCL snapshot lands)
python monthly_zeni_report.py --dry-run
python monthly_zeni_report.py --force   # manual/backfill send
```

Exit codes matter: every entry point exits non-zero when the email could not be
sent, so cron/CI failures are visible. Weekly history snapshots (`data/<bot>/week_*.json`)
are written only **after** a successful send, and a re-run within the same week
never compares the week against its own earlier snapshot.

## Configuration (.env)

| Variable | Purpose |
| --- | --- |
| `REPORT_RECIPIENT` | Where the weekly report goes |
| `SMTP_USERNAME` / `SMTP_PASSWORD` / `SMTP_SERVER` / `SMTP_PORT` | Sending (Gmail app password) |
| `IMAP_USERNAME` / `IMAP_PASSWORD` / `IMAP_SERVER` | Reading DCL inventory emails |
| `EMAIL_SUBJECT_KEYWORD` / `EMAIL_SENDER` | DCL email filters |
| `ANTHROPIC_API_KEY` | Sales + spend analysis (claude-opus-4-8) |
| `OPENAI_API_KEY` | Inventory analysis (gpt-4o) |
| `BREX_API_KEY` / `MERCURY_API_KEY` / `RIPPLING_API_KEY` | Spend sources |
| `SNOWFLAKE_USER` / `ACCOUNT` / `WAREHOUSE` / `DATABASE` / `SCHEMA` + `SNOWFLAKE_PRIVATE_KEY_PATH` or `SNOWFLAKE_PASSWORD` | Sales source (key-pair auth preferred) |
| `CASH_BALANCE_USD` | Enables the cash-runway calculation |
| `ZENI_RECIPIENTS` / `ZENI_CC` | Zeni report routing (empty = preview to `REPORT_RECIPIENT`) |
| `DC1_VALUE_USD` / `KIDS_VALUE_USD` | Zeni valuation (default $729 / $799 retail) |

## Layout

```
weekly_report.py        orchestrator (one unified email)
sales_bot.py            Snowflake queries + sales report
spend_bot.py            spend analysis, subscriptions, runway
inventory_bot.py        DCL CSV parsing, burn rates, reorder flags
monthly_zeni_report.py  month-end unit count for Zeni
adapters/               brex.py · mercury.py · rippling.py (normalize → Date/Description/Amount/Category/Source)
utils/                  email_sender.py · unified_email.py (Gmail HTML) · history.py (weekly JSON snapshots) · docx_generator.py
queries/                reference SQL
data/                   weekly snapshot JSONs per bot (gitignored)
tests/                  unit tests (no network, stdlib unittest)
```

## Tests

```bash
venv/bin/python -m unittest discover tests -v
```

Covers adapter normalization (mocked HTTP), recurring-subscription detection,
runway math, reorder flags, history round-trips with same-week exclusion, and
metric parsing. No test touches the network or sends email.

## Deployment

Runs on a DigitalOcean droplet (`tristan-automation`) via cron, Mondays ~10:00 UTC.
See `DO_DEPLOYMENT.md` and `setup_server.sh`. GitHub repo: `JVogelRSA/TristanAutomation`.
