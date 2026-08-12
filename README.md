# Outbound Followup Automation

Keeps `Interest Status` on Attio's `outbound_people` list (Outbound - Instantly)
in sync with what's actually happened on each lead: Elliot's outbound emails,
prospect replies, and connected Aircall calls (> 10s).

Runs every 30 minutes via GitHub Actions (`.github/workflows/outbound-followup.yml`)
instead of a local Task Scheduler job, so it doesn't depend on any one computer
being on.

## Local run

```
pip install -r requirements.txt
ATTIO_API_KEY=xxx python outbound_followup_automation.py         # dry-run, writes out/outbound_followup_preview.csv
ATTIO_API_KEY=xxx python outbound_followup_automation.py --apply # writes to Attio
```

## Setup

Requires a repo secret `ATTIO_API_KEY` (Settings → Secrets and variables → Actions).
