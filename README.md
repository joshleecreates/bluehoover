BlueHoover
==========

Here be dragons! This is a janky tool for ingesting the BlueSky firehose and displaying some basic analytics.

It may be broken in strange an unexpected ways.

Stack is Asyncio + FastAPI + Clickhouse + Chart.js. It's split into 4 containers:

- `firehose` - ingests the firehose and writes to Clickhouse in batches
- `webapp` - a simple webapp for viewing the dashboard, runs under uvicorn
- `nginx` - reverse proxy; perhaps not strictlyneeded
- `clickhouse` - the Clickhouse server, the powerhouse of the stack

# Running

Easiest is:
```
docker compose up -d
```

# Notes

Sequence number is written to `seq.txt` every 1000 events.

Use:
```sql
OPTIMIZE TABLE posts FINAL DEDUPLICATE BY cid,created_at
```

to remove duplicates following dodgy replay...

