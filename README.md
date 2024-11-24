BlueHoover
==========

BlueHoover is a tool for ingesting the BlueSky firehose and performing trend analysis. It is implemented using Asyncio Python, FastAPI, Clickhouse, and Chart.js. 

It is still quite janky, and may be broken in strange an unexpected ways.

# Architecture

BlueHoover consumes the Bluesky Jetstream websocket feed, extracting post events, and writing them to a `posts` table in ClickHouse in batches. 

On the ClickHouse side, the `tokenizer` Materialized View is attached to the `posts` table and updates a `tokens_by_interval` table with the tokens found in the posts. `tokens_by_interval` is a `SummingMergeTree`, with an ordering key on `(token, period)`, where `period` is the 10 minute interval in which the post was created. Tokenisation is currently very basic, simply splitting on non-alphanumeric characters with ClickHouse's `splitByNonAlpha` function, and then lowercasing.

Trends are identified by comparing the count of a token in the last hour with the average count of that token in the previous 24 hours. This is performed by the `trends_1hr_mv` Refreshable Materialized View, refreshing hourly, and updating the `trends_1hr` table with the results.

A basic FastAPI webapp is provided for viewing the trends, and visualising token rates. 

# Containers 

The project is split into 4 containers, defined in the `docker-compose.yml`:

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

