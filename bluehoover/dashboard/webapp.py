from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
import clickhouse_connect
import os
from loguru import logger
import re
from typing import List, Dict
from datetime import datetime, timedelta
from pydantic import BaseModel
from enum import Enum

app = FastAPI()

# Get the directory where this script is located
BASE_DIR = Path(__file__).resolve().parent

# Mount static files and templates relative to this script
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv("CLICKHOUSE_HOST", "localhost"),
        port=int(os.getenv("CLICKHOUSE_PORT", "8123")),
        database=os.getenv("CLICKHOUSE_DATABASE", "default"),
        username=os.getenv("CLICKHOUSE_USER", "default"),
        password=os.getenv("CLICKHOUSE_PASSWORD", ""),
    )


def process_text(text: str) -> List[str]:
    # Remove URLs, mentions, and special characters
    text = re.sub(r"http\S+|@\w+|[^\w\s]", " ", text.lower())
    # Split into words and filter out short words
    words = [word for word in text.split() if len(word) > 3]
    return words


@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/posts_timeline")
async def get_posts_timeline():
    client = get_clickhouse_client()

    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)

    query = """
        SELECT 
            toStartOfInterval(created_at, INTERVAL 10 MINUTE) as interval_start,
            count(*) as post_count
        FROM posts
        WHERE created_at >= %(start_time)s 
          AND created_at < %(end_time)s
        GROUP BY interval_start
        ORDER BY interval_start
    """

    result = client.query(
        query, parameters={"start_time": start_time, "end_time": end_time}
    )

    return {
        "labels": [row[0].strftime("%H:%M") for row in result.result_rows],
        "data": [row[1] for row in result.result_rows],
    }


class WordList(BaseModel):
    words: List[str]


@app.post("/api/custom_word_timeline")
async def get_custom_word_timeline(word_list: WordList):
    # Sanitize input words
    sanitized_words = [
        word.lower().strip()[:100] for word in word_list.words if len(word.strip()) > 0
    ][:32]

    logger.info(
        f"Processing request for {len(sanitized_words)} words: {sanitized_words}"
    )

    if not sanitized_words:
        return {"error": "No valid words provided"}

    client = get_clickhouse_client()

    # This query returns token and then an array of counts for each 10 minute period in the last 24 hours
    query = """
    SELECT
        min(period) as start_period,
        token,
        groupArray(count) as token_counts,
        groupArray(total_posts) as total_posts
    FROM
    (
        SELECT
            periods.period AS period,
            wanted_tokens.token AS token,
            COALESCE(fast_results.count, 0) AS count,
            COALESCE(period_totals.total_posts, 0) as total_posts
        FROM
        (
            SELECT toStartOfInterval(now() - toIntervalMinute(number * 10), toIntervalMinute(10)) AS period
            FROM numbers(6 * 24)
        ) AS periods
        CROSS JOIN
        (
            SELECT arrayJoin(%(tokens)s) AS token
        ) AS wanted_tokens
        LEFT JOIN
        (
            SELECT
                period,
                token,
                sum(count) AS count
            FROM tokens_by_interval
            WHERE (token IN %(tokens)s) AND (period >= toStartOfInterval(now() - toIntervalHour(24), toIntervalMinute(10)))
            GROUP BY
                period,
                token
        ) AS fast_results ON (periods.period = fast_results.period) AND (wanted_tokens.token = fast_results.token)
        LEFT JOIN
        (
            SELECT 
                period,
                sum(count) as total_posts
            FROM tokens_by_interval
            WHERE period >= toStartOfInterval(now() - toIntervalHour(24), toIntervalMinute(10))
            GROUP BY period
        ) AS period_totals ON periods.period = period_totals.period
        ORDER BY
            periods.period ASC,
            wanted_tokens.token ASC
    )
    GROUP BY token
    """

    result = client.query(query, parameters={"tokens": sanitized_words})

    if len(result.result_rows) == 0:
        return {"error": "No results found"}

    # row[0] is the start period, so use that instead of datetime.now().
    start_period = result.result_rows[0][0]

    time_labels = [
        (start_period + timedelta(minutes=i * 10)).strftime("%H:%M")
        for i in range(6 * 24)  # 6 * 24 = 1440 minutes in 24 hours
    ]

    datasets = []
    for word in sanitized_words:
        row = next((r for r in result.result_rows if r[1] == word), None)
        if row:
            token_counts = row[2]
            total_posts = row[3]
            relative_data = [
                (count / total) * 100 if total > 0 else 0
                for count, total in zip(token_counts, total_posts)
            ]
            datasets.append(
                {"label": word, "absolute": token_counts, "relative": relative_data}
            )
        else:
            datasets.append(
                {"label": word, "absolute": [0] * (6 * 24), "relative": [0] * (6 * 24)}
            )

    return {"labels": time_labels, "datasets": datasets}


@app.get("/api/trending")
async def get_trending():
    client = get_clickhouse_client()

    query = """
    SELECT 
        token, 
        1hr_count/24hr_avg as uplift,
        refresh_time
    FROM trends_1hr 
    WHERE refresh_time = (
        SELECT max(refresh_time) 
        FROM trends_1hr
    ) AND uplift > 2
    ORDER BY uplift DESC 
    LIMIT 30
    """

    result = client.query(query)

    return {
        "trends": [
            {
                "token": row[0],
                "uplift": round(
                    float(row[1]), 2
                ),  # Convert to float and round for display
            }
            for row in result.result_rows
        ],
        "refresh_time": result.result_rows[0][2] if result.result_rows else None,
    }
