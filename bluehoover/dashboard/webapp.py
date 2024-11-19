from fastapi import FastAPI, Request, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from pathlib import Path
import clickhouse_connect
from collections import Counter
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


class TimeRange(str, Enum):
    HOUR_1 = "1h"
    HOURS_6 = "6h"
    HOURS_24 = "24h"
    DAYS_7 = "7d"


def get_time_range(range_str: TimeRange) -> tuple[datetime, datetime, str]:
    end_time = datetime.now().replace(
        second=0, microsecond=0
    )  # Round to nearest minute

    if range_str == TimeRange.HOUR_1:
        start_time = end_time - timedelta(hours=1)
        interval = "1 MINUTE"
    elif range_str == TimeRange.HOURS_6:
        start_time = end_time - timedelta(hours=6)
        interval = "5 MINUTE"
    elif range_str == TimeRange.HOURS_24:
        start_time = end_time - timedelta(days=1)
        interval = "10 MINUTE"
    else:  # 7d
        start_time = end_time - timedelta(days=7)
        interval = "1 HOUR"

    return start_time, end_time, interval


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
    timeRange: TimeRange = TimeRange.HOURS_24


@app.post("/api/custom_word_timeline")
async def get_custom_word_timeline(word_list: WordList):
    # Sanitize input words
    sanitized_words = [
        re.sub(r"[^a-zA-Z0-9\s]", "", word.lower())[:50]
        for word in word_list.words
        if len(word.strip()) > 0
    ][:10]

    if not sanitized_words:
        return {"error": "No valid words provided"}

    client = get_clickhouse_client()
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)

    # Use match() to ensure we're matching whole words
    query = f"""
        SELECT 
            toStartOfInterval(created_at, INTERVAL 10 MINUTE) as interval_start,
            {', '.join([f'countIf(match(lower(text), %(pattern_{i})s))' for i in range(len(sanitized_words))])}
        FROM posts
        WHERE created_at >= %(start_time)s 
          AND created_at < %(end_time)s
        GROUP BY interval_start
        ORDER BY interval_start
    """

    # Create regex patterns with word boundaries
    parameters = {
        "start_time": start_time,
        "end_time": end_time,
        **{f"pattern_{i}": f"\\b{word}\\b" for i, word in enumerate(sanitized_words)},
    }

    result = client.query(query, parameters=parameters)

    colors = [
        f'rgba({(hash(word) % 128 + 128)}, {(hash(word + "1") % 128 + 64)}, {(hash(word + "2") % 128 + 64)}, 0.8)'
        for word in sanitized_words
    ]

    times = [row[0].strftime("%H:%M") for row in result.result_rows]
    datasets = [
        {
            "label": word,
            "data": [row[i + 1] for row in result.result_rows],
            "color": color,
        }
        for i, (word, color) in enumerate(zip(sanitized_words, colors))
    ]

    return {"labels": times, "datasets": datasets}
