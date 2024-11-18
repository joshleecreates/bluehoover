from fastapi import FastAPI, Request
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

app = FastAPI()

# Get the directory where this script is located
BASE_DIR = Path(__file__).resolve().parent

# Mount static files and templates relative to this script
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")
templates = Jinja2Templates(directory=BASE_DIR / "templates")

def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=os.getenv('CLICKHOUSE_HOST', 'localhost'),
        port=int(os.getenv('CLICKHOUSE_PORT', '8123')),
        database=os.getenv('CLICKHOUSE_DATABASE', 'default'),
        username=os.getenv('CLICKHOUSE_USER', 'default'),
        password=os.getenv('CLICKHOUSE_PASSWORD', '')
    )

def process_text(text: str) -> List[str]:
    # Remove URLs, mentions, and special characters
    text = re.sub(r'http\S+|@\w+|[^\w\s]', ' ', text.lower())
    # Split into words and filter out short words
    words = [word for word in text.split() if len(word) > 3]
    return words

@app.get("/", response_class=HTMLResponse)
async def root(request: Request):
    return templates.TemplateResponse("index.html", {"request": request})


@app.get("/api/posts_timeline")
async def get_posts_timeline():
    client = get_clickhouse_client()
    
    query = """
    SELECT
        toStartOfHour(created_at) as hour,
        count() as post_count
    FROM posts
    WHERE created_at >= now() - INTERVAL 24 HOUR
    GROUP BY hour
    ORDER BY hour
    """
    
    result = client.query(query)
    
    return {
        "labels": [row[0].strftime("%Y-%m-%d %H:%M") for row in result.result_rows],
        "data": [row[1] for row in result.result_rows]
    }

class WordList(BaseModel):
    words: List[str]

@app.post("/api/custom_word_timeline")
async def get_custom_word_timeline(word_list: WordList):
    # Sanitize input words
    sanitized_words = [
        re.sub(r'[^a-zA-Z0-9\s]', '', word.lower())[:50]  # Remove special chars and limit length
        for word in word_list.words
        if len(word.strip()) > 0
    ][:10]  # Limit to 10 words maximum
    
    if not sanitized_words:
        return {"error": "No valid words provided"}
    
    client = get_clickhouse_client()
    
    # Define time ranges for the last 24 hours
    end_time = datetime.now()
    start_time = end_time - timedelta(days=1)
    
    # Generate SQL count statements for each word
    word_counts = [
        f"sum(if(positionCaseInsensitive(text, %(word_{i})s) > 0, 1, 0)) as word_{i}"
        for i in range(len(sanitized_words))
    ]
    
    query = f"""
        SELECT 
            toStartOfInterval(created_at, INTERVAL 10 MINUTE) as interval_start,
            {', '.join(word_counts)}
        FROM posts
        WHERE created_at BETWEEN %(start_time)s AND %(end_time)s
        GROUP BY interval_start
        ORDER BY interval_start
    """
    
    # Prepare parameters
    parameters = {
        'start_time': start_time,
        'end_time': end_time,
        **{f'word_{i}': word for i, word in enumerate(sanitized_words)}
    }
    
    result = client.query(query, parameters=parameters)
    
    # Generate colors for each word
    colors = [
        f'rgba({(hash(word) % 128 + 128)}, {(hash(word + "1") % 128 + 64)}, {(hash(word + "2") % 128 + 64)}, 0.5)'
        for word in sanitized_words
    ]
    
    # Process the results
    hours = [row[0].strftime('%H:%M') for row in result.result_rows]
    datasets = [
        {
            'label': word,
            'data': [row[i+1] for row in result.result_rows],
            'color': color
        }
        for i, (word, color) in enumerate(zip(sanitized_words, colors))
    ]
    
    return {
        "labels": hours,
        "datasets": datasets
    } 