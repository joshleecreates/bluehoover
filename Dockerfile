FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install poetry
RUN pip install poetry

# Copy poetry files
COPY pyproject.toml poetry.lock ./

# Copy source code
COPY bluehoover ./bluehoover

# Install dependencies
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

# # Update the clickhouse.py file to use environment variables
# RUN sed -i 's/CLICKHOUSE_HOST = '\''localhost'\''/CLICKHOUSE_HOST = os.getenv('\''CLICKHOUSE_HOST'\'', '\''localhost'\'')/g' bluehoover/clickhouse.py \
#     && sed -i '1s/^/import os\n/' bluehoover/clickhouse.py

CMD ["python", "-m", "bluehoover.clickhouse"] 