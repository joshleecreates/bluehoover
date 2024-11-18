FROM python:3.11-slim

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Copy poetry files
COPY pyproject.toml poetry.lock ./

# Install dependencies
RUN pip install poetry
RUN poetry config virtualenvs.create false \
    && poetry install --no-dev --no-interaction --no-ansi

# Copy the module
COPY bluehoover bluehoover/

# Set Python path to recognize the module
ENV PYTHONPATH=/app

# Run the application
CMD ["poetry", "run", "uvicorn", "bluehoover.dashboard.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "4"] 