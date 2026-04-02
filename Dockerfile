FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv and Python dependencies
RUN pip3 install --upgrade pip && \
    pip3 install uv
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --inexact --no-dev

COPY src/ src/

EXPOSE 8000

ENV UV_PROJECT_ENVIRONMENT=/usr/local
CMD ["uv", "run", "uvicorn", "--host", "0.0.0.0", "--port", "8000", "--factory", "src.main:create_application"]