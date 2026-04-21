FROM ghcr.io/astral-sh/uv:0.11.7 AS uv

FROM python:3.12-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv (pinned version for reproducible builds)
COPY --from=uv /uv /uvx /bin/

# Install Python dependencies
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --inexact --no-dev

COPY src/ src/

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "--host", "0.0.0.0", "--port", "8000", "--factory", "src.main:create_application"]
