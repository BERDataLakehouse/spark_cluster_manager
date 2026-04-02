FROM python:3.13-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Install uv
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

# Install Python dependencies
COPY pyproject.toml uv.lock .python-version ./
RUN uv sync --locked --inexact --no-dev

COPY src/ src/

EXPOSE 8000

CMD ["uv", "run", "uvicorn", "--host", "0.0.0.0", "--port", "8000", "--factory", "src.main:create_application"]