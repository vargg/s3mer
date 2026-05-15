# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

# Set working directory
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy project files
COPY pyproject.toml uv.lock ./

# Install dependencies (including dev for testing)
RUN uv sync --frozen --no-install-project

# Copy the rest of the source code
COPY . .

# Install the project
RUN uv sync --frozen

# --- Test stage ---
FROM builder AS test
# This stage can be used to run pytest
CMD ["uv", "run", "pytest"]

# --- Final image ---
FROM python:3.12-slim-bookworm AS final

WORKDIR /app

# Copy the environment from the builder
# We only need the .venv and src
COPY --from=builder /app /app

# Place executable on path
ENV PATH="/app/.venv/bin:$PATH"

# Default port for the proxy
EXPOSE 8000

# The CMD should be overridden in docker-compose for server vs worker
CMD ["uvicorn", "s3m.app:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
