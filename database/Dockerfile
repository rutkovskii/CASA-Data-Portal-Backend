# Use a Python image with uv pre-installed
FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim

# Install the project into `/app`
WORKDIR /app

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1

# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

# Copy the dependencies files into the container
COPY uv.lock pyproject.toml /app/
RUN --mount=type=cache,target=/root/.cache/uv \
  uv sync --frozen --no-install-project --no-dev

# Copy the project files into the container
COPY . /app
RUN --mount=type=cache,target=/root/.cache/uv \
uv sync --frozen --no-dev

# Ensure the virtual environment is used
ENV PATH="/app/.venv/bin:$PATH"


# Set the default entry point
ENTRYPOINT []

# Copy only the necessary files
# COPY database.py .
# COPY models.py .
# COPY create_tables.py .

CMD ["python", "init_db.py"]
