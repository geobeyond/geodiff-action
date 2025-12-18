FROM ghcr.io/astral-sh/uv:python3.13-alpine

LABEL org.opencontainers.image.source="https://github.com/francbartoli/geodiff-action"
LABEL org.opencontainers.image.description="GeoDiff Action - GitHub Action for geospatial file diff operations"
LABEL org.opencontainers.image.authors="francbartoli"

# Install git for git history mode
RUN apk add --no-cache git

# Enable bytecode compilation
ENV UV_COMPILE_BYTECODE=1
# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy
# Ensure installed tools can be executed out of the box
ENV UV_TOOL_BIN_DIR=/usr/local/bin

COPY pyproject.toml uv.lock /
RUN uv sync --locked --no-dev

COPY src /src
ENV PATH="/.venv/bin:$PATH"
ENTRYPOINT ["python", "/src/main.py"]
