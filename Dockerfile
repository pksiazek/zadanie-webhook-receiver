# syntax=docker/dockerfile:1
# One image, two commands: `api` (default) and `worker`.
FROM python:3.14-slim-trixie

# uv pinned to the same version as the devcontainer (post-create.sh).
COPY --from=ghcr.io/astral-sh/uv:0.13.5 /uv /uvx /bin/

WORKDIR /app
# Use the interpreter from the base image; never download one.
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Dependencies in their own layer: rebuilt only when the lock changes.
# No extras -> no dev/loadtest packages in the runtime image.
COPY pyproject.toml uv.lock ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-install-project

COPY src/ src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --no-editable

# Non-root user; /data is owned by it so an empty named volume mounted
# there inherits that owner on first use.
RUN useradd --system --uid 10001 --user-group --no-create-home app \
    && mkdir -p /data \
    && chown app:app /data

ENV DB_PATH=/data/webhooks.db \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

USER app
EXPOSE 8000 9100

# Exec form: python is PID 1 and receives SIGTERM directly.
ENTRYPOINT ["python", "-m", "webhook_receiver"]
CMD ["api"]
