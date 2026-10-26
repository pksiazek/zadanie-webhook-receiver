# syntax=docker/dockerfile:1
# One image, two commands: `api` (default) and `worker`.
FROM python:3.14-slim-trixie

WORKDIR /app
# Use the interpreter from the base image; never download one.
ENV UV_LINK_MODE=copy \
    UV_COMPILE_BYTECODE=1 \
    UV_PYTHON_DOWNLOADS=never

# Dependencies in their own layer: rebuilt only when the lock changes.
# Only the `loadtest` extra (httpx, for the generator run by the
# `loadtest` compose profile); never `dev` (no pytest/ruff).
COPY pyproject.toml uv.lock ./
# uv (same version as the devcontainer, post-create.sh) is bind-mounted
# only for the sync steps, so it is not part of the final image.
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.5,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra loadtest --no-install-project

COPY src/ src/
RUN --mount=from=ghcr.io/astral-sh/uv:0.13.5,source=/uv,target=/bin/uv \
    --mount=type=cache,target=/root/.cache/uv \
    uv sync --locked --no-dev --extra loadtest --no-editable
# Load generator (not part of the installed package): run from WORKDIR
# as `python -m loadtest.run`.
COPY loadtest/ loadtest/

# Non-root user; /data is owned by it so an empty named volume mounted
# there inherits that owner on first use.
RUN groupadd --system --gid 10001 app \
    && useradd --uid 10001 --gid app --no-create-home app \
    && mkdir -p /data \
    && chown app:app /data

ENV DB_PATH=/data/webhooks.db \
    PYTHONUNBUFFERED=1 \
    PATH="/app/.venv/bin:$PATH"

USER app
EXPOSE 8000 9100

# Exec form: python gets SIGTERM directly (as PID 1, or as the child of
# the init process that compose adds with `init: true`).
ENTRYPOINT ["python", "-m", "webhook_receiver"]
CMD ["api"]
