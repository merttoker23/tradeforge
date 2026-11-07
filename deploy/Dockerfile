FROM ghcr.io/astral-sh/uv:0.10.9 AS uv
FROM python:3.12-slim-bookworm AS build
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy
COPY pyproject.toml uv.lock README.md ./
COPY src/ ./src/
COPY apps/ ./apps/
COPY migrations/ ./migrations/
RUN uv sync --locked --no-dev --no-editable --python /usr/local/bin/python

FROM python:3.12-slim-bookworm
RUN groupadd --gid 10001 tradeforge && useradd --uid 10001 --gid 10001 --no-create-home tradeforge
WORKDIR /app
COPY --from=build /app/.venv /app/.venv
ENV PATH="/app/.venv/bin:$PATH" PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
USER 10001:10001
ENTRYPOINT ["python", "-m", "tradeforge.runtime.container"]
