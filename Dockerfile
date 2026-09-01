ARG PYTHON_IMAGE=python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7

FROM ${PYTHON_IMAGE} AS builder

ARG UV_VERSION=0.12.5
ENV UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy

WORKDIR /build
RUN python -m pip install --no-cache-dir "uv==${UV_VERSION}"
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv sync --frozen --no-dev --no-editable

FROM ${PYTHON_IMAGE} AS runtime

ARG BUILD_VERSION=0.1.0
LABEL org.opencontainers.image.title="distributed-sql" \
    org.opencontainers.image.version="${BUILD_VERSION}"
ENV PATH=/opt/venv/bin:$PATH \
    DISTRIBUTED_SQL_BUILD_VERSION=${BUILD_VERSION} \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN groupadd --gid 10001 distributed-sql \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin distributed-sql \
    && mkdir -p /var/lib/distributed-sql/catalog /var/lib/distributed-sql/runtime /var/lib/distributed-sql/tmp \
    && chown -R 10001:10001 /var/lib/distributed-sql

COPY --from=builder /opt/venv /opt/venv
COPY --chown=10001:10001 deploy/smoke-data.csv /opt/distributed-sql/examples/smoke-data.csv
WORKDIR /var/lib/distributed-sql
USER 10001:10001

FROM runtime AS coordinator
EXPOSE 8080
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=6 \
    CMD ["python", "-c", "import json,urllib.request; data=json.load(urllib.request.urlopen('http://127.0.0.1:8080/health', timeout=2)); assert data['status']=='healthy'"]
ENTRYPOINT ["distributed-sql-coordinator"]

FROM runtime AS worker
EXPOSE 8091
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=6 \
    CMD ["python", "-c", "import json,urllib.request; data=json.load(urllib.request.urlopen('http://127.0.0.1:8091/health', timeout=2)); assert data['status']=='healthy' and data['dependencies']['coordinator']=='registered'"]
ENTRYPOINT ["distributed-sql-worker"]
