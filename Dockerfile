# No pip install. Runtime dependencies are the Python 3.12 standard library.
# See requirements.lock.
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app/src \
    HOME=/tmp

RUN groupadd --gid 10001 neurio \
    && useradd --uid 10001 --gid 10001 --home-dir /tmp --shell /usr/sbin/nologin --no-create-home neurio \
    && mkdir -p /data \
    && chown 10001:10001 /data

WORKDIR /app
COPY src /app/src

USER 10001:10001
VOLUME ["/data"]
EXPOSE 8095

ENTRYPOINT ["python", "-m", "neurio_collector"]
