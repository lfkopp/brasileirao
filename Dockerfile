FROM prefecthq/prefect:3-latest AS builder

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    git \
    && rm -rf /var/lib/apt/lists/*
COPY requirements.txt /tmp/
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && pip install --no-cache-dir \
    asyncpg \
    pandas \
    matplotlib \
    scipy \
    requests \
    "prefect-docker" \
    "docker"

FROM prefecthq/prefect:3-latest

COPY --from=builder /usr/local/lib/python3.12/site-packages /usr/local/lib/python3.12/site-packages

COPY ./flows /app/flows
COPY ./tasks /app/tasks
COPY ./*.txt /app/
COPY ./assets/figs /app/assets/figs

WORKDIR /app
