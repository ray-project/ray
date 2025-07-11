FROM anyscale/ray:2.47.1-py312

RUN mkdir -p /home/ray/default

# Combine update + install + cleanup in one RUN (and no sudo needed).
RUN apt-get update && \
    apt-get install -y podman && \
    rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir "mcp==1.8.0" "asyncio==3.4.3"
