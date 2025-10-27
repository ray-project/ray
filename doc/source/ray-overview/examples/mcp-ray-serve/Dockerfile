FROM anyscale/ray:2.47.1-py312

RUN mkdir -p /home/ray/default

RUN sudo apt-get update && \
    sudo apt-get install -y podman && \
    sudo rm -rf /var/lib/apt/lists/*

RUN python3 -m pip install --no-cache-dir "mcp==1.11.0" "asyncio==3.4.3" "pydantic==2.9.2"
