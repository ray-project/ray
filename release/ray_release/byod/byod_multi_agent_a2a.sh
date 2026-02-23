#!/usr/bin/env bash
set -euxo pipefail

pip install --no-cache-dir \
    "fastapi==0.115.12" \
    "langchain==1.0.5" \
    "langchain-mcp-adapters==0.1.12" \
    "langchain-openai==1.0.2" \
    "langgraph==1.0.3" \
    "openai==2.7.2" \
    "uvicorn==0.38.0" \
    "a2a-sdk[http-server]==0.3.22" \
    "httpx==0.28.1" \
    "mcp==1.22.0" \
    "protego==0.5.0" \
    "readabilipy==0.3.0" \
    "markdownify==0.14.1" \
    "pytest==9.0.2"
