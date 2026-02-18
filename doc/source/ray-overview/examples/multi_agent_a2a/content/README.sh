#!/usr/bin/env bash
# Auto-generated from README.ipynb — do not edit manually.
set -euo pipefail

pip install -r requirements.txt

# Start Serve (returns immediately with --non-blocking)
serve run serve_multi_config.yaml --non-blocking

# Wait for all applications to be RUNNING
echo "Waiting for all Serve applications to be ready..."
for i in $(seq 1 600); do
    STATUS=$(serve status 2>/dev/null || true)
    if echo "$STATUS" | grep -q 'status: RUNNING' && \
       ! echo "$STATUS" | grep -qE 'status: (DEPLOYING|NOT_STARTED)'; then
        echo "All applications RUNNING after ${i}s"
        break
    fi
    if echo "$STATUS" | grep -qE 'status: DEPLOY_FAILED'; then
        echo "ERROR: an application failed to deploy"
        serve status
        exit 1
    fi
    sleep 1
done

# Cleanup on exit
trap 'serve shutdown -y 2>/dev/null || true' EXIT

curl -X POST http://127.0.0.1:8000/llm/v1/chat/completions -H "Content-Type: application/json" -d '{"model": "Qwen/Qwen3-4B-Instruct-2507-FP8", "messages": [{"role": "user", "content": "Hello!"}]}'

curl -X POST http://127.0.0.1:8000/weather-agent/chat -H "Content-Type: application/json" -d '{"user_request": "What is the weather in San Francisco?"}'

curl -X POST http://127.0.0.1:8000/research-agent/chat -H "Content-Type: application/json" -d '{"user_request": "What are the top attractions in Seattle? Reply with sources."}'

curl -X POST http://127.0.0.1:8000/travel-agent/chat -H "Content-Type: application/json" -d '{"user_request": "Plan a 2-day trip to Seattle next week. Include weather details and considerations."}'

curl http://127.0.0.1:8000/a2a-weather/.well-known/agent-card.json
curl http://127.0.0.1:8000/a2a-research/.well-known/agent-card.json
curl http://127.0.0.1:8000/a2a-travel/.well-known/agent-card.json

# Run all tests
python tests/run_all.py

anyscale service deploy -f anyscale_service_multi_config.yaml
anyscale service wait -f anyscale_service_multi_config.yaml --timeout-s 1200

# Extract BASE_URL and ANYSCALE_API_TOKEN from the deployed service
SERVICE_STATUS=$(anyscale service status -f anyscale_service_multi_config.yaml --json)
export BASE_URL=$(echo "$SERVICE_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['query_url'])")
export ANYSCALE_API_TOKEN=$(echo "$SERVICE_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['query_auth_token'])")

curl -X POST "${BASE_URL}/llm/v1/chat/completions" -H "Content-Type: application/json" -H "Authorization: Bearer ${ANYSCALE_API_TOKEN}" -d '{"model": "Qwen/Qwen3-4B-Instruct-2507-FP8", "messages": [{"role": "user", "content": "Hello!"}]}'

curl -X POST "${BASE_URL}/weather-agent/chat" -H "Content-Type: application/json" -H "Authorization: Bearer ${ANYSCALE_API_TOKEN}" -d '{"user_request": "What is the weather in San Francisco?"}'

curl -X POST "${BASE_URL}/research-agent/chat" -H "Content-Type: application/json" -H "Authorization: Bearer ${ANYSCALE_API_TOKEN}" -d '{"user_request": "What are the top attractions in Seattle? Reply with sources."}'

curl -X POST "${BASE_URL}/travel-agent/chat" -H "Content-Type: application/json" -H "Authorization: Bearer ${ANYSCALE_API_TOKEN}" -d '{"user_request": "Plan a 2-day trip to Seattle next week. Include weather details and considerations."}'

curl "${BASE_URL}/a2a-weather/.well-known/agent-card.json" -H "Authorization: Bearer ${ANYSCALE_API_TOKEN}"
curl "${BASE_URL}/a2a-research/.well-known/agent-card.json" -H "Authorization: Bearer ${ANYSCALE_API_TOKEN}"
curl "${BASE_URL}/a2a-travel/.well-known/agent-card.json" -H "Authorization: Bearer ${ANYSCALE_API_TOKEN}"

export TEST_TIMEOUT_SECONDS=2000

python tests/run_all.py

# Test A2A discovery
curl http://127.0.0.1:8000/a2a-weather/.well-known/agent-card.json

# For execution, prefer the Python helper:
# python -c 'import asyncio; from protocols.a2a_client import a2a_execute_text; print(asyncio.run(a2a_execute_text("http://127.0.0.1:8000/a2a-weather","Weather in NYC?")))'

curl http://127.0.0.1:8000/a2a-weather/.well-known/agent-card.json


# Tear down the Anyscale service
anyscale service terminate -f anyscale_service_multi_config.yaml