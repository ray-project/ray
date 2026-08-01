#!/usr/bin/env bash
# Launch native SGLang PD disaggregation: N prefill servers + M decode servers
# behind sglang_router --pd-disaggregation.
#
# This is side B of the benchmark. It must match side A (launch_ray_pd.py) on
# every dimension that affects TTFT — same model, same GPU count per side, same
# tp_size, same NIXL transport, same mem-fraction — so the measured delta is
# Ray Serve routing overhead and nothing else.
#
# Prints NATIVE_PD_READY on stdout once the router answers /health.

set -euo pipefail

MODEL="${MODEL:-Qwen/Qwen2.5-7B-Instruct}"
PREFILL_REPLICAS="${PREFILL_REPLICAS:-1}"
DECODE_REPLICAS="${DECODE_REPLICAS:-1}"
TP_SIZE="${TP_SIZE:-1}"
MEM_FRACTION="${MEM_FRACTION:-0.85}"
ROUTER_PORT="${ROUTER_PORT:-8000}"
BASE_PORT="${BASE_PORT:-30000}"
BOOTSTRAP_BASE_PORT="${BOOTSTRAP_BASE_PORT:-9000}"
LOG_DIR="${LOG_DIR:-/tmp/native_pd_logs}"
STARTUP_TIMEOUT="${STARTUP_TIMEOUT:-1200}"

mkdir -p "$LOG_DIR"

PIDS=()
cleanup() {
  for pid in "${PIDS[@]:-}"; do
    kill "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT INT TERM

gpu_id=0
PREFILL_URLS=()
DECODE_URLS=()

# --- prefill servers: low GPUs, matching launch_ray_pd.py's layout -----------
for ((i = 0; i < PREFILL_REPLICAS; i++)); do
  port=$((BASE_PORT + i))
  bootstrap_port=$((BOOTSTRAP_BASE_PORT + i))
  gpus=$(seq -s, "$gpu_id" $((gpu_id + TP_SIZE - 1)))
  CUDA_VISIBLE_DEVICES="$gpus" python -m sglang.launch_server \
    --model-path "$MODEL" \
    --disaggregation-mode prefill \
    --disaggregation-transfer-backend nixl \
    --disaggregation-bootstrap-port "$bootstrap_port" \
    --tp-size "$TP_SIZE" \
    --mem-fraction-static "$MEM_FRACTION" \
    --host 127.0.0.1 \
    --port "$port" \
    >"$LOG_DIR/prefill_$i.log" 2>&1 &
  PIDS+=($!)
  PREFILL_URLS+=("http://127.0.0.1:$port")
  gpu_id=$((gpu_id + TP_SIZE))
done

# --- decode servers: remaining GPUs -----------------------------------------
for ((i = 0; i < DECODE_REPLICAS; i++)); do
  port=$((BASE_PORT + PREFILL_REPLICAS + i))
  gpus=$(seq -s, "$gpu_id" $((gpu_id + TP_SIZE - 1)))
  CUDA_VISIBLE_DEVICES="$gpus" python -m sglang.launch_server \
    --model-path "$MODEL" \
    --disaggregation-mode decode \
    --disaggregation-transfer-backend nixl \
    --tp-size "$TP_SIZE" \
    --mem-fraction-static "$MEM_FRACTION" \
    --host 127.0.0.1 \
    --port "$port" \
    >"$LOG_DIR/decode_$i.log" 2>&1 &
  PIDS+=($!)
  DECODE_URLS+=("http://127.0.0.1:$port")
  gpu_id=$((gpu_id + TP_SIZE))
done

# --- wait for every worker to answer /health before starting the router ------
wait_for_health() {
  local url="$1" deadline=$((SECONDS + STARTUP_TIMEOUT))
  while ((SECONDS < deadline)); do
    if curl -sf "$url/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "TIMEOUT waiting for $url — see $LOG_DIR" >&2
  return 1
}

for url in "${PREFILL_URLS[@]}" "${DECODE_URLS[@]}"; do
  wait_for_health "$url"
done

# --- router ------------------------------------------------------------------
ROUTER_ARGS=(--pd-disaggregation --host 127.0.0.1 --port "$ROUTER_PORT")
for ((i = 0; i < ${#PREFILL_URLS[@]}; i++)); do
  # Prefill workers are registered with their bootstrap port for KV rendezvous.
  ROUTER_ARGS+=(--prefill "${PREFILL_URLS[$i]}" $((BOOTSTRAP_BASE_PORT + i)))
done
for url in "${DECODE_URLS[@]}"; do
  ROUTER_ARGS+=(--decode "$url")
done

python -m sglang_router.launch_router "${ROUTER_ARGS[@]}" \
  >"$LOG_DIR/router.log" 2>&1 &
PIDS+=($!)

wait_for_health "http://127.0.0.1:$ROUTER_PORT"
echo "NATIVE_PD_READY"

# Stay alive so the benchmark can hit the router; the driver kills this PID.
wait
