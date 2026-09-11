#!/usr/bin/env bash
# Run client traffic against one deployed router variant.

set -euo pipefail

CALLER_PWD="$PWD"
cd "$(dirname "$0")"
source ./env.sh

ROUTER=""; CONC=""; CELL_DIR=""; DAG_FILE=""; SEED_FILE=""; TRIAL=""; PRE_PROFILE_HOOK=""
CLIENT_PYTHON=""; REPLICAS=4; ROOTS=16
REQUEST_TIMEOUT_SECONDS=${REQUEST_TIMEOUT_SECONDS:-900}
FAILED_REQUEST_THRESHOLD=${FAILED_REQUEST_THRESHOLD:-0.15}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --router) ROUTER="$2"; shift 2 ;;
    --conc) CONC="$2"; shift 2 ;;
    --cell-dir) CELL_DIR="$2"; shift 2 ;;
    --dag-file) DAG_FILE="$2"; shift 2 ;;
    --seed-file) SEED_FILE="$2"; shift 2 ;;
    --client-python) CLIENT_PYTHON="$2"; shift 2 ;;
    --trial) TRIAL="$2"; shift 2 ;;
    --replicas) REPLICAS="$2"; shift 2 ;;
    --roots) ROOTS="$2"; shift 2 ;;
    --pre-profile-hook) PRE_PROFILE_HOOK="$2"; shift 2 ;;
    *) echo "unknown arg: $1" >&2; exit 2 ;;
  esac
done

[[ "$ROUTER" == "session-affinity" || "$ROUTER" == "pure-kv-cache" || "$ROUTER" == "kv-token-aware" ]] || {
  echo "unknown router variant: $ROUTER" >&2; exit 2;
}
[[ -n "$CONC" && -n "$CELL_DIR" && -n "$DAG_FILE" && -n "$SEED_FILE" && -n "$TRIAL" && -n "$CLIENT_PYTHON" ]] || {
  echo "need --router --conc --cell-dir --dag-file --seed-file --trial --client-python" >&2; exit 2;
}
[[ -x "$CLIENT_PYTHON" ]] || { echo "missing client Python: $CLIENT_PYTHON" >&2; exit 2; }
export AIPERF_PYTHON="$CLIENT_PYTHON"
for PATH_VAR in CELL_DIR DAG_FILE SEED_FILE; do
  VALUE="${!PATH_VAR}"
  if [[ "$VALUE" != /* ]]; then printf -v "$PATH_VAR" '%s/%s' "$CALLER_PWD" "$VALUE"; fi
done
[[ -f "$DAG_FILE" && -f "$SEED_FILE" ]] || { echo "DAG or seed file is missing" >&2; exit 2; }
[[ -f "$CELL_DIR/meta.json" && -d "$CELL_DIR/routing" ]] || {
  echo "cell must contain deployment metadata and routing logs: $CELL_DIR" >&2; exit 2;
}
[[ ! -e "$CELL_DIR/aiperf_artifacts" && ! -e "$CELL_DIR/seed_summary.json" ]] || {
  echo "traffic artifacts already exist: $CELL_DIR" >&2; exit 2;
}

[[ -n "${AGENTIC_SESSION_PREFIX:-}" ]] || {
  echo "AGENTIC_SESSION_PREFIX is required and must match the generated DAG" >&2
  exit 2
}

mkdir -p "$CELL_DIR/routing" "$CELL_DIR/aiperf_artifacts"
LOG="$CELL_DIR/cell.log"
exec > >(tee -a "$LOG") 2>&1
echo "=== async-rl cell=$CELL_DIR router_variant=$ROUTER client_turn_conc=$CONC trial=$TRIAL ==="
echo "roots=$ROOTS replicas=$REPLICAS"
date -u +"start_utc=%Y-%m-%dT%H:%M:%SZ"

echo "--- validate async RL workload ---"
"$AIPERF_PYTHON" - "$DAG_FILE" "$SEED_FILE" "$ROOTS" <<'PY'
import os
import sys

from aiperf.common.enums import ConversationContextMode
from aiperf.dataset.loader.dag_jsonl import DagJsonlLoader

dag, seed, roots = sys.argv[1], sys.argv[2], int(sys.argv[3])
rollouts = DagJsonlLoader(dag).load()
seed_rollouts = DagJsonlLoader(seed).load()
prefix = os.environ["AGENTIC_SESSION_PREFIX"]
if len(rollouts) != roots or len(seed_rollouts) != 32:
    raise SystemExit(f"unexpected rollout/seed count: {len(rollouts)}/{len(seed_rollouts)}")
if any(not rollout.is_root or rollout.branches for rollout in rollouts):
    raise SystemExit("rollouts must be independent root conversations")
if any(rollout.context_mode != ConversationContextMode.DELTAS_WITHOUT_RESPONSES for rollout in rollouts):
    raise SystemExit("rollouts must append streamed assistant responses")
if any(len(rollout.turns) != 10 for rollout in rollouts):
    raise SystemExit("each rollout must have exactly ten serial turns")
if any(rollout.turns[0].timestamp is None for rollout in rollouts):
    raise SystemExit("every rollout turn 0 requires a fixed-schedule timestamp")
session_ids = [rollout.session_id for rollout in rollouts]
if len(set(session_ids)) != roots or any(not session_id.startswith(f"{prefix}-") for session_id in session_ids):
    raise SystemExit("rollout session IDs must be globally unique and stable")
print(f"[async-rl] validated {roots} independent ten-turn rollouts")
PY

# Calibrate unique straggler IDs after the live session-affinity ring exists.
if [[ -n "$PRE_PROFILE_HOOK" ]]; then
  [[ -x "$PRE_PROFILE_HOOK" ]] || { echo "pre-profile hook is not executable: $PRE_PROFILE_HOOK"; exit 2; }
  echo "--- pre-profile benchmark preparation hook ---"
  "$PRE_PROFILE_HOOK" "$CELL_DIR" "$DAG_FILE" "$SEED_FILE" "$ROUTER" "$ROOTS"
  [[ -s "$DAG_FILE" && -s "$SEED_FILE" ]] || { echo "pre-profile hook left an invalid DAG or seed file"; exit 1; }
fi

COMMON=(
  profile --url http://localhost:8000 --endpoint /v1/chat/completions
  --endpoint-type chat --streaming --model "$MODEL" --session-header x-correlation-id
  --custom-dataset-type dag_jsonl --dataset-sampling-strategy sequential --no-allow-dataset-wrap
  --random-seed "$TRIAL" --request-timeout-seconds "$REQUEST_TIMEOUT_SECONDS"
  --failed-request-threshold "$FAILED_REQUEST_THRESHOLD" --use-server-token-count
  --no-gpu-telemetry --no-server-metrics --tokenizer-trust-remote-code
  --slice-duration 1.0 --stats-interval 30
)

echo "--- excluded deterministic global-prefix seed ---"
# The seed warms only cache state. A small non-streaming client avoids making
# the benchmark depend on AIPerf's streaming-result bookkeeping outside the
# measured closed-loop replay.
SEED_RC=0
"$AIPERF_PYTHON" "$SCRIPT_DIR/seed_prefix_cache.py" \
  --input-file "$SEED_FILE" --url http://localhost:8000 \
  --session-header x-correlation-id --concurrency "$CONC" \
  --out "$CELL_DIR/seed_summary.json"

PROFILE_START_S="$(date +%s.%N)"
echo "profile_start_epoch_s=$PROFILE_START_S"
echo "--- live-response DAG profiling replay ---"
# Hold the global client credit until each stream finishes. This creates exactly
# CONC outstanding HTTP turns from the ready pool of all 80 rollout sessions.
PROFILE_SESSION_CONCURRENCY="$ROOTS"
export AIPERF_CLOSED_LOOP_TURN_CONCURRENCY=1
PROFILE_ARGS=(
  "${COMMON[@]}" --input-file "$DAG_FILE" --concurrency "$PROFILE_SESSION_CONCURRENCY"
  --prefill-concurrency "$CONC" --num-conversations "$ROOTS"
  --fixed-schedule --fixed-schedule-auto-offset
  --trajectory-start-min-ratio 0 --trajectory-start-max-ratio 0
  --export-level raw --output-artifact-dir "$CELL_DIR/aiperf_artifacts"
)
printf '%q ' "$AIPERF_PYTHON" -m aiperf "${PROFILE_ARGS[@]}" > "$CELL_DIR/aiperf_command.txt"; echo >> "$CELL_DIR/aiperf_command.txt"
set +e
"$AIPERF_PYTHON" -m aiperf "${PROFILE_ARGS[@]}"
PROFILE_RC=$?
set -e

PROFILE_LOG="$CELL_DIR/aiperf_artifacts/logs/aiperf.log"
if [[ ! -s "$PROFILE_LOG" ]]; then
  echo "FAIL: profiling log is missing"; PROFILE_RC=1
elif rg --quiet 'grace_period_timeout=True|Phase profiling timed out, cancelling all credits' "$PROFILE_LOG"; then
  TIMEOUT_STATS="$(rg 'Phase profiling timed out, cancelling all credits\. Stats:' "$PROFILE_LOG" | tail -n 1 || true)"
  if [[ "$TIMEOUT_STATS" =~ cancelled=([0-9]+),[[:space:]]in_flight=([0-9]+) ]] && [[ "${BASH_REMATCH[1]}" -eq 0 && "${BASH_REMATCH[2]}" -eq 0 ]]; then
    echo "WARN: accepting AIPerf's empty-credit grace timeout"
    # AIPerf returns a nonzero process status for this DAG bookkeeping edge
    # even though its own final counters attest that all HTTP credits drained.
    # The complete export is valid, so normalize the status after recording
    # the warning; nonzero cancelled/in-flight credits remain a hard failure.
    PROFILE_RC=0
  else
    echo "FAIL: profiling did not drain cleanly"; PROFILE_RC=1
  fi
fi
echo "aiperf_rc=$PROFILE_RC"

# Wait for terminal lifecycle events before validating SelectionService state.
sleep 2

ROUTING_VALIDATION_RC=0
set +e
python - "$CELL_DIR/routing" "$CELL_DIR/aiperf_artifacts/profile_export.jsonl" "$ROUTER" "$PROFILE_START_S" "$REPLICAS" "$AGENTIC_SESSION_PREFIX" <<'PY'
import json
import pathlib
import sys

directory, profile_path, router_variant, start, replicas, session_prefix = pathlib.Path(sys.argv[1]), pathlib.Path(sys.argv[2]), sys.argv[3], float(sys.argv[4]), int(sys.argv[5]), sys.argv[6]
profile_request_ids = set()
profile_end_s = None
for line in profile_path.open():
    try:
        row = json.loads(line)
        metadata = row.get("metadata") or {}
        request_id = metadata.get("x_request_id")
        request_end_ns = metadata.get("request_end_ns")
    except json.JSONDecodeError:
        continue
    if request_id:
        profile_request_ids.add(str(request_id))
    if isinstance(request_end_ns, (int, float)):
        profile_end_s = max(profile_end_s or 0.0, float(request_end_ns) / 1e9)
if not profile_request_ids:
    raise SystemExit("profiling export has no x_request_id values")
routes, seed_replicas, profile_replicas = [], set(), set()
for path in directory.glob("routing.*.jsonl"):
    for line in path.open():
        try: row = json.loads(line)
        except json.JSONDecodeError: continue
        sid, rep = row.get("session_id"), row.get("replica_id")
        if not sid or not rep: continue
        if float(row.get("ts") or 0.0) < start:
            seed_replicas.add(str(rep))
        else:
            routes.append(row); profile_replicas.add(str(rep))
route_request_ids = {str(row.get("request_id")) for row in routes if row.get("request_id")}
missing_lifecycle_ids = profile_request_ids - route_request_ids
# Session affinity must cover its ring; KVAwareRouter may concentrate on a
# cache-rich worker while SelectionService still observes the entire fleet.
if not routes or not seed_replicas or not profile_replicas:
    raise SystemExit(f"routing coverage seed/profile/routes={len(seed_replicas)}/{len(profile_replicas)}/{len(routes)}")
valid_prefixes = (f"{session_prefix}-", f"fixed-schedule-{session_prefix}-")
invalid_session_ids = [
    r for r in routes if not str(r.get("session_id") or "").startswith(valid_prefixes)
]
if invalid_session_ids:
    raise SystemExit(
        "ingress did not preserve the rollout session header after AIPerf's fixed-schedule wrapper: "
        f"invalid_profile_session_ids={len(invalid_session_ids)} "
        f"example={invalid_session_ids[0].get('session_id')!r}"
    )
if router_variant == "session-affinity" and (len(seed_replicas) != replicas or len(profile_replicas) != replicas):
    raise SystemExit(f"session-affinity routing did not cover its ring: seed/profile={len(seed_replicas)}/{len(profile_replicas)} expected={replicas}")
if missing_lifecycle_ids:
    raise SystemExit(
        "ingress lifecycle request-id forwarding failed: "
        f"matched={len(profile_request_ids) - len(missing_lifecycle_ids)}/{len(profile_request_ids)} "
        f"missing_example={next(iter(missing_lifecycle_ids))}"
    )
placements = {}
for row in routes:
    placements.setdefault(str(row["session_id"]), set()).add(str(row["replica_id"]))
if router_variant == "session-affinity" and any(len(reps) != 1 for reps in placements.values()):
    raise SystemExit("session-affinity router failed to keep a rollout pinned")
if router_variant in {"pure-kv-cache", "kv-token-aware"}:
    missing = [r for r in routes if r.get("kv_tracker_present") is not True or r.get("kv_token_count") is None]
    selector = 0
    selector_workers = 0
    post_completion = []
    for path in directory.glob("selector_loads.*.jsonl"):
        for line in path.open():
            try: sample = json.loads(line)
            except json.JSONDecodeError: continue
            loads = [load for model in sample.get("models") or [] for load in model.get("loads") or []]
            sample_ts = float(sample.get("ts") or 0.0)
            if sample_ts >= start and loads:
                selector += 1
                selector_workers = max(selector_workers, len(loads))
            if profile_end_s is not None and sample_ts >= profile_end_s + 0.5 and loads:
                post_completion.append((sample_ts, loads))
    if missing or selector < 2 or selector_workers != replicas:
        raise SystemExit(
            "KVAware validation missing_tracker_or_tokens="
            f"{len(missing)} selector_samples={selector} selector_workers={selector_workers}"
        )
    if not post_completion:
        raise SystemExit("KVAware validation has no post-completion SelectionService sample")
    last_ts, last_loads = max(post_completion, key=lambda item: item[0])
    lingering = sum(int(load.get("active_requests") or 0) for load in last_loads)
    if lingering:
        raise SystemExit(
            "KVAware lifecycle release failed: "
            f"post_completion_active_requests={lingering} at ts={last_ts}"
        )
print(
    f"[live-append] routed: seed={len(seed_replicas)} profile={len(profile_replicas)} "
    f"requests={len(routes)} sessions={len(placements)} "
    f"lifecycle_request_id_match={len(profile_request_ids)}/{len(profile_request_ids)}"
)
PY
ROUTING_VALIDATION_RC=$?
set -e

TOKEN_VALIDATION_RC=0
set +e
python - "$CELL_DIR/aiperf_artifacts/profile_export.jsonl" "$CELL_DIR/aiperf_artifacts/profile_export_raw.jsonl" "$ROOTS" <<'PY'
import collections
import json
import os
import pathlib
import sys

profile_path = pathlib.Path(sys.argv[1])
raw_path = pathlib.Path(sys.argv[2])
roots = int(sys.argv[3])
rows = []
for line in profile_path.open():
    try:
        record = json.loads(line)
    except json.JSONDecodeError:
        continue
    metadata = record.get("metadata") or {}
    if metadata.get("benchmark_phase") not in (None, "profiling"):
        continue
    if metadata.get("was_cancelled") or record.get("error"):
        continue
    rows.append(record)
if len(rows) != int(os.environ["AGENTIC_PROFILE_REQUESTS_TOTAL"]):
    raise SystemExit(f"expected 800 complete turns, got {len(rows)}")

def metric(record, name):
    value = (record.get("metrics") or {}).get(name)
    return value.get("value") if isinstance(value, dict) else value

by_session = collections.defaultdict(list)
by_correlation = collections.defaultdict(list)
tiers = collections.Counter()
cache_telemetry = 0
for record in rows:
    metadata = record.get("metadata") or {}
    session_id = metadata.get("conversation_id")
    correlation_id = metadata.get("x_correlation_id")
    if not session_id or not correlation_id:
        raise SystemExit("missing rollout session metadata")
    by_session[session_id].append(record)
    by_correlation[correlation_id].append(record)
    output_tokens = metric(record, "output_sequence_length")
    if not isinstance(output_tokens, (int, float)):
        raise SystemExit("missing server output token count")
    tiers[int(output_tokens)] += 1
    if all(
        isinstance(metric(record, name), (int, float))
        for name in ("usage_prompt_cache_read_tokens", "usage_prompt_tokens")
    ):
        cache_telemetry += 1

if len(by_session) != roots or len(by_correlation) != roots:
    raise SystemExit("unexpected number of rollout sessions")
if any(len(turns) != 10 for turns in by_session.values()):
    raise SystemExit("a rollout lost a serial turn")
if any(not session_id.startswith(f"{os.environ['AGENTIC_SESSION_PREFIX']}-") for session_id in by_session):
    raise SystemExit("unexpected rollout session namespace")
if any(
    len({record["metadata"]["x_correlation_id"] for record in turns}) != 1
    for turns in by_session.values()
):
    raise SystemExit("a rollout changed its routing session ID")
expected_tiers = {}
for item in os.environ["AGENTIC_REQUIRED_OUTPUT_TIERS"].split(","):
    tier, count = item.split(":", 1)
    expected_tiers[int(tier)] = int(count)
if dict(tiers) != expected_tiers:
    raise SystemExit(f"unexpected output tiers: {dict(tiers)} != {expected_tiers}")
if cache_telemetry != len(rows):
    raise SystemExit("missing response-level prefix-cache telemetry")
if not raw_path.exists():
    raise SystemExit("missing raw payload export")

continuations = 0
stream_errors = []
for line in raw_path.open():
    try:
        raw = json.loads(line)
    except json.JSONDecodeError:
        continue
    metadata = raw.get("metadata") or {}
    if metadata.get("benchmark_phase") not in (None, "profiling"):
        continue
    for response in raw.get("responses") or []:
        for packet in response.get("packets") or []:
            payload = packet.get("value")
            if isinstance(payload, str) and payload.startswith("{"):
                try:
                    event = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                if event.get("error"):
                    stream_errors.append(event["error"])
    if not int(metadata.get("turn_index") or 0):
        continue
    assistants = [
        message.get("content")
        for message in ((raw.get("payload") or {}).get("messages") or [])
        if message.get("role") == "assistant" and isinstance(message.get("content"), str)
    ]
    if not any(content.strip() for content in assistants):
        raise SystemExit("continuation omitted the prior streamed assistant response")
    continuations += 1
if stream_errors:
    raise SystemExit(f"stream contained server error: {stream_errors[0]!r}")
if continuations != len(rows) - roots:
    raise SystemExit(f"expected {len(rows) - roots} live continuations, got {continuations}")

for correlation_id, turns in by_correlation.items():
    turns.sort(key=lambda record: int((record.get("metadata") or {}).get("turn_index") or 0))
    for earlier, later in zip(turns, turns[1:]):
        prior_isl = metric(earlier, "input_sequence_length")
        next_isl = metric(later, "input_sequence_length")
        if not all(isinstance(value, (int, float)) for value in (prior_isl, next_isl)):
            raise SystemExit(f"{correlation_id}: missing input length")
        if next_isl <= prior_isl + 32:
            raise SystemExit(f"{correlation_id}: continuation context did not grow")
print(f"[async-rl] validated 800 turns, {continuations} live continuations, tiers={dict(tiers)}")
PY
TOKEN_VALIDATION_RC=$?
set -e

python - "$CELL_DIR" "$PROFILE_RC" "$SEED_RC" "$CONC" "$ROUTER" "$TRIAL" "$DAG_FILE" "$SEED_FILE" "$PROFILE_START_S" "$ROUTING_VALIDATION_RC" "$TOKEN_VALIDATION_RC" "$PROFILE_SESSION_CONCURRENCY" <<'PY'
import hashlib
import json
import os
import pathlib
import sys

cell = pathlib.Path(sys.argv[1]); dag, seed = pathlib.Path(sys.argv[7]), pathlib.Path(sys.argv[8])
meta_path = cell / "meta.json"; meta = json.loads(meta_path.read_text()) if meta_path.exists() else {}
meta.update({
    "variant": sys.argv[5], "trial": int(sys.argv[6]), "concurrency": int(sys.argv[4]),
    "aiperf_rc": int(sys.argv[2]), "seed_rc": int(sys.argv[3]), "cell_dir": str(cell.resolve()),
    "workload": os.environ.get("AGENTIC_WORKLOAD_KIND", "async_rl_rollout_v1"), "dag_file": str(dag.resolve()), "seed_file": str(seed.resolve()),
    "dag_sha256": hashlib.sha256(dag.read_bytes()).hexdigest(), "seed_sha256": hashlib.sha256(seed.read_bytes()).hexdigest(),
    "routing_session_scope": "conversation", "routing_session_header": "x-correlation-id",
    "parent_scoped_affinity": False, "live_assistant_responses": True,
    "response_cache_telemetry": "usage.prompt_tokens_details.cached_tokens",
    "cpu_kv_offload_enabled": False, "profile_start_epoch_s": float(sys.argv[9]),
    "routing_validation_rc": int(sys.argv[10]), "token_validation_rc": int(sys.argv[11]),
    "closed_loop_turn_concurrency": True,
    "aiperf_session_concurrency": int(sys.argv[12]),
})
meta_path.write_text(json.dumps(meta, indent=2) + "\n")
PY

[[ "$PROFILE_RC" -eq 0 ]] || exit "$PROFILE_RC"
[[ "$ROUTING_VALIDATION_RC" -eq 0 ]] || exit "$ROUTING_VALIDATION_RC"
[[ "$TOKEN_VALIDATION_RC" -eq 0 ]] || exit "$TOKEN_VALIDATION_RC"
