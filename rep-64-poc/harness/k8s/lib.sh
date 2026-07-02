# rep-64-poc/harness/k8s/lib.sh
# Helper functions sourced by every script in this harness. Pure bash, no
# external dependencies beyond kubectl + envsubst + jq.

set -u  # bare `set -e` is footgunny when sourcing; let callers opt in.

log()   { printf '[%s] %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
abort() { log "ABORT: $*"; exit 1; }

# require_env <var> [<var> ...]: abort if any var is unset or empty.
require_env() {
  local missing=0
  for var in "$@"; do
    if [[ -z "${!var:-}" ]]; then
      log "missing required env var: $var"
      missing=1
    fi
  done
  (( missing == 0 )) || abort "set the missing env vars in \$HARNESS_ENV_FILE"
}

# load_env: sources $HARNESS_ENV_FILE and switches kubectl context. Idempotent.
# set -a / set +a causes all assignments in the env file to be exported so that
# render_manifest's envsubst call can see them (envsubst only substitutes exported vars).
load_env() {
  : "${HARNESS_ENV_FILE:?set HARNESS_ENV_FILE=rep-64-poc/harness/k8s/env/k3d.env}"
  [[ -f "$HARNESS_ENV_FILE" ]] || abort "env file not found: $HARNESS_ENV_FILE"
  set -a
  # shellcheck disable=SC1090
  source "$HARNESS_ENV_FILE"
  set +a
  require_env KUBE_CONTEXT NAMESPACE IMAGE STORAGE_CLASS RESULTS_DIR
  kubectl config use-context "$KUBE_CONTEXT" >/dev/null 2>&1 \
    || log "kubectl context '$KUBE_CONTEXT' not yet available (will be created by the setup script)"
}

# render_manifest <template-file>: prints the rendered YAML to stdout.
# The template uses $VAR or ${VAR} envsubst syntax. We restrict envsubst to
# only the vars that appear in our env files so unrelated env doesn't leak in.
render_manifest() {
  local tmpl="$1"
  [[ -f "$tmpl" ]] || abort "template not found: $tmpl"
  local vars
  vars="$(grep -oE '\$\{?[A-Z][A-Z0-9_]+\}?' "$tmpl" | sed -E 's/[${}]//g' | sort -u | sed 's/^/$/' | tr '\n' ' ')"
  envsubst "$vars" < "$tmpl"
}

# wait_ray_ready <namespace> <raycluster-name> <timeout_s>: polls until the
# RayCluster's State is Ready or timeout. Returns 0 on Ready, 1 on timeout.
wait_ray_ready() {
  local ns="$1" name="$2" timeout="$3"
  local start; start=$(date +%s)
  while :; do
    local state
    state="$(kubectl get raycluster -n "$ns" "$name" -o jsonpath='{.status.state}' 2>/dev/null || true)"
    if [[ "$state" == "ready" ]]; then
      log "RayCluster $name is ready"
      return 0
    fi
    local now; now=$(date +%s)
    if (( now - start > timeout )); then
      log "RayCluster $name not ready after ${timeout}s (last state: ${state:-<empty>})"
      kubectl get raycluster -n "$ns" "$name" -o yaml | head -80 >&2
      return 1
    fi
    sleep 5
  done
}

# wait_pod_ready <namespace> <pod-label-selector> <timeout_s>
wait_pod_ready() {
  local ns="$1" sel="$2" timeout="$3"
  kubectl wait --for=condition=Ready pods -n "$ns" -l "$sel" --timeout="${timeout}s"
}

# get_image_digest <namespace> <pod-name>: returns the runtime image digest.
get_image_digest() {
  local ns="$1" pod="$2"
  kubectl get pod -n "$ns" "$pod" -o jsonpath='{.status.containerStatuses[0].imageID}' \
    | sed -E 's/^.*@//'
}

# write_result <test-name> <status> <duration_s> <metrics-json> [skip_reason]
# Emits a JSON result file to $RESULTS_DIR/<test>-<timestamp>.json with the
# common envelope. <metrics-json> must be a valid JSON object.
write_result() {
  local test_name="$1" status="$2" duration="$3" metrics="$4"
  local skip_reason="${5:-null}"
  [[ "$skip_reason" == "null" ]] || skip_reason="\"$skip_reason\""
  mkdir -p "$RESULTS_DIR"
  local out="$RESULTS_DIR/${test_name}-$(date -u +%Y%m%dT%H%M%SZ).json"
  local k8s_version
  k8s_version="$(kubectl version -o json 2>/dev/null | jq -r '.serverVersion.gitVersion // "unknown"')"
  local kuberay_version
  kuberay_version="$(kubectl get deploy -A -l app.kubernetes.io/name=kuberay-operator \
    -o jsonpath='{.items[0].spec.template.spec.containers[0].image}' 2>/dev/null \
    | sed -E 's/^.*://' || echo unknown)"
  local node_count
  node_count="$(kubectl get nodes -o json | jq '.items | length')"
  local git_commit
  git_commit="$(git -C "$(dirname "$HARNESS_ENV_FILE")/../../.." rev-parse HEAD 2>/dev/null || echo unknown)"
  jq -n \
    --arg test_name "$test_name" \
    --arg tier "$(basename "${HARNESS_ENV_FILE%.env}")" \
    --arg status "$status" \
    --arg started_at "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson duration "$duration" \
    --arg kube_context "$KUBE_CONTEXT" \
    --arg namespace "$NAMESPACE" \
    --arg kuberay_version "$kuberay_version" \
    --argjson node_count "$node_count" \
    --arg k8s_server_version "$k8s_version" \
    --arg image "$IMAGE" \
    --arg git_commit "$git_commit" \
    --argjson metrics "$metrics" \
    --argjson skip_reason "$skip_reason" \
    '{
      schema_version: 1,
      test_name: $test_name,
      tier: $tier,
      started_at: $started_at,
      duration_s: $duration,
      status: $status,
      skip_reason: $skip_reason,
      cluster: {
        kube_context: $kube_context,
        namespace: $namespace,
        kuberay_operator_version: $kuberay_version,
        node_count: $node_count,
        k8s_server_version: $k8s_server_version
      },
      image: $image,
      git_commit: $git_commit,
      metrics: $metrics,
      errors: []
    }' > "$out"
  log "wrote $out"
}
