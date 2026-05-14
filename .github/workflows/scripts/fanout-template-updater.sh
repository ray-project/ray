#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

WEBHOOK_URL="${WEBHOOK_URL:-${CURSOR_TEMPLATE_UPDATER_WEBHOOK:-}}"
WEBHOOK_AUTH_TOKEN="${WEBHOOK_AUTH_TOKEN:-${CURSOR_TEMPLATE_UPDATER_AUTH_TOKEN:-}}"

if [[ -z "$WEBHOOK_URL" || -z "$WEBHOOK_AUTH_TOKEN" ]]; then
  echo "set WEBHOOK_URL + WEBHOOK_AUTH_TOKEN (or the CURSOR_TEMPLATE_UPDATER_WEBHOOK / CURSOR_TEMPLATE_UPDATER_AUTH_TOKEN defaults)" >&2
  exit 1
fi

# Resolve target Ray version once. The agent's PR titles use a bare "2.x.y"
# (e.g. "[ray-update-2.55.1] Update foo to Ray 2.55.1"), so strip the "ray-"
# prefix off the GitHub release tag (which looks like "ray-2.55.1").
TARGET_RAY="${TARGET_RAY:-$(gh release view --repo ray-project/ray --json tagName --jq .tagName | sed 's/^ray-//')}"
echo "target_ray=$TARGET_RAY" >&2

# Skip-list: open OR merged PRs against anyscale/templates whose title contains
# the [ray-update-<TARGET_RAY>] marker (the agent's own convention). A merged
# PR means the bump already shipped — re-triggering would be a no-op. An open
# PR means a run is already in flight. Closed-not-merged are deliberately NOT
# in the skip-list (those are abandoned attempts worth re-firing).
# Merged entries are added first so they win on lookup if both states exist.
echo "skip-list (open + merged PRs already bumping to $TARGET_RAY):" >&2
skip_list=""
for state in merged open; do
  while IFS=$'\t' read -r title url; do
    [[ -z "$title" ]] && continue
    # TODO(hardening): dots in ${TARGET_RAY} (e.g. 2.55.1) are interpolated raw
    # and act as regex wildcards. Real agent PR titles won't false-match; if we
    # ever take user-supplied versions, escape the dots before splicing.
    name="$(printf '%s' "$title" | sed -nE "s/^\[ray-update-${TARGET_RAY}\] Update (.+) to Ray ${TARGET_RAY}.*$/\1/p")"
    if [[ -z "$name" ]]; then
      echo "  WARN unparseable PR title: $title ($url)" >&2
      continue
    fi
    skip_list+="$name"$'\t'"$state"$'\t'"$url"$'\n'
    echo "  $name  ($state: $url)" >&2
  done < <(gh pr list --repo anyscale/templates --state "$state" --search "ray-update-${TARGET_RAY} in:title" --limit 100 --json title,url --jq '.[] | [.title, .url] | @tsv')
done
if [[ -z "$skip_list" ]]; then
  echo "  (none)" >&2
fi

# Lookup helper: echoes "<state>: <url>" for a template name if present.
# Merged wins over open because we add merged first and exit on first match.
skip_entry_for() {
  local n="$1"
  printf '%s' "$skip_list" | awk -v n="$n" -F'\t' '$1 == n { print $2 ": " $3; exit }'
}

# Per-run dir for captured response bodies. Created lazily on first real call.
RUNS_DIR="$HERE/runs/$(date -u +%Y%m%dT%H%M%SZ)"

ok=0
fail=0
skipped=0
dry=0
failed_names=()

while IFS= read -r entry; do
  name="$(jq -r '.name' <<<"$entry")"
  dir="$(jq -r '.dir' <<<"$entry")"

  # Optional one-template filter (used by smoke test).
  if [[ -n "${SMOKE_TEMPLATE:-}" && "$SMOKE_TEMPLATE" != "$name" ]]; then
    continue
  fi

  existing="$(skip_entry_for "$name")"
  if [[ -n "$existing" ]]; then
    echo "SKIP $name :: existing PR ($existing)"
    skipped=$((skipped + 1))
    continue
  fi

  payload="$(jq -nc --arg n "$name" --arg d "$dir" --arg t "$TARGET_RAY" '{template: $n, dir: $d, target_ray: $t}')"

  if [[ "${DRY_RUN:-0}" == "1" ]]; then
    echo "DRY  $name :: $payload"
    dry=$((dry + 1))
    continue
  fi

  mkdir -p "$RUNS_DIR"
  body_file="$RUNS_DIR/${name}.json"

  code="$(curl -sS -o "$body_file" -w '%{http_code}' \
    -X POST \
    -H "Authorization: Bearer $WEBHOOK_AUTH_TOKEN" \
    -H "Content-Type: application/json" \
    -d "$payload" \
    "$WEBHOOK_URL")" || code="000"

  if [[ "$code" =~ ^2 ]]; then
    echo "OK   $name [$code] -> $body_file"
    ok=$((ok + 1))
  else
    echo "FAIL $name [$code] $(head -c 200 "$body_file")" >&2
    fail=$((fail + 1))
    failed_names+=("$name")
  fi
done < <("$HERE/list-templates.sh" | jq -c '.[]')

echo "---"
echo "$ok ok / $skipped skipped / $fail failed / $dry dry"
if [[ "${DRY_RUN:-0}" != "1" && -d "$RUNS_DIR" ]]; then
  echo "responses: $RUNS_DIR"
fi

if ((fail > 0)); then
  printf 'failed: %s\n' "${failed_names[@]}" >&2
  exit 1
fi
