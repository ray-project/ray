#!/usr/bin/env bash
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"

WEBHOOK_URL="${WEBHOOK_URL:-${CURSOR_TEMPLATE_UPDATER_WEBHOOK:-}}"
WEBHOOK_AUTH_TOKEN="${WEBHOOK_AUTH_TOKEN:-${CURSOR_TEMPLATE_UPDATER_AUTH_TOKEN:-}}"

if [[ -z "$WEBHOOK_URL" || -z "$WEBHOOK_AUTH_TOKEN" ]]; then
  echo "set WEBHOOK_URL + WEBHOOK_AUTH_TOKEN (or the CURSOR_TEMPLATE_UPDATER_WEBHOOK / CURSOR_TEMPLATE_UPDATER_AUTH_TOKEN defaults)" >&2
  exit 1
fi

# Strip the "ray-" prefix; agent PR titles use the bare "2.x.y" form.
TARGET_RAY="${TARGET_RAY:-$(gh release view --repo ray-project/ray --json tagName --jq .tagName | sed 's/^ray-//')}"
echo "target_ray=$TARGET_RAY" >&2

# Skip-list: open + merged [ray-update-$TARGET_RAY] PRs on anyscale/templates.
# Merged added first so it wins on lookup. Closed-not-merged are intentionally
# excluded — those are abandoned attempts worth re-firing.
echo "skip-list (open + merged PRs already bumping to $TARGET_RAY):" >&2
skip_list=""
for state in merged open; do
  while IFS=$'\t' read -r title url; do
    [[ -z "$title" ]] && continue
    # TODO(hardening): dots in $TARGET_RAY are raw regex here. Safe today (the
    # workflow sets the value); escape if we ever take user input.
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

skip_entry_for() {
  local n="$1"
  printf '%s' "$skip_list" | awk -v n="$n" -F'\t' '$1 == n { print $2 ": " $3; exit }'
}

RUNS_DIR="$HERE/runs/$(date -u +%Y%m%dT%H%M%SZ)"

ok=0
fail=0
skipped=0
dry=0
failed_names=()

while IFS= read -r entry; do
  name="$(jq -r '.name' <<<"$entry")"
  dir="$(jq -r '.dir' <<<"$entry")"

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
