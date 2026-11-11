#!/bin/bash
# Run rounds sequentially: campaign.sh "mode:variant:tag" ...
# Each ON round first installs the variant's observer dist, Lua and Python files.
set -uo pipefail
V=/mnt/local_storage/sm/variants
R=/home/ray/default/ray/python/ray
cd /home/ray/default/sm_work || exit 1
for spec in "$@"; do
  IFS=: read -r mode variant tag <<< "$spec"
  if [ "$variant" != "-" ]; then
    /mnt/local_storage/sm/use_variant.sh "$(cat "$V/$variant/dist")" > /dev/null
    cp "$V/$variant/stream_metrics.lua" "$R/serve/_private/stream_metrics.lua"
    cp "$V/$variant/llm_metrics.py" "$R/llm/_internal/serve/observability/metrics/llm_metrics.py"
  fi
  echo "$(date +%T) START $mode $variant $tag"
  RUNS=${RUNS_ROOT:-/mnt/local_storage/sm/runs}
  if ./round.sh "$mode" "$RUNS/$tag" > "$RUNS/$tag.log" 2>&1; then
    echo "$(date +%T) DONE $tag"
  else
    echo "$(date +%T) FAILED $tag"
  fi
done
echo "CAMPAIGN COMPLETE"
