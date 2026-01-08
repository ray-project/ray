#!/usr/bin/env bash
set -euo pipefail

PORT=2222

SRC="/home/ray/default/torchft/torchft/ddp.py"
DST=$SRC

IPS=(
  10.0.2.189
  10.0.9.110
)

SSH_OPTS=(-p "$PORT" -o StrictHostKeyChecking=accept-new)
SCP_OPTS=(-P "$PORT" -o StrictHostKeyChecking=accept-new)

for ip in "${IPS[@]}"; do
  echo "==> Copying to $ip"

  # Make sure destination directory exists (on the remote)
  ssh "${SSH_OPTS[@]}" "$ip" "mkdir -p '$(dirname "$DST")'"

  # Backup existing file if present
  ssh "${SSH_OPTS[@]}" "$ip" "test -f '$DST' && cp '$DST' '${DST}.bak.$(date +%s)' || true"

  # Copy
  scp "${SCP_OPTS[@]}" "$SRC" "$ip:$DST"
done

echo "Done."
