#!/usr/bin/env bash
# REP-64 POC, Phase 8: cloud-volume fsync sanity check.
#
# Runs the Phase 1 fsync probe + the Phase 4 kill-9 harness against a
# mounted cloud volume, to verify the durability chain on real cloud
# block storage. Designed to run on a cloud VM (EC2 with EBS, GCE with
# Persistent Disk, or equivalent). Cannot run on this dev VM (no cloud
# access).
#
# Usage on a cloud VM (assumes the volume is already mounted at $1):
#   bash rep-64-poc/harness/cloud/cloud_fsync_check.sh /mnt/data
#
# Outputs JSON files under $MOUNT/rep64-poc-results/ that should be
# committed back to this repo's harness/durability/results/ when the
# probe is run on a collaborator's host.

set -euo pipefail

MOUNT="${1:-}"
if [[ -z "$MOUNT" ]]; then
  echo "usage: $0 <mount_path>"
  echo "  mount_path: a directory on the cloud volume under test"
  exit 2
fi
if [[ ! -d "$MOUNT" ]]; then
  echo "error: $MOUNT is not a directory"
  exit 1
fi

# Sanity: the mount should be on a real block device, not tmpfs or overlay.
SUBSTRATE=$(stat -f -c '%T' "$MOUNT")
echo "# substrate type at $MOUNT: $SUBSTRATE"
case "$SUBSTRATE" in
  tmpfs|ramfs|overlay|overlayfs)
    echo "WARN: $MOUNT looks like an in-memory or overlay fs; results will not"
    echo "      reflect real block-storage durability. Continue at your own risk." >&2
    ;;
esac

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
RESULTS="$MOUNT/rep64-poc-results"
mkdir -p "$RESULTS"

# 1. Phase 1 fsync-honesty probe on the cloud volume.
echo "==> Phase 1 fsync probe"
python3 "$REPO_ROOT/rep-64-poc/harness/durability/probe_fsync.py" \
  --target-dir "$MOUNT/probe-fsync" \
  --output "$RESULTS/probe_fsync.json"

# 2. Phase 4 kill-9 harness, sync=true (positive case).
echo "==> Phase 4 kill-9, sync=true"
python3 "$REPO_ROOT/rep-64-poc/harness/durability/kill9/run_kill9.py" \
  --writer "$REPO_ROOT/bazel-bin/rep-64-poc/harness/durability/kill9/writer" \
  --verifier "$REPO_ROOT/bazel-bin/rep-64-poc/harness/durability/kill9/verifier" \
  --num-writes 1000 --sync 1 \
  --db-path "$MOUNT/kill9-sync1-db" \
  --output "$RESULTS/kill9_sync1.json"

# 3. Phase 4 ghost-writes negative control (should FAIL with verdict==FAIL).
echo "==> Phase 4 kill-9, ghost-writes (negative control)"
python3 "$REPO_ROOT/rep-64-poc/harness/durability/kill9/run_kill9.py" \
  --writer "$REPO_ROOT/bazel-bin/rep-64-poc/harness/durability/kill9/writer" \
  --verifier "$REPO_ROOT/bazel-bin/rep-64-poc/harness/durability/kill9/verifier" \
  --num-writes 1000 --sync 1 --ghost-writes 50 --kill-after-ack 10000 \
  --db-path "$MOUNT/kill9-ghost-db" \
  --output "$RESULTS/kill9_ghost.json" || true  # expected to exit 1

# 4. Phase 7 microbenchmark on the cloud volume.
echo "==> Phase 7 microbench"
"$REPO_ROOT/bazel-bin/rep-64-poc/harness/microbench/storage_microbench" \
  --db-dir "$MOUNT/microbench-db" \
  --output "$RESULTS/microbench.json"

# 5. Phase 8 recovery-time benchmark.
echo "==> Phase 8 recovery"
python3 "$REPO_ROOT/rep-64-poc/harness/recovery/run_recovery.py" \
  --bin "$REPO_ROOT/bazel-bin/rep-64-poc/harness/recovery/recovery_bench" \
  --db-root "$MOUNT/recovery-db" \
  --sizes "100,1000,10000" \
  --output "$RESULTS/recovery.json"

echo
echo "Done. Results under $RESULTS:"
ls -la "$RESULTS"
echo
echo "Next: copy $RESULTS into rep-64-poc/harness/{durability,microbench,recovery}/results/"
echo "and commit, with a one-line note in the relevant Phase report identifying"
echo "the substrate (e.g. 'Run on AWS EBS gp3 in us-east-1, 2026-04-30')."
