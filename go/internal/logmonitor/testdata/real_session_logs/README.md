# Real Session Logs for Replay Testing

This directory contains captured real Ray session logs for replay parity testing between Go and Python log monitors.

## Refresh Instructions

To update the snapshot:

1. Run a Ray cluster with the desired workload
2. Copy the session logs from `/tmp/ray/session_latest/logs/` to this directory
3. Remove any sensitive or large files

## Normalization Rules

The replay test normalizes the following fields when comparing Go and Python outputs:

- `pid`: Process ID as string
- `job`: Job ID (e.g., "01000000")
- `is_err`: Whether the log is from stderr
- `actor_name`: Actor name if applicable
- `task_name`: Task name if applicable
- `lines`: Log lines (normalized for line endings)

## Files

- `*.out` / `*.err`: Worker and component log files
- `gcs_server.*`: GCS server logs
- `raylet.*`: Raylet logs
- `monitor.*`: Monitor process logs
