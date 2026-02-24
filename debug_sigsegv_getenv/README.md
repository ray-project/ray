# SIGSEGV in getenv() — Reproduction

Reproduces the crash seen in Ray's OpenTelemetry metric exporter where
`grpc_core::GetEnv()` → `getenv()` segfaults due to a concurrent `setenv()`
from a Python worker thread.

## Root Cause

glibc's `setenv()` can reallocate the `environ` array while `getenv()` on
another thread is iterating it (use-after-free). This is a known glibc
limitation fixed in glibc ≥2.40.

## Files

- **`repro_getenv_race.c`** — Minimal C reproduction. Spawns writer threads
  calling `setenv()` and reader threads calling `getenv()` concurrently.
  Crashes almost instantly on glibc <2.40.

- **`repro_getenv_race.py`** — Python reproduction that mimics the actual Ray
  crash path: `os.environ` mutations racing with gRPC channel creation
  (which triggers internal `getenv()` calls during DNS resolution).

## Building and Running

```bash
# C reproduction (fastest to crash)
gcc -O2 -pthread -o repro_getenv_race repro_getenv_race.c
./repro_getenv_race

# Python reproduction (requires grpcio)
pip install grpcio
python repro_getenv_race.py
```

## Expected Results

- **glibc <2.40** (Ubuntu 24.04, RHEL 9, etc.): SIGSEGV within seconds
- **glibc ≥2.40** (Fedora 41, RHEL 10, etc.): Completes without crash
