# Durability harness

The point of this directory is to answer one question per substrate:

> **Does `fsync` on this filesystem actually flush data to non-volatile media before returning?**

If the answer is no, every later durability test on that substrate is meaningless — it will report success for the wrong reason. If the answer is yes, kill-9 and crash tests can be trusted.

## Why this is its own thing

REP-64's whole durability story is "a caller observes an ack only after the write is durable on disk." That's RocksDB's `WriteOptions::sync = true` translating to a real `fsync`. But **`fsync` is famously dishonest in some environments**:

- Linux guests on Type-2 hypervisors (VirtualBox, VMware Workstation, sometimes Hyper-V) where the host buffers the underlying virtual-disk file in RAM.
- macOS, where `fsync` is documented as flushing to the storage controller but not to permanent media — `fcntl(F_FULLFSYNC)` is the honest signal.
- Containers / VMs with `cache=writeback` mode at the qemu/kvm layer.
- tmpfs (which is just RAM by design — fsync on tmpfs is a no-op).

A POC that tests durability on a lying substrate proves nothing.

## What's here so far

- **`probe_fsync.py`** — measures `fsync`, `fdatasync`, and (on macOS only) `F_FULLFSYNC` latency on a directory you point it at. Self-labels the substrate as `honest` / `suspicious` / `lying` based on p50. Outputs JSON.

Phase 4 will add a kill-9 sweep harness on top of this. The probe is committed first so substrate honesty is observable from day one — including on substrates we don't control (collaborators' cloud VMs, your laptop, etc.).

## Running the probe

```bash
# Probe whatever filesystem the current directory is on:
python3 probe_fsync.py --target-dir . --output results/$(uname -n)-probe.json

# Probe an explicit substrate (e.g. an attached SSD on macOS):
python3 probe_fsync.py --target-dir /Volumes/MyExternalSSD \
  --output results/external-ssd.json
```

No third-party Python deps. Linux + macOS both work; the probe handles the platform difference internally.

## Interpreting the verdict

```jsonc
"label": {
  "verdict": "honest" | "suspicious" | "lying" |
             "honest_via_fullfsync" | "suspicious_via_fullfsync" |
             "macos_fsync_only_no_fullfsync" | "macos_no_fullfsync_data",
  "notes": [ "..." ]
}
```

| Verdict | Meaning | Use this substrate for durability tests? |
|---|---|---|
| `honest` (Linux) | `fsync` p50 ≥ 30 μs — consistent with real media flush | **Yes** |
| `suspicious` (Linux) | 5–30 μs — possible BBU cache or partial virt | With caveat |
| `lying` (Linux) | < 5 μs — host buffering | **No** — false-pass risk |
| `honest_via_fullfsync` (macOS) | `F_FULLFSYNC` p50 ≥ 1 ms | **Yes**, but the test must use F_FULLFSYNC |
| `suspicious_via_fullfsync` (macOS) | F_FULLFSYNC < 1 ms | With caveat |
| `lying` (macOS) | F_FULLFSYNC < 30 μs | **No** |

## Why `fsync` p50 ≥ 30 μs is the threshold

| Storage | Real fsync time |
|---|---|
| NVMe SSD (consumer) | 30–300 μs |
| SATA SSD | 200 μs–1 ms |
| Spinning disk | 5–15 ms |
| Battery-backed RAID controller | 5–20 μs |

Anything < 5 μs is faster than physically possible for a real flush — the syscall is acking from RAM. The 5–30 μs band is technically possible (BBU cache) but rare enough on commodity hardware that we flag it for review rather than passing it silently.

## Substrate matrix observed so far

| Date | Host | Filesystem | fsync p50 | Verdict |
|---|---|---|---|---|
| 2026-04-30 | LinkedIn dev VM (Hyper-V, AMD EPYC) | tmpfs (`/tmp`) | 1.09 μs | `lying` (expected — tmpfs is RAM) |
| 2026-04-30 | LinkedIn dev VM | ext4 on `/dev/sda3` (`/home/...`) | **3608 μs** | **`honest`** |

Earlier in Phase 1 a single fsync probe on `/tmp` returned ≈ 0.7 μs and was incorrectly read as "this VM's disk is dishonest." This more thorough sweep shows the actual ext4 substrate **is honest**; only `/tmp` (tmpfs) lies, which is by design. See the correction note in `reports/phase-1-foundations.md`.

## What to do with results from collaborators

Send a collaborator this directory + a one-line instruction:

```bash
python3 probe_fsync.py --target-dir <a-path-on-real-disk> \
  --output results/<host>-<fs>.json
```

Then commit the JSON file. The verdict + raw numbers tell us immediately whether to trust durability tests run on that substrate. No manual interpretation needed.
