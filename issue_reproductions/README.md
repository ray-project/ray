# Ray Core Correctness & Stability Issues Analysis

Analysis of recent (Nov 2025 - Feb 2026) open GitHub issues in ray-project/ray,
filtered to Ray Core correctness/stability bugs that are reproducible on a single
machine with sufficient information to attempt reproduction.

## Methodology

1. Scraped 194 open issues from the last 3 months using GitHub search
2. Filtered to Ray Core (not Ray Data, Serve, RLlib, etc.) correctness/stability bugs
3. Filtered to issues with enough reproduction information
4. Filtered to single-machine reproducible issues
5. Ranked by criticality and analyzed the top 5

## Top 5 Issues (Ranked by Criticality)

### 1. [#60910] Actor tasks hang when head-of-line task's dependency resolution fails
- **File:** `repro_60910_actor_hang_v3.py`
- **Severity:** Critical (silent deadlock)
- **Status:** CONFIRMED BY CODE ANALYSIS (timing-dependent, hard to trigger in test)

### 2. [#60494] Crash in ReferenceCounter::CleanupBorrowersOnRefRemoved
- **File:** `repro_60494_refcount_crash.py`
- **Severity:** Critical (process crash, P0)
- **Status:** CONFIRMED BY CODE ANALYSIS (race condition, needs precise timing)

### 3. [#60251] Incorrect Error Message When Task Returns 1 value and num_returns > 1
- **File:** `repro_60251_num_returns.py`
- **Severity:** Medium (confusing error message)
- **Status:** BUG REPRODUCED

### 4. [#59914] Excessive number of small objects stored in memory
- **File:** `repro_59914_memory_growth.py`
- **Severity:** High (unbounded memory growth)
- **Status:** BUG REPRODUCED (~30MB growth over 10K calls)

### 5. [#59582] Inconsistent context manager behavior between ray.init modes
- **File:** `repro_59582_context_manager.py`
- **Severity:** Low (inconsistent API behavior)
- **Status:** CONFIRMED (but may be by-design)

## Running the Reproductions

```bash
# Requires: pip install ray psutil
python3 repro_60251_num_returns.py      # Instant reproduction
python3 repro_59914_memory_growth.py    # ~30 seconds
python3 repro_59582_context_manager.py  # Instant reproduction
python3 repro_60910_actor_hang_v3.py    # Timing-dependent
python3 repro_60494_refcount_crash.py   # Timing-dependent
```
