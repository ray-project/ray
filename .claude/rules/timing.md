---
paths:
  - "src/**/*.{cc,h}"
---

- Excluding the Clock implementation and benchmarks, read the current time only through an injected `ray::ClockInterface` (`src/ray/util/clock.h`) so time can be faked and unit tested deterministically. Do not call `absl::Now()`, `std::chrono::{system,steady,high_resolution}_clock::now()`, or other similar functions.
- Inject `ClockInterface &clock_` via the constructor. Pass a `Clock` from the production entrypoint (e.g. `raylet/main.cc`) and a `FakeClock` in tests. Declare the concrete clock member before any object that holds a `ClockInterface &` so it is constructed first and outlives the holder.
- Use `clock_.Now()` / `clock_.NowUnixMillis()` for wall-clock timestamps and deadlines, and `clock_.SteadyNow()` for elapsed/duration measurements
- In unit tests use `FakeClock::AdvanceTime`/`SetTime` instead of `sleep` or wall-clock time. Ensure that the tests are written to be fast and deterministic.
