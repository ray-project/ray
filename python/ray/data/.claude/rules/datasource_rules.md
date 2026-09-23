---
paths:
  - "python/ray/data/_internal/datasource/**/*.py"
  - "python/ray/data/_internal/datasource_v2/**/*.py"
---
- Datasource code runs with unpickling blocked, on the driver and in read tasks, and pickled-object Arrow columns from files are refused automatically. Do not add per-reader pickle checks. Never wrap external bytes in `ArrowPythonObjectType` and never call `ray.cloudpickle.loads` on them; use the stdlib `pickle` so the guard applies.
- A reader whose format can carry pickle (numpy object arrays, torch files) exposes an explicit opt-in, defaults it off, wraps only that call in `allow_unsafe_unpickling()`, and adds a reject test that plants a payload in real file bytes and proves it never runs.
