---
paths:
  - "python/ray/data/_internal/datasource/**/*.py"
  - "python/ray/data/_internal/datasource_v2/**/*.py"
---
- Call `raise_on_pickle_object_columns(table)` on every `pa.Table` read from external data (parquet, Arrow IPC, a service or library returning Arrow) before yielding or materializing it, and add a reject test. deserializing from unknown sources is a RCE bug and has to be avoided.
