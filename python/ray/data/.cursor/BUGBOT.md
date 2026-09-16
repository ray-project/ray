# Bugbot Rules

## Rule: Pickled-object columns in Ray Data readers
- If a changed datasource read path produces a `pa.Table` from data outside the process (parquet, Arrow IPC, a service or library returning Arrow) without calling `raise_on_pickle_object_columns(table)` before any yield or materialization, post:

> ⚠️ Externally read Arrow data may carry `ray.data.arrow_pickled_object` columns, which unpickle on access and execute arbitrary code. Call `raise_on_pickle_object_columns(table)` right after the read and add a reject test proving a planted pickle payload never runs. Blocks built in-process from Python values are exempt.
