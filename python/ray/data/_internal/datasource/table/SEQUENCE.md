# Table-datasink write sequence

This is the canonical sequence diagram for the table-format write
abstraction (`TableDatasink` + `TableAdapter`). An ASCII version is
reproduced inline in the `adapter.py` and `table_datasink.py` module
docstrings; this file is the higher-fidelity Mermaid source for the docs
site and GitHub-rendered Markdown.

The diagram describes one driver-orchestrated write. Worker steps run
inside Ray write tasks, one task per data partition, in parallel.

```mermaid
sequenceDiagram
    autonumber
    participant Ray as Ray Data engine
    participant Sink as TableDatasink<br/>(framework, generic)
    participant Adapter as TableAdapter<br/>(format-specific)
    participant Workers as Ray write tasks<br/>(N workers)

    Note over Sink: Driver: pre-write setup
    Ray->>Sink: on_write_start(schema)
    Sink->>Adapter: preflight(mode, partition_cols, declared_schema)
    Sink->>Adapter: on_write_start(schema_from_first_bundle)

    Note over Workers: One task per partition, in parallel
    par per task
        Ray->>Sink: write(blocks, ctx)
        Sink->>Adapter: start_task(ctx)
        loop per Arrow table in blocks
            Sink->>Adapter: write_block(arrow_table)
            Adapter-->>Sink: (file_actions, emitted_schema, upsert_keys?)
        end
        Sink->>Adapter: finalize_task()
        Sink->>Adapter: task_metadata()
        Sink-->>Workers: TableWriteTaskResult
    end

    Note over Sink: Driver: aggregate + commit
    Ray->>Sink: on_write_complete(results)
    Sink->>Adapter: gather_task_metadata(all_task_metadata)
    Sink->>Adapter: reconcile_schema(unified_schema)

    alt mode == APPEND
        Sink->>Adapter: commit_append(file_actions, unified_schema)
    else mode == OVERWRITE
        Sink->>Adapter: build_overwrite_predicate(overwrite_filter)
        Adapter-->>Sink: delete_predicate
        Sink->>Adapter: commit_overwrite(file_actions, unified_schema, delete_predicate)
    else mode == UPSERT (adapter conforms to SupportsUpserts)
        Sink->>Adapter: build_upsert_predicate(upsert_keys, join_cols)
        Adapter-->>Sink: delete_predicate
        Sink->>Adapter: commit_upsert(file_actions, unified_schema, delete_predicate)
    end

    Note over Sink: On failure (any step above)
    Ray->>Sink: on_write_failed(error)
    Sink->>Adapter: on_failure(orphan_paths)
```

## Step glossary

| Step | Owner | Purpose |
|---|---|---|
| `preflight` | adapter | Load table from catalog/log; validate mode legality + schema/partitions. |
| `on_write_start` | adapter | Optional pre-write hook fed the first bundle's schema. Iceberg evolves schema here; Delta no-ops and evolves at commit. |
| `start_task` | adapter | Per-task setup (e.g. open a per-task file writer). |
| `write_block` | adapter | Persist one Arrow table as object-store files. Returns per-file actions, the emitted schema, and the upsert-key projection (UPSERT mode only). |
| `finalize_task` | adapter | Flush any per-task buffer; return extra file actions and schemas. |
| `task_metadata` | adapter | Free-form per-task state the driver needs (e.g. Delta's per-write UUID). |
| `gather_task_metadata` | adapter | Driver-side merge of per-task metadata across workers. |
| `reconcile_schema` | adapter | Apply the worker-unified schema to the table state. |
| `build_overwrite_predicate` | adapter | Translate a user `overwrite_filter` into the format's predicate type. |
| `build_upsert_predicate` | adapter (`SupportsUpserts`) | Translate upsert-key table into the format's predicate type. |
| `commit_append` / `commit_overwrite` / `commit_upsert` | adapter | One atomic transaction per mode. |
| `on_failure` | adapter | Best-effort cleanup of files written by failed tasks. Never destroys committed data. |
