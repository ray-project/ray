# `ray/data/_internal/` Directory Structure

## Overview

The `_internal/` directory is organized into **15 semantic domains**, each owning a
distinct area of responsibility in Ray Data's implementation.

```
_internal/
├── autoscaling/            # Worker pool and cluster autoscaling
│   ├── actor/              #   Actor pool scaling (per-operator)
│   ├── cluster/            #   Cluster-level autoscaling coordination
│   └── autoscaling_requester.py
│
├── blocks/                 # Block formats and data type extensions
│   ├── arrow_block.py      #   Arrow block implementation
│   ├── pandas_block.py     #   Pandas block implementation
│   ├── table_block.py      #   Base table block
│   ├── block_builder.py    #   Block building utilities
│   ├── numpy_support.py    #   NumPy interop
│   ├── output_buffer.py    #   Block output buffering
│   ├── pyarrow_version_check.py  # PyArrow install/version checks
│   ├── pyarrow_ext_utils.py      # PyArrow extension type helpers
│   ├── row.py              #   Row display helpers (row_str, row_repr)
│   ├── size_estimator.py   #   Serialization size sampling
│   ├── arrow_ops/          #   Arrow/Polars transform helpers
│   ├── tensor_extensions/  #   Arrow & Pandas tensor type extensions
│   └── object_extensions/  #   Arrow & Pandas Python object extensions
│
├── bundle_queue/           # RefBundle queue implementations
│   ├── base.py             #   Abstract base queue
│   ├── fifo.py             #   FIFO queue
│   ├── hash_link.py        #   Hash-linked queue
│   ├── reordering.py       #   Reordering queue
│   └── thread_safe.py      #   Thread-safe wrapper
│
├── execution/              # Runtime execution engine
│   ├── streaming_executor.py       # Main streaming executor
│   ├── streaming_executor_state.py # Executor state machine
│   ├── resource_manager.py         # Resource allocation
│   ├── dataset_state.py            # Dataset execution state
│   ├── legacy_compat.py            # Legacy API compatibility
│   ├── ranker.py                   # Operator ranking
│   ├── util.py                     # Execution utilities
│   ├── backpressure_policy/        # Backpressure strategies
│   ├── callbacks/                  # Execution lifecycle callbacks
│   └── interfaces/                 # Core interfaces
│       ├── execution_options.py    #   ExecutionOptions, ExecutionResources
│       ├── executor.py             #   Executor base class
│       ├── ref_bundle.py           #   RefBundle (unit of data exchange)
│       ├── task_context.py         #   TaskContext passed to UDFs
│       └── transform_fn.py        #   Transform function types
│
├── io/                     # Data I/O (sources and sinks)
│   ├── datasource/         #   Read connectors (one file per format)
│   │   ├── parquet_datasource.py
│   │   ├── csv_datasource.py
│   │   ├── json_datasource.py
│   │   ├── ...             #   ~30 datasource implementations
│   │   └── lance_utils.py  #   Shared utilities
│   ├── datasink/           #   Write connectors (one file per format)
│   │   ├── parquet_datasink.py
│   │   ├── csv_datasink.py
│   │   ├── ...             #   ~15 datasink implementations
│   │   └── webdataset_datasink.py
│   └── datasource_v2/      #   Next-gen scanner/reader framework
│       ├── datasource_v2.py
│       ├── listing/         #   File listing and indexing
│       ├── partitioners/    #   Input partitioning strategies
│       ├── readers/         #   Data readers
│       └── scanners/        #   File scanners
│
├── iteration/              # Dataset iteration and batching
│   ├── iterator_impl.py    #   Core iterator implementation
│   ├── stream_split_iterator.py  # Split-stream iteration
│   ├── equalize.py          #   Equalize row counts across splits
│   ├── batcher.py           #   Block batching logic
│   ├── torch_iterable_dataset.py # PyTorch IterableDataset adapter
│   ├── tensorflow_utils.py  #   NumPy/Arrow → TensorFlow conversion
│   └── batching/            #   Batch assembly pipeline
│       ├── block_batching.py
│       ├── iter_batches.py
│       └── interfaces.py
│
├── logical/                # Logical plan and optimization
│   ├── optimizers.py       #   Logical & physical optimizer entry points
│   ├── ruleset.py          #   Optimizer ruleset registry
│   ├── interfaces/         #   LogicalOperator, LogicalPlan, etc.
│   ├── operators/          #   Logical operator definitions
│   │   ├── map_operator.py
│   │   ├── read_operator.py
│   │   ├── write_operator.py
│   │   ├── all_to_all_operator.py
│   │   ├── join_operator.py
│   │   └── ...
│   └── rules/              #   Optimizer rules
│       ├── operator_fusion.py
│       ├── predicate_pushdown.py
│       ├── projection_pushdown.py
│       ├── limit_pushdown.py
│       └── ...
│
├── node_trackers/          # Actor/node location tracking
│   └── actor_location.py
│
├── observability/          # Stats, metrics, logging, diagnostics
│   ├── stats.py            #   DatasetStats collection
│   ├── op_runtime_metrics.py  # Per-operator runtime metrics
│   ├── common.py           #   Shared metrics types (histograms, etc.)
│   ├── logging.py          #   Ray Data logging configuration
│   ├── memory_tracing.py   #   Memory usage tracing
│   ├── dataset_repr.py     #   Dataset string representation
│   ├── average_calculator.py
│   ├── metadata_exporter.py
│   ├── operator_event_exporter.py
│   ├── operator_schema_exporter.py
│   ├── progress/           #   Execution progress reporting
│   │   ├── base_progress.py
│   │   ├── tqdm_progress.py
│   │   ├── rich_progress.py
│   │   └── logging_progress.py
│   └── diagnostics/        #   Issue detection
│       ├── issue_detector.py
│       ├── issue_detector_manager.py
│       ├── issue_detector_configuration.py
│       └── detectors/
│           ├── hanging_detector.py
│           ├── high_memory_detector.py
│           └── hash_shuffle_detector.py
│
├── physical/               # Physical operator implementations
│   ├── physical_operator.py       # PhysicalOperator base class
│   ├── base_physical_operator.py  # Mixin for internal queues
│   ├── map_operator.py            # MapOperator (core transform)
│   ├── map_transformer.py         # Transform function wrappers
│   ├── task_pool_map_operator.py  # Task-pool execution
│   ├── actor_pool_map_operator.py # Actor-pool execution
│   ├── heapdict.py                # Vendored heap-based priority dict
│   ├── hash_shuffle.py            # Hash shuffle operator
│   ├── hash_aggregate.py          # Hash aggregate operator
│   ├── join.py                    # Join operator
│   ├── limit_operator.py          # Limit operator
│   ├── output_splitter.py         # Output splitting for streaming_split
│   ├── split.py                   # Block splitting by indices/row counts
│   ├── union_operator.py          # Union operator
│   ├── zip_operator.py            # Zip operator
│   ├── input_data_buffer.py       # Leaf operator for materialized data
│   ├── aggregate_num_rows.py      # Row count aggregation
│   ├── sub_progress.py            # Sub-operator progress tracking
│   └── gpu_shuffle/               # GPU-accelerated shuffle
│       ├── hash_shuffle.py
│       └── rapidsmpf_backend.py
│
├── planner/                # Logical-to-physical plan translation
│   ├── planner.py          #   Main planner
│   ├── plan.py             #   ExecutionPlan
│   ├── plan_read_op.py     #   Plan: read operators
│   ├── plan_write_op.py    #   Plan: write operators
│   ├── plan_udf_map_op.py  #   Plan: UDF map operators
│   ├── plan_all_to_all_op.py  # Plan: all-to-all operators
│   ├── plan_download_op.py #   Plan: download operators
│   ├── random_shuffle.py   #   Shuffle planning
│   ├── repartition.py      #   Repartition planning
│   ├── sort.py             #   Sort planning
│   ├── randomize_blocks.py #   Block randomization
│   ├── aggregate.py        #   Aggregation planning
│   ├── random_config.py    #   Random seed configuration
│   ├── streaming_repartition.py  # Streaming repartition bundler
│   ├── checkpoint/         #   Checkpoint read/write planning
│   ├── exchange/           #   Exchange task specs (shuffle, sort, aggregate)
│   └── plan_expression/    #   Expression evaluation planning
│
├── public_api/             # Public-facing classes (canonical location)
│   ├── compute.py          #   ComputeStrategy, TaskPoolStrategy, ActorPoolStrategy
│   └── savemode.py         #   SaveMode enum
│
└── utils/                  # Shared cross-domain utilities
    ├── util.py             #   General helpers (autodetect parallelism, etc.)
    ├── remote_fn.py        #   Cached remote function helpers
    ├── compute.py          #   [shim] → public_api/compute.py + get_compute()
    └── savemode.py         #   [shim] → public_api/savemode.py
```

## Design Principles

1. **One domain per directory** — each top-level folder owns a single area of concern.
2. **Absolute imports across domains** — e.g., `from ray.data._internal.physical.map_operator import MapOperator`.
3. **Public API classes live in `public_api/`** — old locations remain as deprecation shims.
4. **`[shim]` files** forward imports with a `DeprecationWarning` so external code that
   references old paths continues to work during migration.
