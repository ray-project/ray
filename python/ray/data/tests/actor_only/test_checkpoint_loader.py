"""The checkpoint loader's internal dataset runs on the classic backend when the
actor-only backend is enabled.

The loader pipelines (``read_parquet -> [preprocess] -> repartition(1)`` for
``id_column``; plus ``groupby``/``sort`` for ``generated_id_column``) plan
AllToAll operators, which the actor-only executor rejects. The loader is an
internal bookkeeping dataset, so it must execute on the classic backend
regardless of the global flag.
"""
import os

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pyarrow.fs import LocalFileSystem

import ray
from ray.data import DataContext
from ray.data._internal.execution.execution_flags import (
    actor_only_backend_enabled,
)
from ray.data.checkpoint import CheckpointConfig
from ray.data.checkpoint.checkpoint_filter import IdColumnCheckpointManager

_AO_ENV = "RAY_DATA_ACTOR_ONLY_BACKEND"


def test_context_override_scopes_backend_selection(monkeypatch):
    """The per-dataset override wins over the env var for that context only,
    compute promotion included, and never touches the process env."""
    from ray.data._internal.compute import (
        TaskPoolStrategy,
        maybe_promote_compute_strategy,
    )

    monkeypatch.setenv(_AO_ENV, "1")
    ctx = DataContext.get_current().copy()
    ctx._actor_only_backend_override = False

    assert not actor_only_backend_enabled(ctx)
    task_pool = TaskPoolStrategy()
    assert maybe_promote_compute_strategy(task_pool, ctx) is task_pool

    assert actor_only_backend_enabled()
    assert actor_only_backend_enabled(DataContext.get_current())
    assert not isinstance(
        maybe_promote_compute_strategy(TaskPoolStrategy()), TaskPoolStrategy
    )
    assert os.environ[_AO_ENV] == "1"


def test_id_column_loader_runs_under_actor_only_backend(
    ray_start_2_cpus_shared, tmp_path
):
    """``load_checkpoint`` must succeed with the actor-only backend enabled: its
    internal dataset repartitions (an AllToAll), which the actor-only executor
    rejects, so the loader must run classic."""
    assert actor_only_backend_enabled()
    checkpoint_dir = tmp_path / "checkpoint"
    checkpoint_dir.mkdir()
    ids = [3, 1, 2, 5, 4]
    pq.write_table(
        pa.table({"id": pa.array(ids, type=pa.int64())}),
        str(checkpoint_dir / "checkpoint.parquet"),
    )

    manager = IdColumnCheckpointManager(
        checkpoint_config=CheckpointConfig(
            id_column="id", checkpoint_path=str(checkpoint_dir)
        ),
        data_context=DataContext.get_current(),
    )
    ids_ref, size_bytes = manager.load_checkpoint()
    assert ids_ref is not None

    # pyrefly: ignore[no-matching-overload]
    loaded = np.asarray(ray.get(ids_ref))
    assert sorted(loaded.tolist()) == sorted(ids)
    assert size_bytes == loaded.nbytes
    # The loader's backend override is scoped to its own dataset.
    assert actor_only_backend_enabled()
    assert os.environ[_AO_ENV] == "1"


def test_generated_id_loader_runs_under_actor_only_backend(
    ray_start_2_cpus_shared, tmp_path
):
    """The generated-id loader adds groupby (hash shuffle) and sort on top of the
    repartition. It must also run classic under the actor-only backend."""
    from ray.data.checkpoint.checkpoint_filter import (
        GeneratedIdColumnCheckpointManager,
    )
    from ray.data.checkpoint.generated_id import (
        FILE_NAME_FIELD,
        FRAGMENT_FIELD,
        GENERATED_ID_COLUMN_FIELDS,
        NUM_FRAGMENTS_FIELD,
        NUM_ROWS_FIELD,
        PATH_PREFIX_FIELD,
        ROW_ID_FIELD,
    )

    assert actor_only_backend_enabled()
    checkpoint_dir = tmp_path / "checkpoint"
    checkpoint_dir.mkdir()

    rows = [
        {
            PATH_PREFIX_FIELD: "s3://bucket/data",
            FILE_NAME_FIELD: "f1.parquet",
            FRAGMENT_FIELD: 0,
            NUM_FRAGMENTS_FIELD: 1,
            NUM_ROWS_FIELD: 2,
            ROW_ID_FIELD: row_id,
        }
        for row_id in range(2)
    ]
    id_arrays = []
    for field_name in GENERATED_ID_COLUMN_FIELDS:
        values = [row[field_name] for row in rows]
        if field_name in (PATH_PREFIX_FIELD, FILE_NAME_FIELD):
            id_arrays.append(pa.array(values, type=pa.string()).dictionary_encode())
        elif field_name == ROW_ID_FIELD:
            id_arrays.append(pa.array(values, type=pa.int32()))
        else:
            id_arrays.append(pa.array(values, type=pa.int32()).dictionary_encode())
    struct = pa.StructArray.from_arrays(
        id_arrays,
        fields=[
            pa.field(name, dtype, nullable=False)
            for name, dtype in GENERATED_ID_COLUMN_FIELDS.items()
        ],
    )
    pq.write_table(
        pa.table({ROW_ID_FIELD: struct}),
        str(checkpoint_dir / "checkpoint.parquet"),
    )

    manager = GeneratedIdColumnCheckpointManager(
        checkpoint_config=CheckpointConfig(
            checkpoint_path=str(checkpoint_dir),
            generated_id_column=ROW_ID_FIELD,
            override_filesystem=LocalFileSystem(),
        ),
        data_context=DataContext.get_current(),
    )
    checkpoint = manager.load_generated_id_checkpoint()

    # One fully-checkpointed file, and no partly done row groups.
    assert len(checkpoint.done_unit_ids) == 1
    assert checkpoint.partial_masks.num_rows == 0
    assert actor_only_backend_enabled()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
