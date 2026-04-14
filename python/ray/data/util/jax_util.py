import logging
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Iterable,
    Iterator,
    List,
    Optional,
    Tuple,
    Union,
)

import numpy as np

if TYPE_CHECKING:
    import jax

logger = logging.getLogger(__name__)

_GLOBAL_MESH_1D_AXIS = "data"

NumpyBatch = Union[np.ndarray, Dict[str, np.ndarray]]
JaxBatch = Union["jax.Array", Dict[str, "jax.Array"]]
DTypeLikeSpec = Union["jax.typing.DTypeLike", Dict[str, "jax.typing.DTypeLike"]]
Scalar = Union[int, float, bool]
PaddingsSpec = Union[Scalar, Dict[str, Scalar]]


def _get_column_value(mapping_or_value: Any, key: str) -> Any:
    """Get the value for a specific column from a mapping or a single value."""
    if isinstance(mapping_or_value, dict):
        return mapping_or_value[key]
    return mapping_or_value


def _unwrap_single_column_value(mapping_or_value: Any, name: str) -> Any:
    """Unwrap a single value from a mapping if it's a dictionary."""
    if isinstance(mapping_or_value, dict):
        if len(mapping_or_value) != 1:
            raise ValueError(
                f"When constructing a single-tensor batch, only a single {name} "
                f"should be given, instead got: {mapping_or_value}"
            )
        return next(iter(mapping_or_value.values()))
    return mapping_or_value


def _create_sharding_1d(axis_name: str) -> "jax.sharding.Sharding":
    """Create a 1D JAX sharding, preferably using topology-aware mesh_utils."""
    import jax
    from jax.sharding import Mesh, NamedSharding, PartitionSpec

    devices = None
    try:
        from jax.experimental import mesh_utils

        # Attempt to create a topology-aware mesh (e.g. for TPU/GPU interconnects)
        devices = mesh_utils.create_device_mesh((jax.device_count(),))
    except Exception as e:
        logger.debug(f"Failed to use jax.experimental.mesh_utils: {e}")

    if devices is None:
        # Fallback to default device ordering if mesh_utils fails or is unavailable
        devices = np.array(jax.devices())

    physical_mesh = Mesh(devices, (axis_name,))
    return NamedSharding(physical_mesh, PartitionSpec(axis_name))


def _convert_ndarray_to_jax_array(
    ndarray: np.ndarray,
    sharding: "jax.sharding.Sharding",  # noqa: F821
    dtype: Optional["jax.typing.DTypeLike"] = None,
) -> "jax.Array":  # noqa: F821
    import jax

    local_batch_size = ndarray.shape[0]
    host_count = jax.process_count()

    # Global shape assumes each host gets the exact same local batch size.
    global_shape = (local_batch_size * host_count,) + ndarray.shape[1:]

    # Construct the globally aware 1D array from process-local data.
    # This automatically shards the local ndarray across the local devices
    # assigned to this process by the provided sharding.
    physical_array = jax.make_array_from_process_local_data(
        sharding, ndarray, global_shape
    )

    if dtype is not None:
        physical_array = physical_array.astype(dtype)

    return physical_array


def _convert_batch(
    ndarrays: NumpyBatch,
    sharding: "jax.sharding.Sharding",
    dtypes: Optional[DTypeLikeSpec] = None,
) -> JaxBatch:
    """Convert a NumPy ndarray batch to a globally sharded JAX Array batch.

    Args:
        ndarrays: A single NumPy ndarray or dictionary of NumPy ndarrays.
        sharding: The JAX sharding to use for the conversion.
        dtypes: A single JAX dtype or dictionary of JAX dtypes for the created arrays.

    Returns:
         A globally sharded JAX Array (or dictionary of arrays) residing
         in TPU/GPU memory.
    """
    if isinstance(ndarrays, np.ndarray):
        dtype = _unwrap_single_column_value(dtypes, "dtype")
        jax_batch = _convert_ndarray_to_jax_array(ndarrays, sharding, dtype=dtype)
    else:
        jax_batch = {}
        for col_name, col_ndarray in ndarrays.items():
            dtype = _get_column_value(dtypes, col_name)
            try:
                jax_batch[col_name] = _convert_ndarray_to_jax_array(
                    col_ndarray, sharding, dtype=dtype
                )
            except ValueError as e:
                raise ValueError(
                    f"JAX Array Conversion Error for column '{col_name}'"
                ) from e

    return jax_batch


def _get_batch_size(batch: NumpyBatch) -> int:
    """Get the batch size of a NumPy ndarray or dictionary of NumPy ndarrays."""
    if isinstance(batch, dict):
        # Use the first column to determine the batch size
        try:
            return len(next(iter(batch.values())))
        except StopIteration:
            return 0
    return len(batch)


def _pad_array(arr: np.ndarray, target_size: int, pad_value: Scalar) -> np.ndarray:
    """Pad a single array to target_size using pad_value."""
    current_size = len(arr)
    if current_size == target_size:
        return arr
    padding_shape = (target_size - current_size,) + arr.shape[1:]
    padding = np.full(padding_shape, pad_value, dtype=arr.dtype)
    return np.concatenate([arr, padding], axis=0)


def _dummy_array(arr: np.ndarray, target_size: int, pad_value: Scalar) -> np.ndarray:
    """Create a dummy array of target_size filled with pad_value."""
    shape = (target_size,) + arr.shape[1:]
    return np.full(shape, pad_value, dtype=arr.dtype)


def _pad_batch(
    batch: NumpyBatch,
    target_size: int,
    paddings: PaddingsSpec,
) -> NumpyBatch:
    """Pad a batch to target_size using paddings."""

    if isinstance(batch, dict):
        return {
            k: _pad_array(v, target_size, _get_column_value(paddings, k))
            for k, v in batch.items()
        }
    return _pad_array(
        batch,
        target_size,
        _unwrap_single_column_value(paddings, "padding"),
    )


def _create_dummy_batch(
    template_batch: NumpyBatch,
    target_size: int,
    paddings: PaddingsSpec,
) -> NumpyBatch:
    """Create a dummy batch of target_size filled with paddings."""

    if isinstance(template_batch, dict):
        return {
            k: _dummy_array(v, target_size, _get_column_value(paddings, k))
            for k, v in template_batch.items()
        }
    return _dummy_array(
        template_batch,
        target_size,
        _unwrap_single_column_value(paddings, "padding"),
    )


def _yield_batches_no_sync(
    iterator: Iterator[NumpyBatch],
    sharding: "jax.sharding.Sharding",
    num_local_devices: int,
    batch_size: int,
    paddings: Optional[PaddingsSpec],
    dtypes: Optional[DTypeLikeSpec] = None,
) -> Iterator[JaxBatch]:
    """Yield batches without multi-host synchronization."""
    for batch in iterator:
        local_batch_size = _get_batch_size(batch)

        if local_batch_size == 0:
            continue

        if paddings is not None:
            if local_batch_size < batch_size:
                batch = _pad_batch(batch, batch_size, paddings)
        elif local_batch_size % num_local_devices != 0:
            # Without padding, batch size must be divisible by num_local_devices
            raise ValueError(
                f"The local batch size ({local_batch_size}) must be evenly "
                f"divisible by the number of local JAX devices "
                f"({num_local_devices}) on this host. "
                f"To safely truncate or pad the batch, "
                f"set `drop_last=True` or provide a `paddings` in `iter_jax_batches()`."
            )

        yield _convert_batch(batch, sharding, dtypes=dtypes)


def _fetch_lookahead_batches(
    iterator: Iterator[NumpyBatch],
    lookahead: int,
) -> Tuple[List[Optional[NumpyBatch]], List[int], Optional[NumpyBatch]]:
    """Fetch a window of batches and prepare synchronization info."""
    local_batches = []
    local_infos = []
    template_batch: Optional[NumpyBatch] = None
    for _ in range(lookahead):
        try:
            batch = next(iterator)
            has_batch = True
            local_batch_size = _get_batch_size(batch)
            if template_batch is None:
                template_batch = batch
        except StopIteration:
            batch = None
            has_batch = False
            local_batch_size = 0

        local_batches.append(batch)
        local_infos.extend([int(has_batch), local_batch_size])
        if not has_batch:
            break
    return local_batches, local_infos, template_batch


def _yield_batches_with_sync(
    iterator: Iterator[NumpyBatch],
    sharding: "jax.sharding.Sharding",
    num_local_devices: int,
    drop_last: bool,
    batch_size: int,
    paddings: Optional[PaddingsSpec],
    synchronize_lookahead: int,
    dtypes: Optional[DTypeLikeSpec] = None,
) -> Iterator[JaxBatch]:
    """Yield batches with multi-host synchronization."""
    import jax.numpy as jnp
    from jax.experimental.multihost_utils import process_allgather

    template_batch: Optional[NumpyBatch] = None
    while True:
        local_batches, local_infos, window_template = _fetch_lookahead_batches(
            iterator, synchronize_lookahead
        )
        if template_batch is None:
            template_batch = window_template

        # Pad local_infos to 2 * synchronize_lookahead
        padding_needed = 2 * synchronize_lookahead - len(local_infos)
        if padding_needed > 0:
            local_infos.extend([0] * padding_needed)

        gathered = process_allgather(jnp.array(local_infos, dtype=jnp.int32))

        for i in range(synchronize_lookahead):
            h = gathered[:, 2 * i]
            s = gathered[:, 2 * i + 1]

            all_have_batch = bool(h.all())
            any_have_batch = bool(h.any())
            min_batch_size = int(s.min())
            max_batch_size = int(s.max())

            if not any_have_batch:
                return

            if not all_have_batch:
                # Some workers have exhausted their data while others have more.
                if drop_last:
                    # If drop_last=True, we stop as soon as any worker is exhausted.
                    return
                elif paddings is not None:
                    # If paddings is set, we continue until all workers are exhausted.
                    # Workers that are already exhausted will yield dummy batches.
                    pass
                else:
                    raise ValueError(
                        "Uneven number of batches detected across JAX workers. "
                        "To safely drop orphaned batches without hanging, "
                        "set `drop_last=True` or provide a `paddings` in `iter_jax_batches()`."
                    )

            if paddings is not None:
                batch = local_batches[i]
                if batch is None:
                    if template_batch is None:
                        raise ValueError(
                            "Cannot create dummy batches for synchronization because this "
                            "JAX host has not received any data batches to use as a "
                            "template. This usually happens if one JAX host's dataset "
                            "shard is completely empty while others have data. "
                            "Ensure that all JAX hosts have at least one batch of data, "
                            "or use `drop_last=True` to avoid yielding dummy batches."
                        )
                    batch = _create_dummy_batch(template_batch, batch_size, paddings)
                else:
                    local_batch_size = _get_batch_size(batch)
                    if local_batch_size < batch_size:
                        batch = _pad_batch(batch, batch_size, paddings)
                assert batch is not None
                yield _convert_batch(batch, sharding, dtypes=dtypes)
            else:
                if max_batch_size > min_batch_size:
                    raise ValueError(
                        "Uneven batch sizes detected across JAX workers. "
                        f"Host batch sizes range from {min_batch_size} to {max_batch_size}. "
                        "To handle uneven batch sizes, provide a `paddings` in `iter_jax_batches()`."
                    )

                if min_batch_size % num_local_devices != 0:
                    raise ValueError(
                        f"The globally minimum batch size ({min_batch_size}) must be evenly "
                        f"divisible by the number of local JAX devices "
                        f"({num_local_devices}) on this host. "
                        f"To safely truncate or pad the batch, "
                        f"set `drop_last=True` or provide a `paddings` in `iter_jax_batches()`."
                    )

                batch = local_batches[i]
                assert batch is not None
                yield _convert_batch(batch, sharding, dtypes=dtypes)


def jax_sync_generator(
    batch_iterable: Iterable[NumpyBatch],
    drop_last: bool,
    batch_size: int = 256,
    paddings: Optional[PaddingsSpec] = None,
    dtypes: Optional[DTypeLikeSpec] = None,
    synchronize_batches: bool = False,
    synchronize_lookahead: int = 10,
) -> Iterator[JaxBatch]:
    """A generator that synchronizes and shards batches across JAX workers.

    This generator wraps a locally yielded batch iterable and ensures that all JAX
    workers within a multi-host training setup receive the exact same number of batches
    and identical batch shapes, which is required for JAX's SPMD execution.

    Args:
        batch_iterable: An iterable yielding local data batches (either a NumPy ndarray
            or a dictionary of NumPy ndarrays).
        drop_last: Whether to drop partial or uneven batches.
        batch_size: The target batch size for each host.
        paddings: The value to use for padding uneven batches to `batch_size`.
            If a dictionary is provided, it must map column names to padding values.
            If None, padding is disabled.
        dtypes: A single JAX dtype or dictionary of JAX dtypes for the created arrays.
        synchronize_batches: Whether to synchronize batch shapes across all hosts.
            Setting this to False can improve performance if you guarantee that all
            hosts produce identical batch shapes and counts beforehand.
        synchronize_lookahead: The number of batches to look ahead and synchronize at
            once. Increasing this value reduces synchronization overhead but may
            increase memory usage as more batches are buffered locally.

    Yields:
        JaxBatch: Globally sharded batches.
    """
    import jax

    # Physical Sharding (1D across the _GLOBAL_MESH_1D_AXIS dimension)
    # The sharding is created once for the lifetime of this generator and reused
    # across all batches.
    sharding = _create_sharding_1d(_GLOBAL_MESH_1D_AXIS)

    num_local_devices = jax.local_device_count()
    iterator = iter(batch_iterable)

    if not synchronize_batches or jax.process_count() == 1:
        yield from _yield_batches_no_sync(
            iterator,
            sharding,
            num_local_devices,
            batch_size,
            paddings,
            dtypes=dtypes,
        )
    else:
        yield from _yield_batches_with_sync(
            iterator,
            sharding,
            num_local_devices,
            drop_last,
            batch_size,
            paddings,
            synchronize_lookahead,
            dtypes=dtypes,
        )
