import logging
from typing import TYPE_CHECKING, Any, Dict, Iterable, Iterator, Optional, Union

import numpy as np

if TYPE_CHECKING:
    import jax

logger = logging.getLogger(__name__)

_GLOBAL_MESH_1D_AXIS = "data"


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

    return physical_array


def _convert_batch(
    ndarrays: Union[np.ndarray, Dict[str, np.ndarray]],
    sharding: "jax.sharding.Sharding",
) -> Any:
    """Convert a NumPy ndarray batch to a globally sharded JAX Array batch.

    Args:
        ndarrays: A single NumPy ndarray or dictionary of NumPy ndarrays.
        sharding: The JAX sharding to use for the conversion.

    Returns:
         A globally sharded JAX Array (or dictionary of arrays) residing
         in TPU/GPU memory.
    """
    if isinstance(ndarrays, np.ndarray):
        jax_batch = _convert_ndarray_to_jax_array(ndarrays, sharding)
    else:
        jax_batch = {}
        for col_name, col_ndarray in ndarrays.items():
            try:
                jax_batch[col_name] = _convert_ndarray_to_jax_array(
                    col_ndarray, sharding
                )
            except ValueError as e:
                raise ValueError(
                    f"JAX Array Conversion Error for column '{col_name}': \n{e}"
                )

    return jax_batch


def _pad_batch(batch: Any, target_size: int, pad_token_id: Any) -> Any:
    """Pad a batch to target_size using pad_token_id."""

    def pad_array(arr: np.ndarray) -> np.ndarray:
        current_size = len(arr)
        if current_size == target_size:
            return arr
        padding_size = target_size - current_size
        padding_shape = (padding_size,) + arr.shape[1:]
        padding = np.full(padding_shape, pad_token_id, dtype=arr.dtype)
        return np.concatenate([arr, padding], axis=0)

    if isinstance(batch, dict):
        return {k: pad_array(v) for k, v in batch.items()}
    return pad_array(batch)


def _create_dummy_batch(
    template_batch: Any, target_size: int, pad_token_id: Any
) -> Any:
    """Create a dummy batch of target_size filled with pad_token_id."""

    def dummy_array(arr: np.ndarray) -> np.ndarray:
        shape = (target_size,) + arr.shape[1:]
        return np.full(shape, pad_token_id, dtype=arr.dtype)

    if isinstance(template_batch, dict):
        return {k: dummy_array(v) for k, v in template_batch.items()}
    return dummy_array(template_batch)


def jax_sync_generator(
    batch_iterable: Iterable[Any],
    drop_last: bool,
    batch_size: Optional[int] = None,
    pad_token_id: Optional[Any] = None,
    synchronize_batches: bool = False,
    synchronize_lookahead: int = 10,
) -> Iterator[Any]:
    """A generator that synchronizes and shards batches across JAX workers.

    This generator wraps a locally yielded batch iterable and ensures that all JAX
    workers within a multi-host training setup receive the exact same number of batches
    and identical batch shapes, which is required for JAX's SPMD execution.

    Args:
        batch_iterable: An iterable yielding local data batches (either a NumPy ndarray
            or a dictionary of NumPy ndarrays).
        drop_last: Whether to drop partial or uneven batches.
        batch_size: The target batch size for each host.
        pad_token_id: The value to use for padding uneven batches to `batch_size`.
            If None, padding is disabled.
        synchronize_batches: Whether to synchronize batch shapes across all hosts.
            Setting this to False can improve performance if you guarantee that all
            hosts produce identical batch shapes and counts beforehand.
        synchronize_lookahead: The number of batches to look ahead and synchronize at
            once. Increasing this value reduces synchronization overhead but may
            increase memory usage as more batches are buffered locally.

    Yields:
        Any: Globally sharded batches.
    """
    import jax

    # Physical Sharding (1D across the _GLOBAL_MESH_1D_AXIS dimension)
    # The sharding is created once for the lifetime of this generator and reused
    # across all batches.
    sharding = _create_sharding_1d(_GLOBAL_MESH_1D_AXIS)

    num_local_devices = jax.local_device_count()
    iterator = iter(batch_iterable)

    if not synchronize_batches or jax.process_count() == 1:
        for batch in iterator:
            if isinstance(batch, dict):
                # Use the first column to determine the batch size
                try:
                    local_batch_size = len(next(iter(batch.values())))
                except StopIteration:
                    local_batch_size = 0
            else:
                local_batch_size = len(batch)

            if local_batch_size == 0:
                continue

            if pad_token_id is not None:
                assert batch_size is not None
                if local_batch_size < batch_size:
                    batch = _pad_batch(batch, batch_size, pad_token_id)
            elif local_batch_size % num_local_devices != 0:
                # Without padding, batch size must be divisible by num_local_devices
                raise ValueError(
                    f"The local batch size ({local_batch_size}) must be evenly "
                    f"divisible by the number of local JAX devices "
                    f"({num_local_devices}) on this host. "
                    f"To safely truncate or pad the batch, "
                    f"set `drop_last=True` or provide a `pad_token_id` in `iter_jax_batches()`."
                )

            yield _convert_batch(batch, sharding)
        return

    # Multi-host synchronization with lookahead
    template_batch = None
    while True:
        local_batches = []
        local_infos = []
        for _ in range(synchronize_lookahead):
            try:
                batch = next(iterator)
                has_batch = True
                if template_batch is None:
                    template_batch = batch
                if isinstance(batch, dict):
                    # Use the first column to determine the batch size
                    try:
                        local_batch_size = len(next(iter(batch.values())))
                    except StopIteration:
                        local_batch_size = 0
                else:
                    local_batch_size = len(batch)
            except StopIteration:
                batch = None
                has_batch = False
                local_batch_size = 0

            local_batches.append(batch)
            local_infos.extend([int(has_batch), local_batch_size])
            if not has_batch:
                break

        import jax.numpy as jnp
        from jax.experimental.multihost_utils import process_allgather

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
                elif pad_token_id is not None:
                    # If pad_token_id is set, we continue until all workers are exhausted.
                    # Workers that are already exhausted will yield dummy batches.
                    pass
                else:
                    raise ValueError(
                        "Uneven number of batches detected across JAX workers. "
                        "To safely drop orphaned batches without hanging, "
                        "set `drop_last=True` or provide a `pad_token_id` in `iter_jax_batches()`."
                    )

            if pad_token_id is not None:
                assert batch_size is not None
                batch = local_batches[i]
                if batch is None:
                    # This worker is exhausted, but others are not. Yield dummy batch.
                    if template_batch is None:
                        # If template_batch is None, it means THIS host has never received
                        # any data batch, but another host has (any_have_batch is True).
                        # We cannot create a dummy batch without knowing the structure
                        # (keys, shapes of non-batch dims) of the data.
                        raise ValueError(
                            "Cannot create dummy batches for synchronization because this "
                            "JAX host has not received any data batches to use as a "
                            "template. This usually happens if one JAX host's dataset "
                            "shard is completely empty while others have data. "
                            "Ensure that all JAX hosts have at least one batch of data, "
                            "or use `drop_last=True` to avoid yielding dummy batches."
                        )
                    batch = _create_dummy_batch(
                        template_batch, batch_size, pad_token_id
                    )
                else:
                    local_batch_size = local_infos[2 * i + 1]
                    if local_batch_size < batch_size:
                        batch = _pad_batch(batch, batch_size, pad_token_id)
                yield _convert_batch(batch, sharding)
            else:
                if max_batch_size > min_batch_size:
                    raise ValueError(
                        "Uneven batch sizes detected across JAX workers. "
                        f"Host batch sizes range from {min_batch_size} to {max_batch_size}. "
                        "To handle uneven batch sizes, provide a `pad_token_id` in `iter_jax_batches()`."
                    )

                if min_batch_size % num_local_devices != 0:
                    raise ValueError(
                        f"The globally minimum batch size ({min_batch_size}) must be evenly "
                        f"divisible by the number of local JAX devices "
                        f"({num_local_devices}) on this host. "
                        f"To safely truncate or pad the batch, "
                        f"set `drop_last=True` or provide a `pad_token_id` in `iter_jax_batches()`."
                    )

                batch = local_batches[i]
                yield _convert_batch(batch, sharding)
