import logging
from typing import Any, Callable, Dict, Iterable, Iterator, Union

import numpy as np

logger = logging.getLogger(__name__)


def _get_slice_start(sl: slice) -> int:
    return 0 if sl.start is None else sl.start


def _get_slice_stop(sl: slice, length: int) -> int:
    return length if sl.stop is None else sl.stop


def _convert_ndarray_to_jax_array(
    ndarray: np.ndarray,
) -> "jax.Array":  # noqa: F821

    local_batch_size = ndarray.shape[0]

    # Validate rank

    # Ray Data partitions objects equally across the total number of hosts/workers
    # operating in the DataParallelTrainer (along the batch / 0-th dimension).
    # Since the input subset of records `ndarray` is exactly this host's 1D chunk,
    # we first create a JAX array matching this exact physical row-sharding.
    import jax
    from jax.sharding import Mesh, NamedSharding, PartitionSpec

    global_devices = jax.devices()
    host_count = jax.process_count()

    # 1. Physical Sharding (1D across the "data" dimension)
    # The full global_devices list is used to create a 1D mesh across all processes.
    physical_mesh = Mesh(np.array(global_devices), ("data",))
    physical_sharding = NamedSharding(physical_mesh, PartitionSpec("data"))

    # Global shape assumes each host gets the exact same local batch size.
    global_shape = (local_batch_size * host_count,) + ndarray.shape[1:]

    # Use index map to deterministically place local chunks onto correct devices
    device_indices_map = physical_sharding.addressable_devices_indices_map(global_shape)

    # when a tensor is wholly assigned to a single device instead of being partitioned, addressable_devices_indices_map returns slice(None, None, None) for that dimension instead of concrete indices.
    # _get_slice_start and _get_slice_stop are workarounds to handle this case.
    host_start_index = min(
        _get_slice_start(idx[0]) for idx in device_indices_map.values()
    )

    arrays = []
    for device, index in device_indices_map.items():
        # Translate the global row-sharding index to this host's local ndarray slice
        start = _get_slice_start(index[0])
        stop = _get_slice_stop(index[0], global_shape[0])
        local_slice = slice(
            start - host_start_index,
            stop - host_start_index,
            index[0].step,
        )
        local_index = (local_slice,) + index[1:]
        arrays.append(jax.device_put(ndarray[local_index], device))

    # Construct the globally aware 1D array
    physical_array = jax.make_array_from_single_device_arrays(
        global_shape, physical_sharding, arrays
    )

    return physical_array


def _convert_batch(
    ndarrays: Union[np.ndarray, Dict[str, np.ndarray]],
    transform: Callable[
        [Union["jax.Array", Dict[str, "jax.Array"]]], Any  # noqa: F821
    ] = None,
) -> Any:
    """Convert a NumPy ndarray batch to a globally sharded JAX Array batch.

    Args:
        ndarrays: A single NumPy ndarray or dictionary of NumPy ndarrays.
        transform: A flexible mapping function that converts the data parallel
            JAX arrays (or dictionary of arrays) to the final format.
            If None, the default 1D batch sharding is used.

    Returns:
         A globally sharded JAX Array (or dictionary of arrays) residing
         in TPU/GPU memory.
    """
    if isinstance(ndarrays, np.ndarray):
        jax_batch = _convert_ndarray_to_jax_array(ndarrays)
    else:
        jax_batch = {}
        for col_name, col_ndarray in ndarrays.items():
            try:
                jax_batch[col_name] = _convert_ndarray_to_jax_array(col_ndarray)
            except ValueError as e:
                raise ValueError(
                    f"JAX Array Conversion Error for column '{col_name}': \n{e}"
                )

    if transform:
        return transform(jax_batch)
    return jax_batch


def jax_sync_generator(
    batch_iterable: Iterable[Any],
    drop_last: bool,
    transform: Callable[
        [Union["jax.Array", Dict[str, "jax.Array"]]], Any  # noqa: F821
    ] = None,
    synchronize_batches: bool = False,
    synchronize_lookahead: int = 10,
) -> Iterator[Any]:
    """A generator that synchronizes and shards batches across JAX workers.

    This generator wraps a locally yielded batch iterable and ensures that all JAX
    workers within a multi-host training setup receive the exact same number of batches
    and identical batch shapes, which is required for JAX's SPMD execution.

    It performs the following synchronizations:
    1. Checks if all workers have a batch available. If only some workers are exhausted,
       it either drops the remaining batches (`drop_last=True`) or raises an error.
    2. Finds the globally minimum local batch size across all workers.
    3. Ensures the globally minimum batch size is evenly divisible by the number of local devices.
    4. Truncates all locally yielded batches to this globally consistent minimum size.
    5. Converts the truncated local NumPy arrays into globally sharded JAX Arrays.

    Args:
        batch_iterable: An iterable yielding local data batches (either a NumPy ndarray
            or a dictionary of NumPy ndarrays).
        drop_last: If True, drops mismatched or unevenly sized leftover batches. If False,
            raises a ValueError when uneven batches or uneven batch sizes are detected.
        transform: A flexible mapping function that converts the data parallel
            JAX arrays (or dictionary of arrays) to the final format.
            If None, the default 1D batch sharding is used.
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

            min_batch_size = local_batch_size
            if min_batch_size % num_local_devices != 0:
                if drop_last:
                    # Align the minimum batch size to be divisible by local devices
                    min_batch_size = min_batch_size - (
                        min_batch_size % num_local_devices
                    )
                else:
                    raise ValueError(
                        f"The local batch size ({min_batch_size}) must be evenly "
                        f"divisible by the number of local JAX devices "
                        f"({num_local_devices}) on this host. "
                        f"To safely truncate the batch to a divisible size, "
                        f"set `drop_last=True` in `iter_jax_batches()`."
                    )

            if min_batch_size == 0:
                continue

            if local_batch_size > min_batch_size:
                logger.info(
                    f"Dropping last {local_batch_size - min_batch_size} samples on process {jax.process_index()}"
                    f"from the batch to be evenly divisible by the number of local JAX devices."
                )
                if isinstance(batch, dict):
                    batch = {k: v[:min_batch_size] for k, v in batch.items()}
                else:
                    batch = batch[:min_batch_size]

            yield _convert_batch(batch, transform=transform)
        return

    # Multi-host synchronization with lookahead
    while True:
        local_batches = []
        local_infos = []
        for _ in range(synchronize_lookahead):
            try:
                batch = next(iterator)
                has_batch = True
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
                    return
                else:
                    raise ValueError(
                        "Uneven number of batches detected across JAX workers. "
                        "Some workers have exhausted their data while others have more. "
                        "To safely drop orphaned batches without hanging, "
                        "set `drop_last=True` in `iter_jax_batches()`."
                    )

            if min_batch_size % num_local_devices != 0:
                if drop_last:
                    # Align the minimum batch size to be divisible by local devices
                    min_batch_size = min_batch_size - (
                        min_batch_size % num_local_devices
                    )
                else:
                    raise ValueError(
                        f"The globally minimum batch size ({min_batch_size}) must be evenly "
                        f"divisible by the number of local JAX devices "
                        f"({num_local_devices}) on this host. "
                        f"To safely truncate the batch to a divisible size, "
                        f"set `drop_last=True` in `iter_jax_batches()`."
                    )

            if min_batch_size == 0:
                continue

            # Fail all workers if any worker has a different batch size and drop_last=False
            if max_batch_size > min_batch_size and not drop_last:
                raise ValueError(
                    "Uneven batch sizes detected across JAX workers. "
                    f"This host produced a batch of size {local_infos[2*i+1]}, "
                    f"but the globally minimum batch size is {min_batch_size}. "
                    "To safely truncate the batch to the minimum size, "
                    "set `drop_last=True` in `iter_jax_batches()`."
                )

            batch = local_batches[i]
            local_batch_size = local_infos[2 * i + 1]

            if local_batch_size > min_batch_size:
                # Truncate to the minimum batch size across all hosts
                logger.info(
                    f"Truncating batch from size {local_batch_size} to {min_batch_size}."
                )
                if isinstance(batch, dict):
                    batch = {k: v[:min_batch_size] for k, v in batch.items()}
                else:
                    batch = batch[:min_batch_size]

            yield _convert_batch(batch, transform=transform)
