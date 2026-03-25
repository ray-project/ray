import logging
from typing import Any, Dict, Iterable, Iterator, Union

import numpy as np

logger = logging.getLogger(__name__)


def _get_slice_start(sl: slice) -> int:
    return 0 if sl.start is None else sl.start


def _get_slice_stop(sl: slice, length: int) -> int:
    return length if sl.stop is None else sl.stop


def _convert_ndarray_to_jax_tensor(
    ndarray: np.ndarray,
) -> "jax.Array":  # noqa: F821
    """Convert a NumPy ndarray into a JAX Array with 1D batch-parallel sharding."""

    local_batch_size = ndarray.shape[0]

    import jax
    from jax.sharding import Mesh, NamedSharding, PartitionSpec

    global_devices = jax.devices()
    host_count = jax.process_count()

    physical_mesh = Mesh(np.array(global_devices), ("batch",))
    physical_sharding = NamedSharding(physical_mesh, PartitionSpec("batch"))

    global_shape = (local_batch_size * host_count,) + ndarray.shape[1:]

    device_indices_map = physical_sharding.addressable_devices_indices_map(global_shape)

    # addressable_devices_indices_map returns slice(None, None, None) when a
    # tensor is wholly assigned to a single device; the helpers normalise that.
    host_start_index = min(
        _get_slice_start(idx[0]) for idx in device_indices_map.values()
    )

    arrays = []
    for device, index in device_indices_map.items():
        start = _get_slice_start(index[0])
        stop = _get_slice_stop(index[0], global_shape[0])
        local_slice = slice(
            start - host_start_index,
            stop - host_start_index,
            index[0].step,
        )
        local_index = (local_slice,) + index[1:]
        arrays.append(jax.device_put(ndarray[local_index], device))

    return jax.make_array_from_single_device_arrays(
        global_shape, physical_sharding, arrays
    )


def _convert_ndarray_batch_to_jax_tensor_batch(
    ndarrays: Union[np.ndarray, Dict[str, np.ndarray]],
) -> Union["jax.Array", Dict[str, "jax.Array"]]:  # noqa: F821
    """Convert a NumPy ndarray batch to a JAX Array batch with 1D batch sharding.

    Args:
        ndarrays: A single NumPy ndarray or dictionary of NumPy ndarrays.

    Returns:
         A JAX Array (or dictionary of arrays) sharded along the batch
         dimension across all devices.
    """
    if isinstance(ndarrays, np.ndarray):
        return _convert_ndarray_to_jax_tensor(ndarrays)

    jax_batch = {}
    for col_name, col_ndarray in ndarrays.items():
        try:
            jax_batch[col_name] = _convert_ndarray_to_jax_tensor(col_ndarray)
        except ValueError as e:
            raise ValueError(f"JAX Sharding Error for column '{col_name}': \n{e}")

    return jax_batch


def reshard_jax_batch(
    batch: Union["jax.Array", Dict[str, "jax.Array"]],  # noqa: F821
    named_sharding: "jax.sharding.NamedSharding",  # noqa: F821
) -> Union["jax.Array", Dict[str, "jax.Array"]]:  # noqa: F821
    """Reshard a JAX Array batch to match a target ``NamedSharding`` layout.

    Use this after :meth:`~ray.data.Dataset.iter_jax_batches` to move data
    from its default 1-D batch-parallel sharding into an arbitrary N-D mesh
    layout (e.g. 2-D data + model parallelism).  JAX handles the necessary
    device-to-device communication automatically.

    Example:

        .. code-block:: python

            from ray.data.util.jax_util import reshard_jax_batch

            for batch in ds.iter_jax_batches(batch_size=16):
                batch = reshard_jax_batch(batch, named_sharding)
                # batch now has the requested NamedSharding layout
                ...

    Args:
        batch: A JAX Array or a dictionary of JAX Arrays produced by
            ``iter_jax_batches``.
        named_sharding: The target ``jax.sharding.NamedSharding`` that
            defines the mesh and partition layout to reshard into.

    Returns:
        The resharded JAX Array (or dictionary of arrays).

    Raises:
        ValueError: If the ``PartitionSpec`` has more dimensions than any
            array in the batch.
    """
    import jax

    if isinstance(batch, dict):
        resharded = {}
        for col_name, arr in batch.items():
            try:
                _validate_sharding_rank(arr, named_sharding)
                resharded[col_name] = jax.device_put(arr, named_sharding)
            except ValueError as e:
                raise ValueError(
                    f"JAX Sharding Error for column '{col_name}': \n{e}"
                )
        return resharded

    _validate_sharding_rank(batch, named_sharding)
    return jax.device_put(batch, named_sharding)


def _validate_sharding_rank(
    arr: "jax.Array",  # noqa: F821
    named_sharding: "jax.sharding.NamedSharding",  # noqa: F821
) -> None:
    partition_spec = named_sharding.spec
    if len(partition_spec) > len(arr.shape):
        raise ValueError(
            f"PartitionSpec {partition_spec} defines sharding for "
            f"{len(partition_spec)} dimensions, but the input tensor only has "
            f"{len(arr.shape)} dimensions (shape: {arr.shape})."
        )


def jax_sync_generator(
    batch_iterable: Iterable[Any],
    drop_last: bool,
    synchronize_batches: bool = True,
    synchronize_lookahead: int = 10,
) -> Iterator[Union["jax.Array", Dict[str, "jax.Array"]]]:  # noqa: F821
    """A generator that synchronizes and shards batches across JAX workers.

    This generator wraps a locally yielded batch iterable and ensures that all JAX
    workers within a multi-host training setup receive the exact same number of batches
    and identical batch shapes, which is required for JAX's SPMD execution.

    It performs the following synchronizations:
    1. Checks if all workers have a batch available. If only some workers are exhausted,
       it either drops the remaining batches (``drop_last=True``) or raises an error.
    2. Finds the globally minimum local batch size across all workers.
    3. Ensures the globally minimum batch size is evenly divisible by the number of
       local devices.
    4. Truncates all locally yielded batches to this globally consistent minimum size.
    5. Converts the truncated local NumPy arrays into globally sharded JAX Arrays
       with 1-D batch-parallel sharding.

    To reshard the yielded arrays into a custom ``NamedSharding`` layout, use
    :func:`reshard_jax_batch` on each batch after iteration.

    Args:
        batch_iterable: An iterable yielding local data batches (either a NumPy ndarray
            or a dictionary of NumPy ndarrays).
        drop_last: If True, drops mismatched or unevenly sized leftover batches. If False,
            raises a ValueError when uneven batches or uneven batch sizes are detected.
        synchronize_batches: Whether to synchronize batch shapes across all hosts.
            Setting this to False can improve performance if you guarantee that all
            hosts produce identical batch shapes and counts beforehand.
        synchronize_lookahead: The number of batches to look ahead and synchronize at
            once. Increasing this value reduces synchronization overhead but may
            increase memory usage as more batches are buffered locally.

    Yields:
        (jax.Array, Dict[str, jax.Array]): A globally sharded JAX Array or a
            dictionary of JAX Arrays with 1-D batch sharding.
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
                    f"Dropping last {local_batch_size - min_batch_size} samples "
                    f"from the batch to be evenly divisible by the number of local JAX devices."
                )
                if isinstance(batch, dict):
                    batch = {k: v[:min_batch_size] for k, v in batch.items()}
                else:
                    batch = batch[:min_batch_size]

            yield _convert_ndarray_batch_to_jax_tensor_batch(batch)
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

            yield _convert_ndarray_batch_to_jax_tensor_batch(batch)
