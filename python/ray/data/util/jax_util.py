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
    named_sharding: "jax.sharding.NamedSharding" = None,  # noqa: F821
) -> "jax.Array":  # noqa: F821

    local_batch_size = ndarray.shape[0]

    # Validate rank
    if named_sharding:
        partition_spec = named_sharding.spec
        if len(partition_spec) > len(ndarray.shape):
            raise ValueError(
                f"PartitionSpec {partition_spec} defines sharding for {len(partition_spec)} "
                f"dimensions, but the input tensor only has {len(ndarray.shape)} dimensions "
                f"(shape: {ndarray.shape})."
            )

    # Ray Data partitions objects equally across the total number of hosts/workers
    # operating in the DataParallelTrainer (along the batch / 0-th dimension).
    # Since the input subset of records `ndarray` is exactly this host's 1D chunk,
    # we first create a JAX array matching this exact physical row-sharding.
    import jax
    from jax.sharding import Mesh, NamedSharding, PartitionSpec

    global_devices = jax.devices()
    host_count = jax.process_count()

    # 1. Physical Sharding (1D across the batch dimension)
    # The full global_devices list is used to create a 1D mesh across all processes.
    physical_mesh = Mesh(np.array(global_devices), ("batch",))
    physical_sharding = NamedSharding(physical_mesh, PartitionSpec("batch"))

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

    if named_sharding:
        # 2. Reshard to the user's exact requested target sharding (e.g. 2D / 3D)
        # JAX will automatically manage the NCCL all-to-all communications to reshuffle
        # the 1D chunks into the target N-dimensional layout.
        return jax.device_put(physical_array, named_sharding)
    return physical_array


def _convert_ndarray_batch_to_jax_tensor_batch(
    ndarrays: Union[np.ndarray, Dict[str, np.ndarray]],
    named_sharding: "jax.sharding.NamedSharding" = None,  # noqa: F821
) -> Union["jax.Array", Dict[str, "jax.Array"]]:  # noqa: F821
    """Convert a NumPy ndarray batch to a globally sharded JAX Array batch.

    Args:
        ndarrays: A single NumPy ndarray or dictionary of NumPy ndarrays.
        named_sharding: The JAX NamedSharding specification defining the
            global mesh and partition layout. Default is ``None``, in which case
            the array will be sharded along the batch dimension across all devices.

    Returns:
         A globally sharded JAX Array (or dictionary of arrays) residing
         in TPU/GPU memory.
    """
    if isinstance(ndarrays, np.ndarray):
        return _convert_ndarray_to_jax_tensor(ndarrays, named_sharding)

    jax_batch = {}
    for col_name, col_ndarray in ndarrays.items():
        try:
            jax_batch[col_name] = _convert_ndarray_to_jax_tensor(
                col_ndarray, named_sharding
            )
        except ValueError as e:
            raise ValueError(f"JAX Sharding Error for column '{col_name}': \n{e}")

    return jax_batch


def jax_sync_generator(
    batch_iterable: Iterable[Any],
    drop_last: bool,
    named_sharding: "jax.sharding.NamedSharding" = None,  # noqa: F821
) -> Iterator[Union["jax.Array", Dict[str, "jax.Array"]]]:  # noqa: F821
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
        named_sharding: An optional JAX NamedSharding specification defining the mesh
            and partition layout. If None, the array is sharded 1D along the batch dimension.

    Yields:
        Union[jax.Array, Dict[str, jax.Array]]: A globally sharded JAX Array or a
            dictionary of JAX Arrays natively placed on devices.
    """
    import jax

    num_local_devices = jax.local_device_count()
    iterator = iter(batch_iterable)
    while True:
        try:
            batch = next(iterator)
            has_batch = True
        except StopIteration:
            has_batch = False
            local_batch_size = 0
            batch = None

        if has_batch:
            if isinstance(batch, dict):
                # Use the first column to determine the batch size
                try:
                    local_batch_size = len(next(iter(batch.values())))
                except StopIteration:
                    local_batch_size = 0
            else:
                local_batch_size = len(batch)

        if jax.process_count() > 1:
            import jax.numpy as jnp
            from jax.experimental.multihost_utils import process_allgather

            # Synchronize batch availability and size across all hosts.
            stack = jnp.array([int(has_batch), local_batch_size], dtype=jnp.int32)
            gathered = process_allgather(stack)

            all_have_batch = bool(gathered[:, 0].all())
            any_have_batch = bool(gathered[:, 0].any())

            if not any_have_batch:
                # All workers have exhausted their data.
                break

            if not all_have_batch:
                # Some workers have exhausted their data while others have more.
                if drop_last:
                    # Drop the remaining batches from the workers that still have data.
                    break
                else:
                    # Raise an error because the remaining batches will be unevenly distributed.
                    raise ValueError(
                        "Uneven number of batches detected across JAX workers. "
                        "Some workers have exhausted their data while others have more. "
                        "To safely drop orphaned batches without hanging, "
                        "set `drop_last=True` in `iter_jax_batches()`."
                    )

            min_batch_size = int(gathered[:, 1].min())
            max_batch_size = int(gathered[:, 1].max())
            # Fail all workers if any worker has a different batch size
            if max_batch_size > min_batch_size and not drop_last:
                raise ValueError(
                    "Uneven batch sizes detected across JAX workers. "
                    f"This host produced a batch of size {local_batch_size}, "
                    f"but the globally minimum batch size is {min_batch_size}. "
                    "To safely truncate the batch to the minimum size, "
                    "set `drop_last=True` in `iter_jax_batches()`."
                )
        else:
            if not has_batch:
                break
            min_batch_size = local_batch_size

        if min_batch_size % num_local_devices != 0:
            if drop_last:
                # Align the minimum batch size to be divisible by the number of local devices
                min_batch_size = min_batch_size - (min_batch_size % num_local_devices)
            else:
                raise ValueError(
                    f"The globally minimum batch size ({min_batch_size}) must be evenly "
                    f"divisible by the number of local JAX devices "
                    f"({num_local_devices}) on this host. "
                    f"To safely truncate the batch to a divisible size, "
                    f"set `drop_last=True` in `iter_jax_batches()`."
                )

        if min_batch_size == 0:
            # Data insufficient for even a single row across devices, skip and drop
            continue

        # At this point, if local_batch_size > min_batch_size, drop_last must be True
        if local_batch_size > min_batch_size:
            # Truncate to the minimum batch size across all hosts
            logger.info(
                f"Truncating batch from size {local_batch_size} to {min_batch_size}."
            )
            if isinstance(batch, dict):
                batch = {k: v[:min_batch_size] for k, v in batch.items()}
            else:
                batch = batch[:min_batch_size]

        yield _convert_ndarray_batch_to_jax_tensor_batch(
            batch, named_sharding=named_sharding
        )
