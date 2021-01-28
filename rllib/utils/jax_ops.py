import numpy as np
import tree

from ray.rllib.utils.framework import try_import_jax

jax, _ = try_import_jax()
jnp = None
if jax:
    import jax.numpy as jnp


def convert_to_jax_device_array(x, device=None):
    """Converts any struct to jax.numpy.DeviceArray.

    x (any): Any (possibly nested) struct, the values in which will be
        converted and returned as a new struct with all leaves converted
        to torch tensors.

    Returns:
        Any: A new struct with the same structure as `stats`, but with all
            values converted to jax.numpy.DeviceArray types.
    """

    def mapping(item):
        # Already JAX DeviceArray -> make sure it's on right device.
        if isinstance(item, jnp.DeviceArray):
            return item if device is None else item.to(device)
        # Numpy arrays.
        if isinstance(item, np.ndarray):
            # np.object_ type (e.g. info dicts in train batch): leave as-is.
            if item.dtype == np.object_:
                return item
            # Already numpy: Wrap as torch tensor.
            else:
                tensor = jnp.array(item)
        # Everything else: Convert to numpy, then wrap as torch tensor.
        else:
            tensor = jnp.asarray(item)
        # Floatify all float64 tensors.
        if tensor.dtype == jnp.double:
            tensor = tensor.astype(jnp.float32)
        return tensor if device is None else tensor.to(device)

    return tree.map_structure(mapping, x)


def convert_to_non_jax_type(stats):
    """Converts values in `stats` to non-JAX numpy or python types.

    Args:
        stats (any): Any (possibly nested) struct, the values in which will be
            converted and returned as a new struct with all JAX DeviceArrays
            being converted to numpy types.

    Returns:
        Any: A new struct with the same structure as `stats`, but with all
            values converted to non-JAX Tensor types.
    """

    # The mapping function used to numpyize JAX DeviceArrays.
    def mapping(item):
        if isinstance(item, jnp.DeviceArray):
            return np.array(item)
        else:
            return item

    return tree.map_structure(mapping, stats)


def explained_variance(y, pred):
    y_var = jnp.var(y, axis=[0])
    diff_var = jnp.var(y - pred, axis=[0])
    min_ = 1.0
    return jnp.maximum(min_, 1 - (diff_var / y_var))


def sequence_mask(lengths, maxlen=None, dtype=None, time_major=False):
    """Offers same behavior as tf.sequence_mask for JAX numpy.

    Thanks to Dimitris Papatheodorou
    (https://discuss.pytorch.org/t/pytorch-equivalent-for-tf-sequence-mask/
    39036).
    """
    if maxlen is None:
        maxlen = int(lengths.max())

    mask = ~(jnp.ones((len(lengths), maxlen)).cumsum(axis=1).t() > lengths)
    if not time_major:
        mask = mask.t()
    mask.type(dtype or jnp.bool_)

    return mask
