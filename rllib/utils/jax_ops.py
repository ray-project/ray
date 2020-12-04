from ray.rllib.utils.framework import try_import_jax

jax, _ = try_import_jax()
jnp = None
if jax:
    import jax.numpy as jnp


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

    mask = ~(jnp.ones(
        (len(lengths), maxlen)).cumsum(axis=1).t() > lengths)
    if not time_major:
        mask = mask.t()
    mask.type(dtype or jnp.bool_)

    return mask
