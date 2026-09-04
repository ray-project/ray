import tensorflow as tf

from ray.util.annotations import PublicAPI


@PublicAPI(stability="beta")
def prepare_dataset_shard(tf_dataset_shard: tf.data.Dataset):
    """A utility function that overrides default config for Tensorflow Dataset.

    This should be used on a TensorFlow ``Dataset`` created by calling
    ``iter_tf_batches()`` or ``to_tf()`` on a ``ray.data.Dataset`` returned by
    ``ray.train.get_dataset_shard()`` since the dataset has already
    been sharded across the workers.

    When used with ``tf.distribute.MultiWorkerMirroredStrategy``, disabling
    auto-sharding is critical: the strategy would otherwise attempt to
    re-partition the already-sharded data, causing the per-epoch step count
    to double (e.g., 33 -> 66 for 2000 rows, 2 workers, batch 31). See
    https://github.com/ray-project/ray/issues/61838.

    Args:
        tf_dataset_shard: A TensorFlow Dataset.

    Returns:
        A TensorFlow Dataset with:
            - autosharding turned off
            - prefetching turned on with autotune enabled
    """
    options = tf.data.Options()
    options.experimental_distribute.auto_shard_policy = (
        tf.data.experimental.AutoShardPolicy.OFF
    )
    return tf_dataset_shard.with_options(options).prefetch(tf.data.AUTOTUNE)
