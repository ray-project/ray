from ray.util.annotations import PublicAPI

import tensorflow as tf


@PublicAPI(stability="beta")
def prepare_dataset_shard(tf_dataset_shard: tf.data.Dataset):
    """A utility function that disables Tensorflow autosharding.

    This should be used on a  TensorFlow ``Dataset`` created by calling ``to_tf()``
    on a ``ray.data.Dataset`` returned by ``ray.train.get_dataset_shard()`` since
    the dataset has already been sharded across the workers.

    Args:
        tf_dataset_shard (tf.data.Dataset): A TensorFlow Dataset.

    Returns:
        A TensorFlow Dataset with autosharding turned off.
    """
    options = tf.data.Options()
    options.experimental_distribute.auto_shard_policy = (
        tf.data.experimental.AutoShardPolicy.OFF
    )
    return tf_dataset_shard.with_options(options)
