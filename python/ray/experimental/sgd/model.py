from __future__ import absolute_import
from __future__ import division
from __future__ import print_function


class Model(object):
    """Your class must implement this interface to be used with Ray SGD.

    This supports any form of input pipeline: it is up to you to define it
    using TensorFlow. Between SGD steps, you can access the models via

    Examples:
        >>> # Setup distributed SGD
        >>> model_creator = (
        ...   lambda worker_idx, device_idx: TFBenchModel(
        ...     batch=args.batch_size, use_cpus=True))
        >>> sgd = DistributedSGD(
        ...   model_creator, num_workers=2,
        ...   devices_per_worker=4, gpu=True, strategy="ps")

        >>> # To train
        >>> for i in range(100):
        ...   stats = sgd.step(fetch_stats=i % 10 == 0)

        >>> # To access or update model state
        >>> sgd.foreach_model(lambda model: ...)

    Attributes:
        loss (tf.Tensor): Loss function to minimize.
        optimizer (tf.train.Optimizer): Optimizer to use to minimize the loss.
    """

    def get_feed_dict(self):
        """Feed dict to pass in when computing gradients for the loss."""
        return {}
