.. _train-tensorflow-overview:

Distributed Tensorflow & Keras
==============================

Ray Train configures ``TF_CONFIG`` for you, allowing you to run
your ``MultiWorkerMirroredStrategy`` training script. See `Distributed
training with TensorFlow <https://www.tensorflow.org/guide/distributed_training>`_
for more information.


Most of the examples in this guide use Tensorflow with Keras, but
Ray Train also works with vanilla Tensorflow.


Updating your training function
-------------------------------

First, you'll want to update your training function to support distributed
training.


.. note::
   The current TensorFlow implementation supports
   ``MultiWorkerMirroredStrategy`` (and ``MirroredStrategy``). If there are
   other strategies you wish to see supported by Ray Train, please let us know
   by submitting a `feature request on GitHub <https://github.com/ray-project/ray/issues>`_.

These instructions closely follow TensorFlow's `Multi-worker training
with Keras <https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras>`_
tutorial. One key difference is that Ray Train will handle the environment
variable set up for you.

**Step 1:** Wrap your model in ``MultiWorkerMirroredStrategy``.

The `MultiWorkerMirroredStrategy <https://www.tensorflow.org/api_docs/python/tf/distribute/experimental/MultiWorkerMirroredStrategy>`_
enables synchronous distributed training. The ``Model`` *must* be built and
compiled within the scope of the strategy.

.. code-block:: python

    with tf.distribute.MultiWorkerMirroredStrategy().scope():
        model = ... # build model
        model.compile()

**Step 2:** Update your ``Dataset`` batch size to the *global* batch
size.

The `batch <https://www.tensorflow.org/api_docs/python/tf/data/Dataset#batch>`_
will be split evenly across worker processes, so ``batch_size`` should be
set appropriately.

.. code-block:: diff

    -batch_size = worker_batch_size
    +batch_size = worker_batch_size * session.get_world_size()


.. warning::
    Ray will not automatically set any environment variables or configuration
    related to local parallelism / threading
    :ref:`aside from "OMP_NUM_THREADS" <omp-num-thread-note>`.
    If you desire greater control over TensorFlow threading, use
    the ``tf.config.threading`` module (eg.
    ``tf.config.threading.set_inter_op_parallelism_threads(num_cpus)``)
    at the beginning of your ``train_loop_per_worker`` function.

Creating a :class:`~ray.train.tensorflow.TensorflowTrainer`
-----------------------------------------------------------

``Trainer``\s are the primary Ray Train classes that are used to manage state and
execute training. For distributed Tensorflow,
we use a :class:`~ray.train.tensorflow.TensorflowTrainer`
that you can setup like this:

.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.tensorflow import TensorflowTrainer
    # For GPU Training, set `use_gpu` to True.
    use_gpu = False
    trainer = TensorflowTrainer(
        train_func,
        scaling_config=ScalingConfig(use_gpu=use_gpu, num_workers=2)
    )

To customize the backend setup, you can pass a
:class:`~ray.train.tensorflow.TensorflowConfig`:

.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.tensorflow import TensorflowTrainer, TensorflowConfig

    trainer = TensorflowTrainer(
        train_func,
        tensorflow_backend=TensorflowConfig(...),
        scaling_config=ScalingConfig(num_workers=2),
    )


For more configurability, please reference the :py:class:`~ray.train.data_parallel_trainer.DataParallelTrainer` API.


Running your training function
------------------------------

With a distributed training function and a Ray Train ``Trainer``, you are now
ready to start training!

.. code-block:: python

    trainer.fit()

Data loading and preprocessing
------------------------------

