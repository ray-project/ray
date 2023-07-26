Distributed Tensorflow and Keras
================================

Ray integrates with Tensorflow as a training backend. This includes training
with Keras.

Ray Train configures ``TF_CONFIG`` for you, allowing you to run
your ``MultiWorkerMirroredStrategy`` training script.

Quickstart
----------

This example shows how you can use Ray Train to set up `Multi-worker training
with Keras <https://www.tensorflow.org/tutorials/distribute/multi_worker_with_keras>`_.

First, set up your dataset and model.

.. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
    :language: python
    :start-after: __tf_setup_begin__
    :end-before: __tf_setup_end__

Now define your single-worker TensorFlow training function.

.. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
    :language: python
    :start-after: __tf_single_begin__
    :end-before: __tf_single_end__

This training function can be executed with:

.. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
    :language: python
    :start-after: __tf_single_run_begin__
    :end-before: __tf_single_run_end__
    :dedent:

Now let's convert this to a distributed multi-worker training function!
All you need to do is:

1. Set the per-worker batch size - each worker will process the same size
   batch as in the single-worker code.
2. Choose your TensorFlow distributed training strategy. In this example
   we use the ``MultiWorkerMirroredStrategy``.

.. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
    :language: python
    :start-after: __tf_distributed_begin__
    :end-before: __tf_distributed_end__

Then, instantiate a ``TensorflowTrainer`` with 4 workers,
and use it to run the new training function!

.. literalinclude:: /../../python/ray/train/examples/tf/tensorflow_quick_start.py
    :language: python
    :start-after: __tf_trainer_begin__
    :end-before: __tf_trainer_end__
    :dedent:


.. _train-porting-code-tensorflow:

Porting code from an existing training loop
-------------------------------------------

The following instructions assume you have a training function
that can already be run on a single worker.

Updating your training function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Creating a TensorflowTrainer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``Trainer``\s are the primary Ray Train classes that are used to manage state and
execute training. You can create a ``TensorflowTrainer`` like this:

.. warning::
    Ray will not automatically set any environment variables or configuration
    related to local parallelism / threading
    :ref:`aside from "OMP_NUM_THREADS" <omp-num-thread-note>`.
    If you desire greater control over TensorFlow threading, use
    the ``tf.config.threading`` module (eg.
    ``tf.config.threading.set_inter_op_parallelism_threads(num_cpus)``)
    at the beginning of your ``train_loop_per_worker`` function.

.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.tensorflow import TensorflowTrainer
    # For GPU Training, set `use_gpu` to True.
    use_gpu = False
    trainer = TensorflowTrainer(
        train_func,
        scaling_config=ScalingConfig(use_gpu=use_gpu, num_workers=2)
    )

To customize the backend setup, you can specify a :class:`TensorflowConfig <ray.train.tensorflow.TensorflowConfig>`:


.. code-block:: python

    from ray.air import ScalingConfig
    from ray.train.tensorflow import TensorflowTrainer, TensorflowConfig

    trainer = TensorflowTrainer(
        train_func,
        tensorflow_backend=TensorflowConfig(...),
        scaling_config=ScalingConfig(num_workers=2),
    )

Running your training function
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

With a distributed training function and the ``TensorflowTrainer``, you are now
ready to start training!

.. code-block:: python

    trainer.fit()


Saving and loading checkpoints
------------------------------
:ref:`Checkpoints <checkpoint-api-ref>` can be saved by calling ``session.report(metrics, checkpoint=Checkpoint(...))`` in the
training function. This will cause the checkpoint state from the distributed
workers to be saved on the ``Trainer`` (where your python script is executed).

The latest saved checkpoint can be accessed through the ``checkpoint`` attribute of
the :py:class:`~ray.air.result.Result`, and the best saved checkpoints can be accessed by the ``best_checkpoints``
attribute.

Saving checkpoints
~~~~~~~~~~~~~~~~~~

To save a checkpoint, you create a :class:`Checkpoint <ray.air.checkpoint.Checkpoint>`
object and report it to Ray Train:


        .. code-block:: python
            :emphasize-lines: 23

            from ray.air import session, Checkpoint, ScalingConfig
            from ray.train.tensorflow import TensorflowTrainer

            import numpy as np

            def train_func(config):
                import tensorflow as tf
                n = 100
                # create a toy dataset
                # data   : X - dim = (n, 4)
                # target : Y - dim = (n, 1)
                X = np.random.normal(0, 1, size=(n, 4))
                Y = np.random.uniform(0, 1, size=(n, 1))

                strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()
                with strategy.scope():
                    # toy neural network : 1-layer
                    model = tf.keras.Sequential([tf.keras.layers.Dense(1, activation="linear", input_shape=(4,))])
                    model.compile(optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

                for epoch in range(config["num_epochs"]):
                    model.fit(X, Y, batch_size=20)
                    checkpoint = Checkpoint.from_dict(
                        dict(epoch=epoch, model_weights=model.get_weights())
                    )
                    session.report({}, checkpoint=checkpoint)

            trainer = TensorflowTrainer(
                train_func,
                train_loop_config={"num_epochs": 5},
                scaling_config=ScalingConfig(num_workers=2),
            )
            result = trainer.fit()

            print(result.checkpoint.to_dict())
            # {'epoch': 4, 'model_weights': [array([[-0.31858477],
            #    [ 0.03747174],
            #    [ 0.28266194],
            #    [ 0.8626015 ]], dtype=float32), array([0.02230084], dtype=float32)], '_timestamp': 1656107383, '_preprocessor': None, '_current_checkpoint_id': 4}

The checkpoints are saved to the configured :ref:`storage_path <train-persistent-storage>`.

Loading checkpoints
~~~~~~~~~~~~~~~~~~~

Checkpoints can be loaded into the training function in 2 steps:

1. From the training function, :func:`ray.air.session.get_checkpoint` can be used to access
   the most recently saved :py:class:`~ray.air.checkpoint.Checkpoint`. This is useful to continue training even
   if there's a worker failure.
2. The checkpoint to start training with can be bootstrapped by passing in a
   :py:class:`~ray.air.checkpoint.Checkpoint` to ``Trainer`` as the ``resume_from_checkpoint`` argument.


.. code-block:: python
    :emphasize-lines: 15, 21, 22, 25, 26, 27, 30

    from ray.air import session, Checkpoint, ScalingConfig
    from ray.train.tensorflow import TensorflowTrainer

    import numpy as np

    def train_func(config):
        import tensorflow as tf
        n = 100
        # create a toy dataset
        # data   : X - dim = (n, 4)
        # target : Y - dim = (n, 1)
        X = np.random.normal(0, 1, size=(n, 4))
        Y = np.random.uniform(0, 1, size=(n, 1))

        start_epoch = 0
        strategy = tf.distribute.experimental.MultiWorkerMirroredStrategy()

        with strategy.scope():
            # toy neural network : 1-layer
            model = tf.keras.Sequential([tf.keras.layers.Dense(1, activation="linear", input_shape=(4,))])
            checkpoint = session.get_checkpoint()
            if checkpoint:
                # assume that we have run the session.report() example
                # and successfully save some model weights
                checkpoint_dict = checkpoint.to_dict()
                model.set_weights(checkpoint_dict.get("model_weights"))
                start_epoch = checkpoint_dict.get("epoch", -1) + 1
            model.compile(optimizer="Adam", loss="mean_squared_error", metrics=["mse"])

        for epoch in range(start_epoch, config["num_epochs"]):
            model.fit(X, Y, batch_size=20)
            checkpoint = Checkpoint.from_dict(
                dict(epoch=epoch, model_weights=model.get_weights())
            )
            session.report({}, checkpoint=checkpoint)

    trainer = TensorflowTrainer(
        train_func,
        train_loop_config={"num_epochs": 2},
        scaling_config=ScalingConfig(num_workers=2),
    )
    # save a checkpoint
    result = trainer.fit()

    # load a checkpoint
    trainer = TensorflowTrainer(
        train_func,
        train_loop_config={"num_epochs": 5},
        scaling_config=ScalingConfig(num_workers=2),
        resume_from_checkpoint=result.checkpoint,
    )
    result = trainer.fit()

    print(result.checkpoint.to_dict())
    # {'epoch': 4, 'model_weights': [array([[-0.70056134],
    #    [-0.8839263 ],
    #    [-1.0043601 ],
    #    [-0.61634773]], dtype=float32), array([0.01889327], dtype=float32)], '_timestamp': 1656108446, '_preprocessor': None, '_current_checkpoint_id': 3}

Aggregating results across workers
----------------------------------

TensorFlow Keras automatically aggregates metrics from all workers. If you wish to have more
control over that, consider implementing a `custom training loop <https://www.tensorflow.org/tutorials/distribute/custom_training>`_.
