.. _computer-vision:

Computer Vision
===============

This guide explains how to perform common computer vision tasks like:
* Reading image data
* Transforming images
* Training vision models
* Batch predicting images
* Serving vision models

Reading image data
------------------

.. tabbed:: Images

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_images_start__
        :end-before: __read_images_stop__
        :dedent:

.. tabbed:: NumPy

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_numpy_start__
        :end-before: __read_numpy_stop__
        :dedent:

.. tabbed:: TFRecords

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_tfrecords_start__
        :end-before: __read_tfrecords_stop__
        :dedent:

.. tabbed:: Parquet

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_parquet_start__
        :end-before: __read_parquet_stop__
        :dedent:

For more information on creating datasets, see Creating Datasets.

Transforming images
-------------------

.. tabbed:: Torch

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_preprocessors_start__
        :end-before: __torch_preprocessors_stop__
        :dedent:


.. tabbed:: TensorFlow

    .. testcode::

        from tensorflow.keras.applications import imagenet_utils

        from ray.data.preprocessors import BatchMapper

        def preprocess(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
            batch["image"] = imagenet_utils.preprocess_input(batch["image"])
            return batch

        preprocessor = BatchMapper(preprocess, batch_format="numpy")

For more information on transforming data, see Using Preprocessors and Transforming Datasets.

Training vision models
----------------------

.. tabbed:: Torch

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_trainer_start__
        :end-before: __torch_trainer_stop__
        :dedent:

.. tabbed:: TensorFlow

    ham

Creating checkpoints
--------------------

:class:`Checkpoints <ray.air.checkpoint.Checkpoint>` are required for batch inference and model
serving. They contain model state and optionally a preprocessor.

.. tabbed:: Torch

    To create a :class:`~ray.train.torch.TorchCheckpoint`, pass a Torch model to
    :meth:`TorchCheckpoint.from_model() <ray.train.torch.TorchCheckpoint.from_model>`. If you want
    to preprocess images before inference, also pass a :class:`~ray.data.preprocessor.Preprocessor`.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_checkpoint_start__
        :end-before: __torch_checkpoint_stop__
        :dedent:

.. tabbed:: TensorFlow

    ham

.. tip::
    :meth:`Trainer.fit() <ray.train.trainer.BaseTrainer.fit>` returns a :class:`~ray.air.result.Result` object.
    If you're going from training to prediction, don't create a new checkpoint. Instead,
    use :attr:`Result.checkpoint <ray.air.result.Result.checkpoint>`.

Batch predicting images
-----------------------

:class:`~ray.train.batch_predictor.BatchPredictor` lets you perform inference on large
image datasets. To create a ``BatchPredictor``, call
:meth:`BatchPredictor.from_checkpoint <ray.train.batch_predictor.BatchPredictor.from_checkpoint>` and pass the checkpoint
you created in `Creating checkpoints`_.

.. tabbed:: Torch

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_batch_predictor_start__
        :end-before: __torch_batch_predictor_stop__
        :dedent:

.. tabbed:: TensorFlow

    ham

Serving vision models
---------------------

.. tabbed:: Torch

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_serve_start__
        :end-before: __torch_serve_stop__
        :dedent:

.. tabbed:: TensorFlow

    ham
