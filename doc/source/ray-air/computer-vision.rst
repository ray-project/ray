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

    Datasets like ImageNet store files like this:

    .. code-block::

        root/dog/xxx.png
        root/dog/xxy.png
        root/dog/[...]/xxz.png

        root/cat/123.png
        root/cat/nsdf3.png
        root/cat/[...]/asd932_.png

    To load images stored in this layout, read the raw images and include the
    image paths.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_images1_start__
        :end-before: __read_images1_stop__
        :dedent:

    Then, apply a :ref:`user-defined functions <transform_datasets_writing_udfs>` to
    parse the class names from the image paths.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_images2_start__
        :end-before: __read_images2_stop__
        :dedent:

.. tabbed:: NumPy

    To load NumPy arrays into a ``Dataset``, separately read the image and label arrays.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_numpy1_start__
        :end-before: __read_numpy1_stop__
        :dedent:

    Then, combine the datasets and rename the columns.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_numpy2_start__
        :end-before: __read_numpy2_stop__
        :dedent:

.. tabbed:: TFRecords

    Image datasets often contain ``tf.train.Example`` messages that look like this:

    .. code-block::

        features {
            feature {
                key: "image"
                value {
                    bytes_list {
                        value: ...  # Raw image bytes
                    }
                }
            }
            feature {
                key: "label"
                value {
                    int64_list {
                        value: 3
                    }
                }
            }
        }

    To load examples stored in this format, read the TFRecords into a ``Dataset``.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_tfrecords1_start__
        :end-before: __read_tfrecords1_stop__
        :dedent:

    Then, apply a :ref:`user-defined function <transform_datasets_writing_udfs>` to
    decode the raw image bytes.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_tfrecords2_start__
        :end-before: __read_tfrecords2_stop__
        :dedent:

.. tabbed:: Parquet

    To load image data stored in Parquet files, call :func:`ray.data.read_parquet`.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __read_parquet_start__
        :end-before: __read_parquet_stop__
        :dedent:


For more information on creating datasets, see Creating Datasets.


Transforming images
-------------------

To transform images, create a Preprocessor. They're the standard way to preprocess data
with Ray.

.. tabbed:: Torch

    To apply TorchVision transforms, create a ``TorchVisionPreprocessor``.

    Create two ``TorchVisionPreprocessors`` -- one to normalize images, and another to
    augment images. Later, you'll pass the preprocessors to ``Trainers``, ``Predictors``,
    and ``PredictorDeployments``.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_preprocessors_start__
        :end-before: __torch_preprocessors_stop__
        :dedent:

    .. warning::
        If you read raw images in `Reading image data`_, then your dataset
        contains class names instead of integer targets. To fix this issue, chain
        ``preprocessor`` with a label encoder:

        .. code-block:: python

            from ray.data.preprocessors import Chain, LabelEncoder

            preprocessor = Chain(
                preprocessor,
                LabelEncoder(columns=["label"])
            )


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

:class:`Trainers <ray.train.trainer.BaseTrainer>` let you train models in parallel.

.. tabbed:: Torch

    To train a vision model, define the training loop per worker.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_training_loop_start__
        :end-before: __torch_training_loop_stop__
        :dedent:

    Then, create a :class:`~ray.train.torch.TorchTrainer` and call
    :meth:`~ray.train.torch.TorchTrainer.fit`.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_trainer_start__
        :end-before: __torch_trainer_stop__
        :dedent:

    For a more in-depth example, read :doc:`/ray-air/examples/torch_image_example`.

.. tabbed:: TensorFlow

    ham

Creating checkpoints
--------------------

:class:`Checkpoints <ray.air.checkpoint.Checkpoint>` are required for batch inference and model
serving. They contain model state and optionally a preprocessor.

.. tabbed:: Torch

    To create a :class:`~ray.train.torch.TorchCheckpoint`, pass a Torch model and
    the :class:`~ray.data.preprocessor.Preprocessor` you created in `Transforming images`_
    to :meth:`TorchCheckpoint.from_model() <ray.train.torch.TorchCheckpoint.from_model>`.

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
image datasets.

.. tabbed:: Torch

    To create a ``BatchPredictor``, call
    :meth:`BatchPredictor.from_checkpoint <ray.train.batch_predictor.BatchPredictor.from_checkpoint>` and pass the checkpoint
    you created in `Creating checkpoints`_.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_batch_predictor_start__
        :end-before: __torch_batch_predictor_stop__
        :dedent:

    For a more in-depth example, read :doc:`/ray-air/examples/pytorch_resnet_batch_prediction`.

.. tabbed:: TensorFlow

    ham

Serving vision models
---------------------

:class:`~ray.serve.air_integrations.PredictorDeployment` lets you
deploy a model to an endpoint and make predictions over the Internet.

Deployments use :ref:`HTTP adapters <serve-http>` to define how HTTP messages are converted to model
inputs. For example, :func:`~ray.serve.http_adapters.json_to_ndarray` converts HTTP messages like:

.. code-block::

    {"array": [[1, 2], [3, 4]]}

To a NumPy ndarrays like:

.. code-block::

    array([[1., 2.],
            [3., 4.]])


.. tabbed:: Torch

    To deploy a Torch model to an endpoint, pass the checkpoint you created in `Creating checkpoints`_
    to :meth:`PredictorDeployment.bind <ray.serve.air_integrations.PredictorDeployment.bind>` and specify
    :func:`~ray.serve.http_adapters.json_to_ndarray` as the HTTP adapter.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_serve_start__
        :end-before: __torch_serve_stop__
        :dedent:

    Then, make a request to classify an image.

    .. literalinclude:: ./doc_code/torch_computer_vision.py
        :start-after: __torch_online_predict_start__
        :end-before: __torch_online_predict_stop__
        :dedent:

.. tabbed:: TensorFlow

    ham
