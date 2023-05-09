.. _computer-vision:

Computer Vision
===============

This guide explains how to perform common computer vision tasks like:

* `Reading image data`_
* `Transforming images`_
* `Training vision models`_
* `Batch predicting images`_
* `Serving vision models`_

Reading image data
------------------

.. tab-set::

    .. tab-item:: Raw images

        Datasets like ImageNet store files like this:

        .. code-block::

            root/dog/xxx.png
            root/dog/xxy.png
            root/dog/[...]/xxz.png

            root/cat/123.png
            root/cat/nsdf3.png
            root/cat/[...]/asd932_.png

        To load images stored in this layout, read the raw images and include the
        class names.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __read_images1_start__
            :end-before: __read_images1_stop__
            :dedent:

        Then, apply a :ref:`user-defined function <transform_datastreams_writing_udfs>` to
        encode the class names as integer targets.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __read_images2_start__
            :end-before: __read_images2_stop__
            :dedent:

        .. tip::

            You can also use :class:`~ray.data.preprocessors.LabelEncoder` to encode labels.

    .. tab-item:: NumPy

        To load NumPy arrays into a :class:`~ray.data.Datastream`, separately read the image and label arrays.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __read_numpy1_start__
            :end-before: __read_numpy1_stop__
            :dedent:

        Then, combine the datasets and rename the columns.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __read_numpy2_start__
            :end-before: __read_numpy2_stop__
            :dedent:

    .. tab-item:: TFRecords

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

        To load examples stored in this format, read the TFRecords into a :class:`~ray.data.Datastream`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __read_tfrecords1_start__
            :end-before: __read_tfrecords1_stop__
            :dedent:

        Then, apply a :ref:`user-defined function <transform_datastreams_writing_udfs>` to
        decode the raw image bytes.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __read_tfrecords2_start__
            :end-before: __read_tfrecords2_stop__
            :dedent:

    .. tab-item:: Parquet

        To load image data stored in Parquet files, call :func:`ray.data.read_parquet`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __read_parquet_start__
            :end-before: __read_parquet_stop__
            :dedent:


For more information on creating datastreams, see :ref:`Loading Data <loading_data>`.


Transforming images
-------------------

To transform images, create a :class:`~ray.data.preprocessor.Preprocessor`. They're the
standard way to preprocess data with Ray.

.. tab-set::

    .. tab-item:: Torch

        To apply TorchVision transforms, create a :class:`~ray.data.preprocessors.TorchVisionPreprocessor`.

        Create two :class:`TorchVisionPreprocessors <ray.data.preprocessors.TorchVisionPreprocessor>`
        -- one to normalize images, and another to augment images. Later, you'll pass the preprocessors to :class:`Trainers <ray.train.trainer.BaseTrainer>`,
        :class:`Predictors <ray.train.predictor.Predictor>`, and
        :class:`PredictorDeployments <ray.serve.air_integrations.PredictorDeployment>`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_preprocessors_start__
            :end-before: __torch_preprocessors_stop__
            :dedent:

    .. tab-item:: TensorFlow

        To apply TorchVision transforms, create a :class:`~ray.data.preprocessors.BatchMapper`.

        Create two :class:`~ray.data.preprocessors.BatchMapper` -- one to normalize images, and another to
        augment images. Later, you'll pass the preprocessors to :class:`Trainers <ray.train.trainer.BaseTrainer>`,
        :class:`Predictors <ray.train.predictor.Predictor>`, and
        :class:`PredictorDeployments <ray.serve.air_integrations.PredictorDeployment>`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __tensorflow_preprocessors_start__
            :end-before: __tensorflow_preprocessors_stop__
            :dedent:

For more information on transforming data, see
:ref:`Using Preprocessors <air-preprocessors>` and
:ref:`Transforming Data <transforming_data>`.

Training vision models
----------------------

:class:`Trainers <ray.train.trainer.BaseTrainer>` let you train models in parallel.

.. tab-set::

    .. tab-item:: Torch

        To train a vision model, define the training loop per worker.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_training_loop_start__
            :end-before: __torch_training_loop_stop__
            :dedent:

        Then, create a :class:`~ray.train.torch.TorchTrainer` and call
        :meth:`~ray.train.torch.TorchTrainer.fit`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_trainer_start__
            :end-before: __torch_trainer_stop__
            :dedent:

        For more in-depth examples, read :doc:`/ray-air/examples/torch_image_example` and
        :ref:`Using Trainers <air-trainers>`.

    .. tab-item:: TensorFlow

        To train a vision model, define the training loop per worker.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __tensorflow_training_loop_start__
            :end-before: __tensorflow_training_loop_stop__
            :dedent:

        Then, create a :class:`~ray.train.tensorflow.TensorflowTrainer` and call
        :meth:`~ray.train.tensorflow.TensorflowTrainer.fit`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __tensorflow_trainer_start__
            :end-before: __tensorflow_trainer_stop__
            :dedent:

        For more information, read :ref:`Using Trainers <air-trainers>`.

Creating checkpoints
--------------------

:class:`Checkpoints <ray.air.checkpoint.Checkpoint>` are required for batch inference and model
serving. They contain model state and optionally a preprocessor.

If you're going from training to prediction, don't create a new checkpoint.
:meth:`Trainer.fit() <ray.train.trainer.BaseTrainer.fit>` returns a
:class:`~ray.air.result.Result` object. Use
:attr:`Result.checkpoint <ray.air.result.Result.checkpoint>` instead.

.. tab-set::

    .. tab-item:: Torch

        To create a :class:`~ray.train.torch.TorchCheckpoint`, pass a Torch model and
        the :class:`~ray.data.preprocessor.Preprocessor` you created in `Transforming images`_
        to :meth:`TorchCheckpoint.from_model() <ray.train.torch.TorchCheckpoint.from_model>`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_checkpoint_start__
            :end-before: __torch_checkpoint_stop__
            :dedent:

    .. tab-item:: TensorFlow

        To create a :class:`~ray.train.tensorflow.TensorflowCheckpoint`, pass a TensorFlow model and
        the :class:`~ray.data.preprocessor.Preprocessor` you created in `Transforming images`_
        to :meth:`TensorflowCheckpoint.from_model() <ray.train.tensorflow.TensorflowCheckpoint.from_model>`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __tensorflow_checkpoint_start__
            :end-before: __tensorflow_checkpoint_stop__
            :dedent:


Batch predicting images
-----------------------

:class:`~ray.train.batch_predictor.BatchPredictor` lets you perform inference on large
image datasets.

.. tab-set::

    .. tab-item:: Torch

        To create a :class:`~ray.train.batch_predictor.BatchPredictor`, call
        :meth:`BatchPredictor.from_checkpoint <ray.train.batch_predictor.BatchPredictor.from_checkpoint>` and pass the checkpoint
        you created in `Creating checkpoints`_.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_batch_predictor_start__
            :end-before: __torch_batch_predictor_stop__
            :dedent:

        For more in-depth examples, read :doc:`/ray-air/examples/pytorch_resnet_batch_prediction`
        and :ref:`Using Predictors for Inference <air-predictors>`.

    .. tab-item:: TensorFlow

        To create a :class:`~ray.train.batch_predictor.BatchPredictor`, call
        :meth:`BatchPredictor.from_checkpoint <ray.train.batch_predictor.BatchPredictor.from_checkpoint>` and pass the checkpoint
        you created in `Creating checkpoints`_.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __tensorflow_batch_predictor_start__
            :end-before: __tensorflow_batch_predictor_stop__
            :dedent:

        For more information, read :ref:`Using Predictors for Inference <air-predictors>`.

Serving vision models
---------------------

:class:`~ray.serve.air_integrations.PredictorDeployment` lets you
deploy a model to an endpoint and make predictions over the Internet.

Deployments use :ref:`HTTP adapters <serve-http>` to define how HTTP messages are converted to model
inputs. For example, :func:`~ray.serve.http_adapters.json_to_ndarray` converts HTTP messages like this:

.. code-block::

    {"array": [[1, 2], [3, 4]]}

To NumPy ndarrays like this:

.. code-block::

    array([[1., 2.],
            [3., 4.]])

.. tab-set::

    .. tab-item:: Torch

        To deploy a Torch model to an endpoint, pass the checkpoint you created in `Creating checkpoints`_
        to :meth:`PredictorDeployment.bind <ray.serve.air_integrations.PredictorDeployment.bind>` and specify
        :func:`~ray.serve.http_adapters.json_to_ndarray` as the HTTP adapter.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_serve_start__
            :end-before: __torch_serve_stop__
            :dedent:

        Then, make a request to classify an image.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_online_predict_start__
            :end-before: __torch_online_predict_stop__
            :dedent:

        For more in-depth examples, read :doc:`/ray-air/examples/torch_image_example`
        and :doc:`/ray-air/examples/serving_guide`.

    .. tab-item:: TensorFlow

        To deploy a TensorFlow model to an endpoint, pass the checkpoint you created in `Creating checkpoints`_
        to :meth:`PredictorDeployment.bind <ray.serve.air_integrations.PredictorDeployment.bind>` and specify
        :func:`~ray.serve.http_adapters.json_to_multi_ndarray` as the HTTP adapter.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __tensorflow_serve_start__
            :end-before: __tensorflow_serve_stop__
            :dedent:

        Then, make a request to classify an image.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __tensorflow_online_predict_start__
            :end-before: __tensorflow_online_predict_stop__
            :dedent:

        For more information, read :doc:`/ray-air/examples/serving_guide`.
