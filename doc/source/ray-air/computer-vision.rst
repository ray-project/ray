:orphan:

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

        Then, apply a :ref:`user-defined function <transforming_data>` to
        encode the class names as integer targets.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __read_images2_start__
            :end-before: __read_images2_stop__
            :dedent:

        .. tip::

            You can also use :class:`~ray.data.preprocessors.LabelEncoder` to encode labels.

    .. tab-item:: NumPy

        To load NumPy arrays into a :class:`~ray.data.Dataset`, separately read the image and label arrays.

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

        To load examples stored in this format, read the TFRecords into a :class:`~ray.data.Dataset`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __read_tfrecords1_start__
            :end-before: __read_tfrecords1_stop__
            :dedent:

        Then, apply a :ref:`user-defined function <transforming_data>` to
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


For more information on creating datasets, see :ref:`Loading Data <loading_data>`.


Transforming images
-------------------

To transform images, create a :class:`~ray.data.preprocessor.Preprocessor`. They're the
standard way to preprocess data with Ray.

.. tab-set::

    .. tab-item:: Torch

        To apply TorchVision transforms, create a :class:`~ray.data.preprocessors.TorchVisionPreprocessor`.

        Create two :class:`TorchVisionPreprocessors <ray.data.preprocessors.TorchVisionPreprocessor>`
        -- one to normalize images, and another to augment images. Later, you'll pass the preprocessors to :class:`Trainers <ray.train.trainer.BaseTrainer>` and
        :class:`Predictors <ray.train.predictor.Predictor>`.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_preprocessors_start__
            :end-before: __torch_preprocessors_stop__
            :dedent:

    .. tab-item:: TensorFlow

        To apply TorchVision transforms, create a :class:`~ray.data.preprocessors.BatchMapper`.

        Create two :class:`~ray.data.preprocessors.BatchMapper` -- one to normalize images, and another to
        augment images. Later, you'll pass the preprocessors to :class:`Trainers <ray.train.trainer.BaseTrainer>` and
        :class:`Predictors <ray.train.predictor.Predictor>`.

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

        For more in-depth examples, see :ref:`the Ray Train documentation <train-docs>`.

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

        For more information, check out :ref:`the Ray Train documentation <train-docs>`.

Batch predicting images
-----------------------

To perform inference with a pre-trained model on images, first load and transform your data.

.. testcode::

    from typing import Any, Dict
    from torchvision import transforms
    import ray

    def transform_image(row: Dict[str, Any]) -> Dict[str, Any]:
        transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Resize((32, 32))
        ])
        row["image"] = transform(row["image"])
        return row

    ds = (
        ray.data.read_images("s3://anonymous@ray-example-data/batoidea/JPEGImages")
        .map(transform_image)
    )

Next, implement a callable class that sets up and invokes your model.

.. testcode::

    import torch
    from torchvision import models

    class ImageClassifier:
        def __init__(self):
            weights = models.ResNet18_Weights.DEFAULT
            self.model = models.resnet18(weights=weights)
            self.model.eval()

        def __call__(self, batch):
            inputs = torch.from_numpy(batch["image"])
            with torch.inference_mode():
                outputs = self.model(inputs)
            return {"class": outputs.argmax(dim=1)}

Finally, call :meth:`Dataset.map_batches() <ray.data.Dataset.map_batches>`.

.. testcode::

    predictions = ds.map_batches(
        ImageClassifier,
        compute=ray.data.ActorPoolStrategy(size=2),
        batch_size=4
    )
    predictions.show(3)

.. testoutput::

    {'class': 118}
    {'class': 153}
    {'class': 296}

For more information on performing inference on images, see
:ref:`End-to-end: Offline Batch Inference <batch_inference_home>`
and :ref:`Working with images <working_with_images>`.

Serving vision models
---------------------

:class:`~ray.serve.Deployment` lets you
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

        To deploy a Torch model to an endpoint, create a predictor from the checkpoint you created in `Creating checkpoints`_
        and serve via a Ray Serve deployment.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_serve_start__
            :end-before: __torch_serve_stop__
            :dedent:

        Then, make a request to classify an image.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __torch_online_predict_start__
            :end-before: __torch_online_predict_stop__
            :dedent:

        For more in-depth examples, read about :ref:`Ray Serve <serve-getting-started>`.

    .. tab-item:: TensorFlow

        To deploy a TensorFlow model to an endpoint, use the checkpoint you created in `Creating checkpoints`_
        to create a Ray Serve deployment serving the model.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __tensorflow_serve_start__
            :end-before: __tensorflow_serve_stop__
            :dedent:

        Then, make a request to classify an image.

        .. literalinclude:: ./doc_code/computer_vision.py
            :start-after: __tensorflow_online_predict_start__
            :end-before: __tensorflow_online_predict_stop__
            :dedent:

        For more information, see :ref:`Ray Serve <serve-getting-started>`.
