.. _working_with_images:

Working with Images
===================

With Ray Data, you can easily read and transform large image datasets.

This guide shows you how to:

* :ref:`Read images <reading_images>`
* :ref:`Transform images <transforming_images>`
* :ref:`Perform inference on images <performing_inference_on_images>`
* :ref:`Save images <saving_images>`

.. _reading_images:

Reading images
--------------

Ray Data can read images from a variety of formats.

To view the full list of supported file formats, see the
:ref:`Input/Output reference <input-output>`.

.. tab-set::

    .. tab-item:: Raw images

        To load raw images like JPEG files, call :func:`~ray.data.read_images`.

        .. note::

            :func:`~ray.data.read_images` uses
            `PIL <https://pillow.readthedocs.io/en/stable/index.html>`_. For a list of
            supported file formats, see
            `Image file formats <https://pillow.readthedocs.io/en/stable/handbook/image-file-formats.html>`_.

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/batoidea/JPEGImages")

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            image   numpy.ndarray(shape=(32, 32, 3), dtype=uint8)

    .. tab-item:: NumPy

        To load images stored in NumPy format, call :func:`~ray.data.read_numpy`.

        .. testcode::

            import ray

            ds = ray.data.read_numpy("s3://anonymous@air-example-data/cifar-10/images.npy")

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            data    numpy.ndarray(shape=(32, 32, 3), dtype=uint8)

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

        To load examples stored in this format, call :func:`~ray.data.read_tfrecords`.
        Then, call :meth:`~ray.data.Dataset.map` to decode the raw image bytes.

        .. testcode::

            import io
            from typing import Any, Dict
            import numpy as np
            from PIL import Image
            import ray

            def decode_bytes(row: Dict[str, Any]) -> Dict[str, Any]:
                data = row["image"]
                image = Image.open(io.BytesIO(data))
                row["image"] = np.array(image)
                return row

            ds = (
                ray.data.read_tfrecords(
                    "s3://anonymous@air-example-data/cifar-10/tfrecords"
                )
                .map(decode_bytes)
            )

            print(ds.schema())

        ..
            The following `testoutput` is mocked because the order of column names can
            be non-deterministic. For an example, see
            https://buildkite.com/ray-project/oss-ci-build-branch/builds/4849#01892c8b-0cd0-4432-bc9f-9f86fcd38edd.

        .. testoutput::
            :options: +MOCK

            Column  Type
            ------  ----
            image   numpy.ndarray(shape=(32, 32, 3), dtype=uint8)
            label   int64

    .. tab-item:: Parquet

        To load image data stored in Parquet files, call :func:`ray.data.read_parquet`.

        .. testcode::

            import ray

            ds = ray.data.read_parquet("s3://anonymous@air-example-data/cifar-10/parquet")

            print(ds.schema())

        .. testoutput::

            Column  Type
            ------  ----
            image   numpy.ndarray(shape=(32, 32, 3), dtype=uint8)
            label   int64


For more information on creating datasets, see :ref:`Loading Data <loading_data>`.

.. _transforming_images:

Transforming images
-------------------

To transform images, call :meth:`~ray.data.Dataset.map` or
:meth:`~ray.data.Dataset.map_batches`.

.. testcode::

    from typing import Any, Dict
    import numpy as np
    import ray

    def increase_brightness(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        batch["image"] = np.clip(batch["image"] + 4, 0, 255)
        return batch

    ds = (
        ray.data.read_images("s3://anonymous@ray-example-data/batoidea/JPEGImages")
        .map_batches(increase_brightness)
    )

For more information on transforming data, see
:ref:`Transforming data <transforming_data>`.

.. _performing_inference_on_images:

Performing inference on images
------------------------------

To perform inference with a pre-trained model, first load and transform your data.

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

For more information on performing inference, see
:ref:`End-to-end: Offline Batch Inference <batch_inference_home>`
and :ref:`Stateful Transforms <stateful_transforms>`.

.. _saving_images:

Saving images
-------------

Save images with formats like PNG, Parquet, and NumPy. To view all supported formats,
see the :ref:`Input/Output reference <input-output>`.

.. tab-set::

    .. tab-item:: Images

        To save images as image files, call :meth:`~ray.data.Dataset.write_images`.

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
            ds.write_images("/tmp/simple", column="image", file_format="png")

    .. tab-item:: Parquet

        To save images in Parquet files, call :meth:`~ray.data.Dataset.write_parquet`.

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
            ds.write_parquet("/tmp/simple")


    .. tab-item:: NumPy

        To save images in a NumPy file, call :meth:`~ray.data.Dataset.write_numpy`.

        .. testcode::

            import ray

            ds = ray.data.read_images("s3://anonymous@ray-example-data/image-datasets/simple")
            ds.write_numpy("/tmp/simple", column="image")

For more information on saving data, see :ref:`Saving data <loading_data>`.
