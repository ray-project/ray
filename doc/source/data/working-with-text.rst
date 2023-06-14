Working with Text
=================

Text data is ubiquitous in use cases like Natural Language Processing.

This guide shows you how to:

* :ref:`Read text files <reading-text-files>`
* :ref:`Transform text <transforming-text>`
* :ref:`Perform inference on text <performing-inference-on-text>`
* :ref:`Save text <saving-text>`

.. _reading-text-files:

Reading text files
------------------

To read text files, call :func:`~ray.data.read_text`. Ray Data creates a row for each
line of text.

.. testcode::

    import ray

    ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")

    ds.show(3)

.. testoutput::

    {'text': 'The Zen of Python, by Tim Peters'}
    {'text': 'Beautiful is better than ugly.'}
    {'text': 'Explicit is better than implicit.'}

For more information on reading files, see :ref:`Loading data <loading_data>`.

.. _transforming-text:

Transforming text
-----------------

To transform text, implement your transformation in a function or callable class. Then,
call :meth:`Dataset.map() <ray.data.Dataset.map>` or
:meth:`Dataset.map_batches() <ray.data.Dataset.map_batches>`. Ray Data transforms your
text in parallel.

.. testcode::

    from typing import Any, Dict
    import ray

    def to_lower(row: Dict[str, Any]) -> Dict[str, Any]:
        row["text"] = row["text"].lower()
        return row

    ds = (
        ray.data.read_text("s3://anonymous@ray-example-data/this.txt")
        .map(to_lower)
    )

    ds.show(3)

.. testoutput::

    {'text': 'the zen of python, by tim peters'}
    {'text': 'beautiful is better than ugly.'}
    {'text': 'explicit is better than implicit.'}

For more information on transforming data, see `Transforming data <transforming_data>`_.

.. _performing-inference-on-text:

Performing inference on text
----------------------------

To perform inference on text, implement a callable class that sets up and invokes a
model. Then, call :meth:`Dataset.map_batches() <ray.data.Dataset.map_batches>`.

.. testcode::

    from typing import Dict

    import numpy as np
    from transformers import pipeline

    import ray

    class TextClassifier:
        def __init__(self):

            self.model = pipeline("text-classification")

        def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, list]:
            predictions = self.model(list(batch["text"]))
            batch["label"] = [prediction["label"] for prediction in predictions]
            return batch

    ds = (
        ray.data.read_text("s3://anonymous@ray-example-data/this.txt")
        .map_batches(TextClassifier, compute=ray.data.ActorPoolStrategy(size=2))
    )

    ds.show(3)

.. testoutput::

    {'text': 'The Zen of Python, by Tim Peters', 'label': 'POSITIVE'}
    {'text': 'Beautiful is better than ugly.', 'label': 'POSITIVE'}
    {'text': 'Explicit is better than implicit.', 'label': 'POSITIVE'}

For more information on performing inference, see
`End-to-end: Offline Batch Inference <batch_inference_home>`_
and `Transforming batches with actors <transforming_data_actors>`_.

.. _saving-text:

Saving text
-----------

To save text, call a method like :meth:`~ray.data.Dataset.write_parquet`. Ray Data can
save text in many formats.

.. testcode::

    import ray

    ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")

    ds.write_parquet("local:///tmp/results")

For more information on saving data, see `Saving data <saving_data>`_.
