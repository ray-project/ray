Working with Text
=================

With Ray Data, you can easily read and transform large amounts of text data.

This guide shows you how to:

* :ref:`Read text files <reading-text-files>`
* :ref:`Transform text data <transforming-text>`
* :ref:`Perform inference on text data <performing-inference-on-text>`
* :ref:`Save text data <saving-text>`

.. _reading-text-files:

Reading text files
------------------

Ray Data can read lines of text and JSONL. Alternatively, you can read raw binary
files and manually decode data.

.. tab-set::

    .. tab-item:: Text lines

        To read lines of text, call :func:`~ray.data.read_text`. Ray Data creates a
        row for each line of text.

        .. testcode::

            import ray

            ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")

            ds.show(3)

        .. testoutput::

            {'text': 'The Zen of Python, by Tim Peters'}
            {'text': 'Beautiful is better than ugly.'}
            {'text': 'Explicit is better than implicit.'}

    .. tab-item:: JSON Lines

        `JSON Lines <https://jsonlines.org/>`_ is a text format for structured data.
        It's typically used to process data one record at a time.

        To read JSON Lines files, call :func:`~ray.data.read_json`. Ray Data creates a
        row for each JSON object.

        .. testcode::

            import ray

            ds = ray.data.read_json("s3://anonymous@ray-example-data/logs.json")

            ds.show(3)

        .. testoutput::

            {'timestamp': datetime.datetime(2022, 2, 8, 15, 43, 41), 'size': 48261360}
            {'timestamp': datetime.datetime(2011, 12, 29, 0, 19, 10), 'size': 519523}
            {'timestamp': datetime.datetime(2028, 9, 9, 5, 6, 7), 'size': 2163626}


    .. tab-item:: Other formats

        To read other text formats, call :func:`~ray.data.read_binary_files`. Then,
        call :meth:`~ray.data.Dataset.map` to decode your data.

        .. testcode::

            from typing import Any, Dict
            from bs4 import BeautifulSoup
            import ray

            def parse_html(row: Dict[str, Any]) -> Dict[str, Any]:
                html = row["bytes"].decode("utf-8")
                soup = BeautifulSoup(html, features="html.parser")
                return {"text": soup.get_text().strip()}

            ds = (
                ray.data.read_binary_files("s3://anonymous@ray-example-data/index.html")
                .map(parse_html)
            )

            ds.show()

        .. testoutput::

            {'text': 'Batoidea\nBatoidea is a superorder of cartilaginous fishes...'}

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

For more information on transforming data, see
:ref:`Transforming data <transforming_data>`.

.. _performing-inference-on-text:

Performing inference on text
----------------------------

To perform inference with a pre-trained model on text data, implement a callable class
that sets up and invokes a model. Then, call
:meth:`Dataset.map_batches() <ray.data.Dataset.map_batches>`.

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
:ref:`End-to-end: Offline Batch Inference <batch_inference_home>`
and :ref:`Stateful Transforms <stateful_transforms>`.

.. _saving-text:

Saving text
-----------

To save text, call a method like :meth:`~ray.data.Dataset.write_parquet`. Ray Data can
save text in many formats.

To view the full list of supported file formats, see the
:ref:`Input/Output reference <input-output>`.

.. testcode::

    import ray

    ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")

    ds.write_parquet("local:///tmp/results")

For more information on saving data, see :ref:`Saving data <saving-data>`.
