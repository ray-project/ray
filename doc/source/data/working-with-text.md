---
myst:
  html_meta:
    description: "Read, transform, run inference on, and save large text datasets with Ray Data."
---

# Working with text

Use Ray Data to read and transform large amounts of text data.

This guide shows you how to do the following:

* {ref}`Read text files <reading-text-files>`.
* {ref}`Transform text data <transforming-text>`.
* {ref}`Perform inference on text data <performing-inference-on-text>`.
* {ref}`Save text data <saving-text>`.

(reading-text-files)=

## Read text files

Ray Data reads lines of text and JSON Lines files. For other text formats, read the raw binary files and decode the data yourself.

::::{tab-set}

:::{tab-item} Text lines

To read lines of text, call {func}`~ray.data.read_text`. Ray Data creates a row for each line of text. The column name in the schema defaults to `text`.

```{testcode}
import ray

ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")

ds.show(3)
```

```{testoutput}
{'text': 'The Zen of Python, by Tim Peters'}
{'text': 'Beautiful is better than ugly.'}
{'text': 'Explicit is better than implicit.'}
```

:::

:::{tab-item} JSON Lines

[JSON Lines](https://jsonlines.org/) is a text format for structured data. It's typically used to process data one record at a time.

To read JSON Lines files, call {func}`~ray.data.read_json`. Ray Data creates a row for each JSON object.

```{testcode}
import ray

ds = ray.data.read_json("s3://anonymous@ray-example-data/logs.json")

ds.show(3)
```

```{testoutput}
{'timestamp': datetime.datetime(2022, 2, 8, 15, 43, 41), 'size': 48261360}
{'timestamp': datetime.datetime(2011, 12, 29, 0, 19, 10), 'size': 519523}
{'timestamp': datetime.datetime(2028, 9, 9, 5, 6, 7), 'size': 2163626}
```

:::

:::{tab-item} Other formats

To read other text formats, call {func}`~ray.data.read_binary_files`. Then call {meth}`~ray.data.Dataset.map` to decode your data.

```{testcode}
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
```

```{testoutput}
{'text': 'Batoidea\nBatoidea is a superorder of cartilaginous fishes...'}
```

:::

::::

For more information on reading files, see {ref}`Loading data <loading_data>`.

(transforming-text)=

## Transform text

To transform text, implement your transformation in a function or callable class. Then call {meth}`Dataset.map() <ray.data.Dataset.map>` or {meth}`Dataset.map_batches() <ray.data.Dataset.map_batches>`. Ray Data transforms your text in parallel.

```{testcode}
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
```

```{testoutput}
{'text': 'the zen of python, by tim peters'}
{'text': 'beautiful is better than ugly.'}
{'text': 'explicit is better than implicit.'}
```

For more information on transforming data, see {ref}`Transforming data <transforming_data>`.

(performing-inference-on-text)=

## Perform inference on text

To perform inference on text data with a pre-trained model, implement a callable class that sets up and invokes the model. Then call {meth}`Dataset.map_batches() <ray.data.Dataset.map_batches>`.

```{testcode}
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
    .map_batches(TextClassifier, compute=ray.data.ActorPoolStrategy(size=2), batch_size="auto")
)

ds.show(3)
```

```{testoutput}
{'text': 'The Zen of Python, by Tim Peters', 'label': 'POSITIVE'}
{'text': 'Beautiful is better than ugly.', 'label': 'POSITIVE'}
{'text': 'Explicit is better than implicit.', 'label': 'POSITIVE'}
```

For more information on working with large language models, see {ref}`Working with LLMs <working-with-llms>`.

For more information on performing inference, see {ref}`End-to-end: Offline Batch Inference <batch_inference_home>` and {ref}`Stateful transforms <stateful_transforms>`.

(saving-text)=

## Save text

To save text, call a method such as {meth}`~ray.data.Dataset.write_parquet`. Ray Data can save text in many formats.

For the full list of supported file formats, see the {ref}`Saving Data API <saving-data-api>`.

```{testcode}
:skipif: True

import ray

ds = ray.data.read_text("s3://anonymous@ray-example-data/this.txt")

ds.write_parquet("s3://my-bucket/results")
```

For more information on saving data, see {ref}`Saving data <saving-data>`.
