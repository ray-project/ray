---
myst:
  html_meta:
    description: "Ray Data is a scalable data processing library for AI workloads, with a streaming engine for batch inference, preprocessing, and ML training ingest across CPUs and GPUs."
---

(data)=

# Ray Data: Scalable data processing for AI workloads

```{toctree}
:hidden:

quickstart
key-concepts
user-guide
examples
contributing/contributing
comparisons
benchmark
internals
```

Ray Data is a scalable data processing library for AI workloads, built on Ray. It provides APIs for common operations such as {ref}`batch inference <batch_inference_home>`, data preprocessing, and data loading for ML training. Unlike other distributed data systems, Ray Data uses a {ref}`streaming execution engine <streaming-execution>` to process large datasets efficiently and keep utilization high across both CPU and GPU workloads.

## Quickstart

To learn more about installing Ray and its libraries, see {ref}`Installing Ray <installation>`. To install Ray Data, run the following command:

```console
$ pip install -U 'ray[data]'
```

The following example runs a batch text classification task with Ray Data:

```{testcode}
import ray
import pandas as pd

class ClassificationModel:
    def __init__(self):
        from transformers import pipeline
        self.pipe = pipeline("text-classification")

    def __call__(self, batch: pd.DataFrame):
        results = self.pipe(list(batch["text"]))
        result_df = pd.DataFrame(results)
        return pd.concat([batch, result_df], axis=1)

ds = ray.data.read_text("s3://anonymous@ray-example-data/sms_spam_collection_subset.txt")
ds = ds.map_batches(
    ClassificationModel,
    compute=ray.data.ActorPoolStrategy(size=2),
    batch_size=64,
    batch_format="pandas"
    # num_gpus=1  # this will set 1 GPU per worker
)
ds.show(limit=1)
```

```{testoutput}
:options: +MOCK

{'text': 'ham\tGo until jurong point, crazy.. Available only in bugis n great world la e buffet... Cine there got amore wat...', 'label': 'NEGATIVE', 'score': 0.9935141801834106}
```

## Why choose Ray Data?

AI workloads revolve around deep learning models, which are computationally intensive and often require specialized hardware such as GPUs. Unlike CPUs, GPUs often have less memory, different scheduling semantics, and a much higher cost to run. Systems built for traditional data processing pipelines often don't use these resources well.

Ray Data treats AI workloads as a first-class use case and offers four advantages:

- **Faster and cheaper for deep learning**: Ray Data streams data between CPU preprocessing tasks and GPU inference or training tasks. Keeping GPUs active maximizes resource utilization and reduces costs.
- **Framework friendly**: Ray Data integrates with common AI frameworks such as vLLM, PyTorch, Hugging Face, and TensorFlow, and with common cloud providers such as AWS, GCP, and Azure.
- **Support for multi-modal data**: Ray Data uses Apache Arrow and pandas and supports many data formats used in ML workloads, such as Parquet, Lance, images, JSON, CSV, audio, and video.
- **Scalable by default**: Ray Data builds on Ray to scale automatically across heterogeneous clusters of CPU and GPU machines. The same code runs unchanged on one machine or on hundreds of nodes processing hundreds of TB of data.

% https://docs.google.com/drawings/d/16AwJeBNR46_TsrkOmMbGaBK7u-OPsf_V8fHjU-d2PPQ/edit

## Learn more

::::{grid} 1 2 2 2
:gutter: 1
:class-container: container pb-5

:::{grid-item-card}
**Quickstart**
^^^

Run a basic example to get started with Ray Data.

+++
```{button-ref} data_quickstart
:color: primary
:outline:
:expand:

Quickstart
```
:::

:::{grid-item-card}
**Key concepts**
^^^

Learn the key concepts behind Ray Data, including what Datasets are and how to use them.

+++
```{button-ref} data_key_concepts
:color: primary
:outline:
:expand:

Key concepts
```
:::

:::{grid-item-card}
**User guides**
^^^

Learn how to use Ray Data, from basic usage to end-to-end guides.

+++
```{button-ref} data_user_guide
:color: primary
:outline:
:expand:

Learn how to use Ray Data
```
:::

:::{grid-item-card}
**Examples**
^^^

Find basic and scaled-out examples of Ray Data workloads.

+++
```{button-ref} examples
:color: primary
:outline:
:expand:

Ray Data examples
```
:::

:::{grid-item-card}
**API**
^^^

Get in-depth information about the Ray Data API.

+++
```{button-ref} data-api
:color: primary
:outline:
:expand:

Read the API reference
```
:::
::::

(case-studies-for-ray-data)=

## Case studies

The following case studies use Ray Data for training ingest:

- [Pinterest uses Ray Data to do last mile data processing for model training](https://medium.com/pinterest-engineering/last-mile-data-processing-with-ray-629affbf34ff).
- [DoorDash elevates model training with Ray Data](https://www.youtube.com/watch?v=pzemMnpctVY).
- [Instacart builds distributed machine learning model training on Ray Data](https://tech.instacart.com/distributed-machine-learning-at-instacart-4b11d7569423).

The following case studies use Ray Data for batch inference:

- [ByteDance scales offline inference with multi-modal LLMs to 200 TB on Ray Data](https://www.anyscale.com/blog/how-bytedance-scales-offline-inference-with-multi-modal-llms-to-200TB-data).
- [Spotify's ML platform built on Ray Data for batch inference](https://engineering.atspotify.com/2023/02/unleashing-ml-innovation-at-spotify-with-ray/).
- [Sewer AI speeds up object detection on videos 3x using Ray Data](https://www.anyscale.com/blog/inspecting-sewer-line-safety-using-thousands-of-hours-of-video).
