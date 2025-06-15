.. _data:

==========================================
Ray Data: Data Processing for AI Workloads
==========================================

.. toctree::
    :hidden:

    quickstart
    key-concepts
    user-guide
    examples
    api/api
    comparisons
    data-internals

Ray Data is a data processing library for AI workloads.

Modern data-intensive AI workloads revolve around the usage of deep learning models, which are computationally intensive and often require specialized hardware such as GPUs. This results in workloads that have data formats that are mixed between *tabular* and *tensor* and are *GPU-bound*, which are not well supported by traditional data processing systems.

Ray Data aims to bridge the gap between the needs of AI workloads and the capabilities of existing data processing systems by providing:

- key primitives for efficient GPU batch inference, distributed training ingestion, and data preprocessing
- support for many data formats used in Data and AI workloads (Iceberg, Parquet, Lance, images, audio, video, etc.)
- integration with common AI frameworks (vLLM, Pytorch, Tensorflow, etc)
- support for distributed data processing (map, filter, join, groupby, aggregate, etc.)

Quickstart
----------

To install Ray Data, run:

.. code-block:: console

    $ pip install -U 'ray[data]'


Ray Data is built to target data-intensive AI workloads, such as batch inference, distributed training ingestion, and data preprocessing.

.. tab-set::

    .. tab-item:: Batch Inference
        :sync: Batch Inference

        .. testcode::

            from typing import Dict
            import numpy as np

            import ray

            ds = ray.data.read_json(...)

            class HuggingFacePredictor:
                def __init__(self):
                    from transformers import pipeline
                    # Initialize a pre-trained GPT2 Huggingface pipeline.
                    self.model = pipeline("text-generation", model="gpt2")

                # Logic for inference on 1 batch of data.
                def __call__(self, batch: Dict[str, np.ndarray]) -> Dict[str, list]:
                    predictions = self.model(list(batch["data"]), max_length=20, num_return_sequences=1)
                    # [[{'generated_text': 'output_1'}], ..., [{'generated_text': 'output_2'}]]

                    batch["output"] = [sequences[0]["generated_text"] for sequences in predictions]
                    # ['output_1', 'output_2']
                    return batch

            predictions = ds.map_batches(HuggingFacePredictor, concurrency=2, num_gpus=1)
            predictions.show(limit=1)

            # {'data': 'Complete this', 'output': 'Complete this information or purchase any item from this site.\n\nAll purchases are final and non-'}

        See :ref:`batch_inference_home` for more details.

    .. tab-item:: Distributed Training Ingestion
        :sync: Training Ingestion

        .. testcode::

            import ray
            from ray import train
            from ray.data.datasource.partitioning import Partitioning
            from ray.train import ScalingConfig
            from ray.train.torch import TorchTrainer
            import torch
            from torchvision import models, transforms


            # Training function
            def train_loop_per_worker(config):
                train_loader = train.get_dataset_shard("train").iter_torch_batches(
                    batch_size=config.get("batch_size", 32),
                    dtypes={"image": torch.float32, "label": torch.int64},
                    device="auto"
                )

                model = train.torch.prepare_model(
                    models.resnet50(pretrained=False, num_classes=config["num_classes"])
                )
                loss_fn = torch.nn.CrossEntropyLoss()
                optimizer = torch.optim.SGD(model.parameters(), lr=config["lr"])
                normalize = transforms.Normalize(mean=[0.485, 0.456, 0.406],
                                                std=[0.229, 0.224, 0.225])

                for epoch in range(config["num_epochs"]):
                    total_loss, count = 0.0, 0
                    for batch in train_loader:
                        images = normalize((batch["image"].permute(0, 3, 1, 2) / 255.0).contiguous())
                        labels = batch["label"]
                        loss = loss_fn(model(images), labels)

                        optimizer.zero_grad()
                        loss.backward()
                        optimizer.step()

                        total_loss += loss.item()
                        count += 1

                    train.report({"epoch": epoch + 1, "loss": total_loss / count if count else 0.0})

            data_path = "s3://anonymous@air-example-data-2/imagenette2/train/"

            dataset = ray.data.read_images(
                data_path,
                mode="RGB",
                size=(224, 224),
                include_paths=False,
                partitioning=Partitioning(
                    "dir",
                    field_names=["label"],
                    base_dir=data_path
                )
            )

            # Encode labels
            label_mapping = {
                label: idx for idx, label in enumerate(sorted(dataset.unique("label")))}
            dataset = dataset.map(
                lambda r: {"image": r["image"], "label": label_mapping[r["label"]]})

            # Configure and execute distributed training
            trainer = TorchTrainer(
                train_loop_per_worker=train_loop_per_worker,
                train_loop_config={
                    "lr": 1e-3,
                    "batch_size": 64,
                    "num_epochs": 5,
                    "num_classes": len(label_mapping)
                },
                scaling_config=ScalingConfig(
                    num_workers=2,
                    use_gpu=True),
                datasets={"train": dataset}
            )
            result = trainer.fit()

            print("Training finished. Final loss:", result.metrics.get("loss"))

        See :ref:`data-ingest-torch` for more details.

    .. tab-item:: Data Preprocessing
        :sync: Data Preprocessing

        .. testcode::

            import ray
            from pathlib import Path

            movielens = Path("s3://air-example-data/movielens-25m/clean_featurized_data/")

            users = ray.data.read_parquet(movielens / "users.parquet")
            movies = ray.data.read_parquet(movielens / "movies.parquet")
            ratings = ray.data.read_parquet(movielens / "ratings.parquet")

            users = users.drop_columns(["rating_avg", "rating_count"])

            # Perform joins across datasets
            users_with_ratings = users.join(ratings, num_partitions=20, on=("userId",), join_type="inner")
            all_combined = users_with_ratings.join(movies, num_partitions=20, on=("movieId",), join_type="inner")

            # Calculate the mean rating of the dataset
            mean_rating = all_combined.mean("rating")
            print(mean_rating)

            # Calculate the standard deviation of the rating
            std_rating = all_combined.std("rating")
            print(std_rating)

            # Normalize the rating
            all_combined = all_combined.map(lambda r: {"rating": (r["rating"] - mean_rating) / std_rating})

            all_combined.show(limit=10)

        Ray Data also offers prebuilt scalable implementations of common preprocessing operations. See :ref:`Data Preprocessors <preprocessor-ref>` for more details.



Learn more
----------

.. grid:: 1 2 2 2
    :gutter: 1
    :class-container: container pb-5

    .. grid-item-card::

        **Quickstart**
        ^^^

        Get started with Ray Data with a simple example.

        +++
        .. button-ref:: data_quickstart
            :color: primary
            :outline:
            :expand:

            Quickstart

    .. grid-item-card::

        **Key Concepts**
        ^^^

        Learn the key concepts behind Ray Data. Learn what
        Datasets are and how they're used.

        +++
        .. button-ref:: data_key_concepts
            :color: primary
            :outline:
            :expand:

            Key Concepts

    .. grid-item-card::

        **User Guides**
        ^^^

        Learn how to use Ray Data, from basic usage to end-to-end guides.

        +++
        .. button-ref:: data_user_guide
            :color: primary
            :outline:
            :expand:

            Learn how to use Ray Data

    .. grid-item-card::

        **Examples**
        ^^^

        Find both simple and scaling-out examples of using Ray Data.

        +++
        .. button-ref:: examples
            :color: primary
            :outline:
            :expand:

            Ray Data Examples

    .. grid-item-card::

        **API**
        ^^^

        Get more in-depth information about the Ray Data API.

        +++
        .. button-ref:: data-api
            :color: primary
            :outline:
            :expand:

            Read the API Reference


Case studies for Ray Data
-------------------------

**Training ingest using Ray Data**

- `Pinterest uses Ray Data to do last mile data processing for model training <https://medium.com/pinterest-engineering/last-mile-data-processing-with-ray-629affbf34ff>`_
- `DoorDash elevates model training with Ray Data <https://www.youtube.com/watch?v=pzemMnpctVY>`_
- `Instacart builds distributed machine learning model training on Ray Data <https://tech.instacart.com/distributed-machine-learning-at-instacart-4b11d7569423>`_
- `Predibase speeds up image augmentation for model training using Ray Data <https://predibase.com/blog/ludwig-v0-7-fine-tuning-pretrained-image-and-text-models-50x-faster-and>`_

**Batch inference using Ray Data**

- `ByteDance scales offline inference with multi-modal LLMs to 200 TB on Ray Data <https://www.anyscale.com/blog/how-bytedance-scales-offline-inference-with-multi-modal-llms-to-200TB-data>`_
- `Spotify's new ML platform built on Ray Data for batch inference <https://engineering.atspotify.com/2023/02/unleashing-ml-innovation-at-spotify-with-ray/>`_
- `Sewer AI speeds up object detection on videos 3x using Ray Data <https://www.anyscale.com/blog/inspecting-sewer-line-safety-using-thousands-of-hours-of-video>`_
