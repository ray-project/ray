ray.data.context.DatasetContext.get_current().use_streaming_executor = True


def get_datasets(
    global_batch_size: int = None, batch_size_per_worker: int = None
) -> dict:
    import numpy as np
    from ray.data.datasource.partitioning import Partitioning

    IMG_SIZE = 224

    def get_dataset_for_split(split: str):
        data_folder = f"s3://anonymous@air-example-data/food-101-tiny/{split}"
        partitioning = Partitioning("dir", field_names=["class"], base_dir=data_folder)

        def resize(batch: dict):
            batch["image"] = tf.convert_to_tensor(batch["image"], dtype=tf.uint8)
            batch["image"] = tf.image.resize(
                batch["image"], (IMG_SIZE, IMG_SIZE)
            ).numpy()
            return batch

        return (
            ray.data.read_images(
                data_folder, size=(512, 512), partitioning=partitioning, mode="RGB"
            )
            .map_batches(resize, batch_format="numpy")
            .random_shuffle()
        )

    datasets = {split: get_dataset_for_split(split) for split in ("train", "valid")}

    labels = datasets["valid"].groupby("class").count().to_pandas()
    class_to_idx = {class_name: i for i, class_name in enumerate(labels["class"])}

    def map_labels(batch: dict) -> dict:
        batch["label"] = np.vectorize(class_to_idx.get)(batch["class"])
        return {"image": batch["image"], "label": batch["label"]}

    datasets = {
        split: dataset.map_batches(map_labels, batch_format="numpy")
        for split, dataset in datasets.items()
    }
    return datasets
