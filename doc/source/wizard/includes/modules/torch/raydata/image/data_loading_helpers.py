def get_datasets(
    global_batch_size: int = None, batch_size_per_worker: int = None
) -> dict:
    import numpy as np
    from torchvision import datasets, transforms
    from ray.data.datasource.partitioning import Partitioning

    def get_dataset_for_split(split: str):
        data_folder = f"s3://anonymous@air-example-data/food-101-tiny/{split}"
        partitioning = Partitioning("dir", field_names=["class"], base_dir=data_folder)

        return ray.data.read_images(
            data_folder, size=(256, 256), partitioning=partitioning, mode="RGB"
        ).random_shuffle()

    datasets = {split: get_dataset_for_split(split) for split in ("train", "valid")}

    labels = datasets["valid"].groupby("class").count().to_pandas()
    class_to_idx = {class_name: i for i, class_name in enumerate(labels["class"])}

    def preprocess(batch: dict) -> dict:
        # Convert input image to tensors
        def to_tensor(batch: np.ndarray) -> torch.Tensor:
            tensor = torch.as_tensor(batch, dtype=torch.float)
            # (B, H, W, C) -> (B, C, H, W)
            tensor = tensor.permute(0, 3, 1, 2).contiguous()
            # [0., 255.] -> [0., 1.]
            tensor = tensor.div(255)
            return tensor

        # Normalize the input images
        # Reference: https://huggingface.co/spaces/pytorch/EfficientNet/blob/main/app.py
        data_transforms = transforms.Compose(
            [
                transforms.Lambda(to_tensor),
                transforms.Resize(256),
                transforms.CenterCrop(224),
                transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
            ]
        )
        images = [data_transforms(image) for image in batch["image"]]

        batch["label"] = np.vectorize(class_to_idx.get)(batch["class"])
        return {"image": np.array(images), "label": batch["label"]}

    datasets = {
        split: dataset.map_batches(preprocess, batch_format="numpy")
        for split, dataset in datasets.items()
    }
    return datasets
