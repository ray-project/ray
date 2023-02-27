def main():
    test(framework="torch", datasource="tfrecords")


def test(*, framework: str, datasource: str):
    assert framework in {"torch", "tensorflow"}
    assert datasource in {"tfrecords", "images", "numpy", "parquet"}

    if datasource == "tfrecords":
        dataset = read_tfrecords()
    if datasource == "images":
        dataset = read_images()
    if datasource == "numpy":
        dataset = read_numpy()
    if datasource == "parquet":
        dataset = read_parquet()

    if framework == "torch":
        preprocessor, per_epoch_preprocessor = create_torch_preprocessors()
        train_torch_model(dataset, preprocessor, per_epoch_preprocessor)
        checkpoint = create_torch_checkpoint(preprocessor)
        batch_predict_torch(dataset, checkpoint)
        online_predict_torch(checkpoint)


def read_tfrecords():
    # __read_tfrecords_start__
    import io
    from typing import Dict

    import numpy as np
    from PIL import Image
    import ray

    dataset = ray.data.read_tfrecords(
        "s3://anonymous@air-example-data/cifar-10/tfrecords"
    )

    def decode_bytes(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        images = []
        for data in batch["image"]:
            image = Image.open(io.BytesIO(data))
            images.append(np.array(image))
        batch["image"] = np.array(images)
        return batch

    dataset = dataset.map_batches(decode_bytes, batch_format="numpy")
    # __read_tfrecords_stop__
    return dataset


def read_numpy():
    # __read_numpy_start__
    import ray

    images = ray.data.read_numpy("s3://anonymous@air-example-data/cifar-10/images.npy")
    labels = ray.data.read_numpy("s3://anonymous@air-example-data/cifar-10/labels.npy")
    dataset = images.zip(labels)
    dataset = dataset.map_batches(
        lambda batch: batch.rename(
            columns={"__value__": "image", "__value___1": "label"}
        )
    )
    # __read_numpy_stop__
    return dataset


def read_parquet():
    # __read_parquet_start__
    import ray

    dataset = ray.data.read_parquet("s3://anonymous@air-example-data/cifar-10/parquet")
    # __read_parquet_stop__
    return dataset


def read_images():
    # __read_images_start__
    import os
    from typing import Dict

    import numpy as np

    import ray

    dataset = ray.data.read_images(
        "s3://anonymous@air-example-data/cifar-10/images",
        include_paths=True,
    )

    def add_class_column(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        classes = []
        for path in batch["path"]:
            # Paths look like '../tiny-imagenet/train/n01443537/n01443537_0.JPEG'
            path = os.path.normpath(path)
            parts = path.split(os.sep)
            classes.append(parts[-2])
        batch["class"] = np.array(classes)
        return batch

    def remove_path_column(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
        del batch["path"]
        return batch

    dataset = dataset.map_batches(add_class_column).map_batches(remove_path_column)
    # __read_images_stop__
    return dataset


def create_torch_preprocessors():
    # __torch_preprocessors_start__
    from torchvision import transforms

    from ray.data.preprocessors import TorchVisionPreprocessor

    transform = transforms.Compose([transforms.ToTensor(), transforms.CenterCrop(224)])
    preprocessor = TorchVisionPreprocessor(columns=["image"], transform=transform)

    per_epoch_transform = transforms.RandomHorizontalFlip(p=0.5)
    per_epoch_preprocessor = TorchVisionPreprocessor(
        columns=["image"], transform=per_epoch_transform
    )
    # __torch_preprocessors_stop__
    return preprocessor, per_epoch_preprocessor


def train_torch_model(dataset, preprocessor, per_epoch_preprocessor):
    # __torch_trainer_start__
    import torch.nn as nn
    import torch.optim as optim
    from torchvision import models

    from ray import train
    from ray.train.torch import TorchTrainer, TorchCheckpoint
    from ray.air import session
    from ray.air.config import DatasetConfig, ScalingConfig

    def train_one_epoch(model, *, criterion, optimizer, batch_size, epoch):
        dataset_shard = session.get_dataset_shard("train")

        running_loss = 0
        for i, batch in enumerate(
            dataset_shard.iter_torch_batches(
                batch_size=batch_size, local_shuffle_buffer_size=256
            )
        ):
            inputs, labels = batch["image"], batch["label"]

            outputs = model(inputs)
            loss = criterion(outputs, labels)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            running_loss += loss.item()
            if i & 2000 == 1999:
                session.report(
                    metrics={
                        "epoch": epoch,
                        "batch": i,
                        "running_loss": running_loss / 2000,
                    },
                    checkpoint=TorchCheckpoint.from_model(model),
                )
                running_loss = 0

    def train_loop_per_worker(config):
        model = train.torch.prepare_model(models.resnet50())
        criterion = nn.CrossEntropyLoss()
        optimizer = optim.SGD(model.parameters(), lr=config["lr"])

        for epoch in range(config["epochs"]):
            train_one_epoch(
                model,
                criterion=criterion,
                optimizer=optimizer,
                batch_size=config["batch_size"],
                epoch=epoch,
            )

    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config={"batch_size": 32, "lr": 0.02, "epochs": 1},
        datasets={"train": dataset},
        dataset_config={
            "train": DatasetConfig(per_epoch_preprocessor=per_epoch_preprocessor)
        },
        scaling_config=ScalingConfig(num_workers=2),
        preprocessor=preprocessor,
    )
    results = trainer.fit()
    # __torch_trainer_stop__
    return results


def create_torch_checkpoint(preprocessor):
    # __torch_checkpoint_start__
    from torchvision import models

    from ray.train.torch import TorchCheckpoint

    model = models.resnet50(pretrained=True)
    checkpoint = TorchCheckpoint.from_model(model, preprocessor=preprocessor)
    # __torch_checkpoint_stop__
    return checkpoint


def batch_predict_torch(dataset, checkpoint):
    # __torch_batch_predictor_start__
    from ray.train.batch_predictor import BatchPredictor
    from ray.train.torch import TorchPredictor

    predictor = BatchPredictor.from_checkpoint(checkpoint, TorchPredictor)
    predictor.predict(dataset, feature_columns=["image"], keep_columns=["label"])
    # __torch_batch_predictor_stop__


def online_predict_torch(checkpoint):
    # __torch_serve_start__
    from ray import serve
    from ray.serve import PredictorDeployment
    from ray.serve.http_adapters import json_to_ndarray
    from ray.train.torch import TorchPredictor

    serve.run(
        PredictorDeployment.bind(
            TorchPredictor,
            checkpoint,
            http_adapter=json_to_ndarray,
        )
    )

    from io import BytesIO

    from PIL import Image
    import requests
    import numpy as np

    response = requests.get("http://placekitten.com/200/300")
    image = Image.open(BytesIO(response.content))

    payload = {"array": np.array(image).tolist(), "dtype": "float32"}
    response = requests.post("http://localhost:8000/", json=payload)
    predictions = response.json()
    # __torch_serve_stop__


if __name__ == "__main__":
    main()
