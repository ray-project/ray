import os
import numpy as np

os.environ['RAY_ML_DEV'] = "1"

import ray
from ray.train.lightning import LightningPredictor, LightningCheckpoint
from ptl_tests.utils import LightningMNISTClassifier, MNISTDataModule, LightningMNISTModelConfig
from ray.air import Checkpoint
from ray.train.batch_predictor import BatchPredictor
from torchvision.datasets import MNIST
from torchvision import transforms

ckpt_path = "/mnt/cluster_storage/ray_lightning_results/ptl-e2e-classifier/LightningTrainer_89272_00000_0_2023-02-28_14-30-48/checkpoint_000009"
checkpoint_local = LightningCheckpoint.from_directory(ckpt_path)

checkpoint_uri = "s3://anyscale-yunxuanx-demo/checkpoint_000008/"
checkpoint_s3 = LightningCheckpoint.from_uri(checkpoint_uri)

def test_predictor(checkpoint, dataloader, use_gpu=False):
    model_init_config = {"config": LightningMNISTModelConfig}
    predictor = LightningPredictor.from_checkpoint(
        checkpoint=checkpoint, model=LightningMNISTClassifier, model_init_config=model_init_config, use_gpu=use_gpu)
    batch = next(iter(dataloader))
    input_batch = batch[0].numpy()
    print(predictor.predict(input_batch))

def test_batch_predictor(checkpoint):
    model_init_config = {"config": LightningMNISTModelConfig}
    batch_predictor = BatchPredictor(checkpoint, LightningPredictor, use_gpu=True, model=LightningMNISTClassifier, model_init_config=model_init_config)

    transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.1307,), (0.3081,))
        ])

    test_dataset = MNIST(os.getcwd(), train=False, download=True, transform=transform)
    ds = ray.data.from_torch(test_dataset)

    def convert_batch_to_numpy(batch):
        images = np.stack([np.array(image) for image, _ in batch])
        labels = np.array([label for _, label in batch])
        return {"image": images, "label": labels}

    ds = ds.map_batches(convert_batch_to_numpy)

    results = batch_predictor.predict(
        ds,
        feature_columns=["image"],
        keep_columns=["label"],
        batch_size=128,
        min_scoring_workers=4,
        max_scoring_workers=4,
        num_gpus_per_worker=1,
    )

    def convert_logits_to_classes(batch):
        batch["correct"] = batch["predictions"] == batch["label"]
        return batch

    predictions = results.map_batches(convert_logits_to_classes, batch_format="numpy")
    predictions.show(1)

    print("Evaluation Accuracy = ", predictions.mean(on="correct"))


if __name__ == "__main__":
    datamodule = MNISTDataModule(batch_size=10)
    datamodule.setup()
    dataloader = datamodule.val_dataloader()

    test_predictor(checkpoint_local, dataloader, use_gpu=False)
    test_predictor(checkpoint_s3, dataloader, use_gpu=False)

    test_predictor(checkpoint_local, dataloader, use_gpu=True)
    test_predictor(checkpoint_s3, dataloader, use_gpu=True)

    test_batch_predictor(checkpoint_local)


    