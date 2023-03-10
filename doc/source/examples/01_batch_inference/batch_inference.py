import torch
import numpy as np
from torchvision import transforms
from typing import Dict

import ray
from ray.data.datasource.partitioning import Partitioning
from ray.data.preprocessors import BatchMapper
from ray.train.batch_predictor import BatchPredictor

# Replace this with your own framework/custom checkpoint/predictors!
from ray.train.torch import TorchCheckpoint, TorchPredictor


# 1. Load your own data with Ray Data!
# Start
s3_uri = "s3://anonymous@air-example-data-2/imagenette2/val/"
partitioning = Partitioning("dir", field_names=["class"], base_dir=s3_uri)
ds = ray.data.read_images(
    s3_uri, size=(256, 256), partitioning=partitioning, mode="RGB"
)
# End


# 2. Replace this with your own custom preprocessing logic!


def preprocess(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    # Start
    def to_tensor(batch: np.ndarray) -> torch.Tensor:
        tensor = torch.as_tensor(batch, dtype=torch.float)
        # (B, H, W, C) -> (B, C, H, W)
        tensor = tensor.permute(0, 3, 1, 2).contiguous()
        # [0., 255.] -> [0., 1.]
        tensor = tensor.div(255)
        return tensor

    transform = transforms.Compose(
        [
            transforms.Lambda(to_tensor),
            transforms.CenterCrop(224),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    return {"image": transform(batch["image"]).numpy()}
    # End


preprocessor = BatchMapper(fn=preprocess, batch_format="numpy")

# 3. Replace these with the framework of your choice!
# Start
predictor_cls = TorchPredictor
checkpoint_cls = TorchCheckpoint
# End

# 4. Replace this with building and loading your own model!


def build_model_checkpoint():
    # Start
    from torchvision import models

    # Load the pretrained resnet model and construct a checkpoint
    model = models.resnet152(pretrained=True)
    checkpoint = checkpoint_cls.from_model(model=model, preprocessor=preprocessor)
    return checkpoint
    # End


checkpoint = build_model_checkpoint()

# 5. Finally, build the BatchPredictor and perform GPU batch prediction!
batch_predictor = BatchPredictor(checkpoint=checkpoint, predictor_cls=predictor_cls)
predictions = batch_predictor.predict(
    ds,
    feature_columns=["image"],
    batch_size=128,
    min_scoring_workers=4,
    max_scoring_workers=4,
    num_gpus_per_worker=1,
)

# 6. Save predictions to our local filesystem (sharded between multiple files)
num_shards = 3
predictions.repartition(num_shards).write_parquet("local:///tmp/predictions")

print(predictions.take(2))
print("Predictions saved to `/tmp/predictions`!")
