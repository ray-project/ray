import torch
import torch.nn as nn
import torch.optim as optim
from torchvision import models, transforms
import numpy as np
import warnings

import ray
from ray.tune.syncer import SyncConfig
from ray.air.config import ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer, TorchCheckpoint
from ray.data.preprocessors import TorchVisionPreprocessor
from ray.train.torch import TorchCheckpoint
from ray.data.datasource.partitioning import Partitioning
from ray.air import session
import ray.train as train


warnings.filterwarnings("ignore")

# ------------------------
# Data Loading
# ------------------------

ray_datasets = {}
for split in ["train", "valid"]:
    data_folder = f"s3://anonymous@air-example-data/food-101-tiny/{split}"
    partitioning = Partitioning("dir", field_names=["class"], base_dir=data_folder)
    ray_datasets[split] = ray.data.read_images(
        data_folder, size=(256, 256), partitioning=partitioning, mode="RGB"
    )

labels_df = ray_datasets["valid"].groupby("class").count().to_pandas()
class_to_idx = {class_str: i for i, class_str in enumerate(labels_df["class"])}


def map_labels(batch: np.ndarray) -> np.ndarray:
    batch["label"] = [class_to_idx[class_str] for class_str in batch["class"]]
    batch.pop("class")
    return batch


ray_datasets = {split: ds.map_batches(map_labels) for split, ds in ray_datasets.items()}


def build_preprocessor():
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
    # Accelerate image processing with batched transformations
    return TorchVisionPreprocessor(
        columns=["image"], transform=data_transforms, batched=True
    )


# ------------------------
# Training Loop
# ------------------------
def initialize_model():
    # Load pretrained model params
    model = models.efficientnet_b0(pretrained=True)

    # Replace the original classifier with a new Linear layer
    num_features = model.classifier[1].in_features
    model.classifier[1] = nn.Linear(num_features, 10)

    for param in model.parameters():
        param.requires_grad = True
    return model


def evaluate(logits, labels):
    _, preds = torch.max(logits, 1)
    corrects = torch.sum(preds == labels).item()
    return corrects


def train_loop_per_worker(configs):
    warnings.filterwarnings("ignore")
    # Prepare dataloader for each worker
    datasets = dict()
    datasets["train"] = session.get_dataset_shard("train")
    datasets["valid"] = session.get_dataset_shard("valid")

    # Calculate the batch size for a single worker
    worker_batch_size = configs["batch_size"] // session.get_world_size()

    device = train.torch.get_device()

    # Prepare DDP Model, optimizer, and loss function
    model = initialize_model()  # [TODO]

    model = train.torch.prepare_model(model)

    # optimizer = optim.SGD(
    #     model.parameters(), lr=configs["lr"], momentum=configs["momentum"]
    # )
    optimizer = optim.Adam(model.parameters(), lr=configs["lr"])
    criterion = nn.CrossEntropyLoss()

    # Start training loops
    for epoch in range(configs["num_epochs"]):
        # Each epoch has a training and validation phase
        for phase in ["train", "valid"]:
            if phase == "train":
                model.train()  # Set model to training mode
            else:
                model.eval()  # Set model to evaluate mode

            running_loss = 0.0
            running_corrects = 0

            # Create a dataset iterator for the shard on the current worker
            dataset_iterator = datasets[phase].iter_torch_batches(
                batch_size=worker_batch_size, device=device
            )
            for batch in dataset_iterator:
                inputs = batch["image"]
                labels = batch["label"]

                # zero the parameter gradients
                optimizer.zero_grad()

                # forward
                with torch.set_grad_enabled(phase == "train"):
                    # Get model outputs and calculate loss
                    outputs = model(inputs)
                    loss = criterion(outputs, labels)

                    # backward + optimize only if in training phase
                    if phase == "train":
                        loss.backward()
                        optimizer.step()

                # calculate statistics
                running_loss += loss.item() * inputs.size(0)
                running_corrects += evaluate(outputs, labels)

            epoch_loss = running_loss / datasets[phase].count()
            epoch_acc = running_corrects / datasets[phase].count()

            if session.get_world_rank() == 0:
                print(
                    "Epoch {}-{} Loss: {:.4f} Acc: {:.4f}".format(
                        epoch, phase, epoch_loss, epoch_acc
                    )
                )

            # Report metrics and checkpoint every epoch
            if phase == "valid":
                checkpoint = TorchCheckpoint.from_dict(
                    {
                        "epoch": epoch,
                        "model": model.module.state_dict(),
                        "optimizer_state_dict": optimizer.state_dict(),
                    }
                )
                session.report(
                    metrics={"loss": epoch_loss, "acc": epoch_acc},
                    checkpoint=checkpoint,
                )


num_workers = 4

# Scale out model training across 4 workers, each assigned 1 CPU and 1 GPU.
scaling_config = ScalingConfig(
    num_workers=num_workers, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
)

# Save only the latest checkpoint
checkpoint_config = CheckpointConfig(num_to_keep=1)

# Set experiment name and checkpoint configs
run_config = RunConfig(
    name="resnet-hackathon",
    sync_config=SyncConfig(syncer=None),
    checkpoint_config=checkpoint_config,
)

train_loop_config = {
    "input_size": 224,  # Input image size (224 x 224)
    "batch_size": 128,  # Batch size for training
    "num_epochs": 10,  # Number of epochs to train for
    "lr": 0.001,  # Learning Rate
}

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config=train_loop_config,
    scaling_config=scaling_config,
    run_config=run_config,
    datasets=ray_datasets,
    preprocessor=build_preprocessor(),
)

result = trainer.fit()
