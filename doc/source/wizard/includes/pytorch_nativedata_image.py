import os
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import DataLoader
from torchvision import datasets, models, transforms

import boto3
from pathlib import Path

from ray.tune.syncer import SyncConfig
from ray.air.config import ScalingConfig, RunConfig, CheckpointConfig
from ray.train.torch import TorchTrainer
from ray.train.torch import TorchCheckpoint
from ray.air import session
import ray.train as train

import warnings

warnings.filterwarnings("ignore")

# ------------------------
# Data Loading
# ------------------------


def download_dataset_from_s3(destination_dir: str):
    destination_path = Path(destination_dir).resolve()
    s3_resource = boto3.resource('s3')
    bucket = s3_resource.Bucket("air-example-data")
    for obj in bucket.objects.filter(Prefix="food-101-tiny"):
        os.makedirs(os.path.dirname(obj.key), exist_ok=True)
        download_path = str(destination_path / obj.key)
        if not os.path.exists(download_path):
            bucket.download_file(obj.key, download_path)


def build_datasets():
    download_dataset_from_s3(destination_dir=".")

    data_transforms = transforms.Compose(
        [
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.ToTensor(),
            transforms.Normalize([0.485, 0.456, 0.406], [0.229, 0.224, 0.225]),
        ]
    )

    torch_datasets = {}
    for split in ["train", "valid"]:
        torch_datasets[split] = datasets.ImageFolder(
            os.path.join("./food-101-tiny", split), data_transforms
        )
    return torch_datasets


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

    worker_batch_size = configs["batch_size"] // session.get_world_size()

    torch_datasets = build_datasets()

    dataloaders = dict()
    for split, dataset in torch_datasets.items():
        dataloader = DataLoader(
            dataset=dataset, batch_size=worker_batch_size, shuffle=True)
        dataloaders[split] = train.torch.prepare_data_loader(dataloader)

    # Calculate the batch size for a single worker
    worker_batch_size = configs["batch_size"] // session.get_world_size()

    device = train.torch.get_device()

    # Prepare DDP Model, optimizer, and loss function
    model = initialize_model()

    model = train.torch.prepare_model(model)

    optimizer = optim.Adam(
        model.parameters(), lr=configs["lr"]
    )
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

            for inputs, labels in dataloaders[phase]:
                inputs = inputs.to(device)
                labels = labels.to(device)

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

            size = len(torch_datasets[phase]) // session.get_world_size()
            epoch_loss = running_loss / size
            epoch_acc = running_corrects / size

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
)

result = trainer.fit()
