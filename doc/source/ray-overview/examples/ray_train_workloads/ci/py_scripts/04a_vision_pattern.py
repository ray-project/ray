# 00. Runtime setup — install same deps and set env vars
import os, sys, subprocess

# Non-secret env var
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# Install Python dependencies
subprocess.check_call(
    [
        sys.executable,
        "-m",
        "pip",
        "install",
        "--no-cache-dir",
        "torch==2.8.0",
        "torchvision==0.23.0",
        "matplotlib==3.10.6",
        "pyarrow==14.0.2",
        "datasets==2.19.2",
    ]
)

# 01. Imports

# ————————————————————————
# Standard Library Utilities
# ————————————————————————
import os, io, tempfile, shutil  # file I/O and temp dirs
import json  # reading/writing configs
import random, uuid  # randomness and unique IDs

# ————————————————————————
# Core Data & Storage Libraries
# ————————————————————————
import pandas as pd  # tabular data handling
import numpy as np  # numerical ops
import pyarrow as pa  # in-memory columnar format
import pyarrow.parquet as pq  # reading/writing Parquet files
from tqdm import tqdm  # progress bars

# ————————————————————————
# Image Handling & Visualization
# ————————————————————————
from PIL import Image
import matplotlib.pyplot as plt  # plotting loss curves, images

# ————————————————————————
# PyTorch + TorchVision Core
# ————————————————————————
import torch
import torch.nn as nn
import torch.optim as optim
from torch.utils.data import Dataset, DataLoader
from torchvision import transforms as T
from torchvision.models import resnet18
from torchvision.transforms import Compose, Resize, CenterCrop

# ————————————————————————
# Ray Train: Distributed Training Primitives
# ————————————————————————
import ray
import ray.train as train
from ray.train.torch import (
    prepare_model,
    prepare_data_loader,
    TorchTrainer,
)
from ray.train import (
    ScalingConfig,
    RunConfig,
    FailureConfig,
    CheckpointConfig,
    Checkpoint,
    get_checkpoint,
    get_context,
)

# ————————————————————————
# Dataset Access
# ————————————————————————
from datasets import load_dataset  # Hugging Face Datasets

# 02. Load 10% of food101 (~7,500 images)
ds = load_dataset("food101", split="train[:10%]")

# 03. Resize and encode as JPEG bytes
transform = Compose([Resize(256), CenterCrop(224)])
records = []

for example in tqdm(ds, desc="Preprocessing images", unit="img"):
    try:
        img = transform(example["image"])
        buf = io.BytesIO()
        img.save(buf, format="JPEG")
        records.append({"image_bytes": buf.getvalue(), "label": example["label"]})
    except Exception as e:
        continue

# 04. Visualize the dataset

label_names = ds.features["label"].names  # maps int → string

samples = random.sample(records, 9)

fig, axs = plt.subplots(3, 3, figsize=(8, 8))
fig.suptitle("Sample Resized Images from food101-lite", fontsize=16)

for ax, rec in zip(axs.flatten(), samples):
    img = Image.open(io.BytesIO(rec["image_bytes"]))
    label_name = label_names[rec["label"]]
    ax.imshow(img)
    ax.set_title(label_name)
    ax.axis("off")

plt.tight_layout()
plt.show()

# 05. Write Dataset to Parquet

output_dir = "/mnt/cluster_storage/food101_lite/parquet_256"
os.makedirs(output_dir, exist_ok=True)

table = pa.Table.from_pydict(
    {
        "image_bytes": [r["image_bytes"] for r in records],
        "label": [r["label"] for r in records],
    }
)
pq.write_table(table, os.path.join(output_dir, "shard_0.parquet"))

print(f"Wrote {len(records)} records to {output_dir}")

# 06. Define PyTorch Dataset that loads from Parquet


class Food101Dataset(Dataset):
    def __init__(self, parquet_path: str, transform=None):
        self.parquet_file = pq.ParquetFile(parquet_path)
        self.transform = transform

        # Precompute a global row index to (row_group_idx, local_idx) map
        self.row_group_map = []
        for rg_idx in range(self.parquet_file.num_row_groups):
            rg_meta = self.parquet_file.metadata.row_group(rg_idx)
            num_rows = rg_meta.num_rows
            self.row_group_map.extend([(rg_idx, i) for i in range(num_rows)])

    def __len__(self):
        return len(self.row_group_map)

    def __getitem__(self, idx):
        row_group_idx, local_idx = self.row_group_map[idx]
        # Read only the relevant row group (in memory-efficient batch---for scalability)
        table = self.parquet_file.read_row_group(
            row_group_idx, columns=["image_bytes", "label"]
        )
        row = table.to_pandas().iloc[local_idx]

        img = Image.open(io.BytesIO(row["image_bytes"])).convert("RGB")
        if self.transform:
            img = self.transform(img)
        return img, row["label"]


# 07. Define data preprocessing transform
IMAGENET_MEAN = [0.485, 0.456, 0.406]
IMAGENET_STD = [0.229, 0.224, 0.225]

transform = T.Compose(
    [
        T.ToTensor(),
        T.Normalize(mean=IMAGENET_MEAN, std=IMAGENET_STD),
    ]
)

# 08. Create train/val Parquet splits
full_path = "/mnt/cluster_storage/food101_lite/parquet_256/shard_0.parquet"

df = (
    pq.read_table(full_path)
    .to_pandas()
    .sample(frac=1.0, random_state=42)  # shuffle for reproducibility
)

df[:-500].to_parquet("/mnt/cluster_storage/food101_lite/train.parquet")  # training
df[-500:].to_parquet("/mnt/cluster_storage/food101_lite/val.parquet")  # validation

# 09. Observe data shape

loader = DataLoader(
    Food101Dataset(
        "/mnt/cluster_storage/food101_lite/train.parquet", transform=transform
    ),
    batch_size=16,
    shuffle=True,
    num_workers=4,
)

for images, labels in loader:
    print(images.shape, labels.shape)
    break

# 10. Define helper to create prepared DataLoader
from torch.utils.data.distributed import DistributedSampler


def build_dataloader(parquet_path: str, batch_size: int, shuffle=True):
    dataset = Food101Dataset(parquet_path, transform=transform)

    # Add a DistributedSampler to shard data across workers
    sampler = DistributedSampler(
        dataset,
        num_replicas=train.get_context().get_world_size(),
        rank=train.get_context().get_world_rank(),
        shuffle=shuffle,
        drop_last=shuffle,
    )

    loader = DataLoader(
        dataset,
        batch_size=batch_size,
        sampler=sampler,
        num_workers=2,
    )
    return prepare_data_loader(loader)


# 11. Define Ray Train train_loop_per_worker
def train_loop_per_worker(config):

    # === Model ===
    net = resnet18(num_classes=101)
    model = prepare_model(net)

    # === Optimizer / Loss ===
    optimizer = optim.Adam(model.parameters(), lr=config["lr"])
    criterion = nn.CrossEntropyLoss()

    # === Resume from Checkpoint ===
    checkpoint = get_checkpoint()
    start_epoch = 0
    if checkpoint:
        with checkpoint.as_directory() as ckpt_dir:
            model.load_state_dict(torch.load(os.path.join(ckpt_dir, "model.pt")))
            optimizer.load_state_dict(
                torch.load(os.path.join(ckpt_dir, "optimizer.pt"))
            )
            start_epoch = torch.load(os.path.join(ckpt_dir, "extra.pt"))["epoch"]
        print(
            f"[Rank {get_context().get_world_rank()}] Resumed from checkpoint at epoch {start_epoch}"
        )

    # === DataLoaders ===
    train_loader = build_dataloader(
        "/mnt/cluster_storage/food101_lite/train.parquet",
        config["batch_size"],
        shuffle=True,
    )
    val_loader = build_dataloader(
        "/mnt/cluster_storage/food101_lite/val.parquet",
        config["batch_size"],
        shuffle=False,
    )

    # === Training Loop ===
    for epoch in range(start_epoch, config["epochs"]):
        train_loader.sampler.set_epoch(epoch)  # required when using DistributedSampler
        model.train()
        train_loss_total = 0.0
        train_batches = 0

        for xb, yb in train_loader:
            optimizer.zero_grad()
            loss = criterion(model(xb), yb)
            loss.backward()
            optimizer.step()
            train_loss_total += loss.item()
            train_batches += 1

        train_loss = train_loss_total / train_batches

        # === Validation Loop ===
        model.eval()
        val_loss_total = 0.0
        val_batches = 0
        with torch.no_grad():
            for val_xb, val_yb in val_loader:
                val_loss_total += criterion(model(val_xb), val_yb).item()
                val_batches += 1
        val_loss = val_loss_total / val_batches

        metrics = {"train_loss": train_loss, "val_loss": val_loss, "epoch": epoch}
        if train.get_context().get_world_rank() == 0:
            print(metrics)

        # === Save checkpoint only on rank 0 ===
        if get_context().get_world_rank() == 0:
            ckpt_dir = f"/mnt/cluster_storage/food101_lite/tmp_checkpoints/epoch_{epoch}_{uuid.uuid4().hex}"
            os.makedirs(ckpt_dir, exist_ok=True)
            torch.save(model.state_dict(), os.path.join(ckpt_dir, "model.pt"))
            torch.save(optimizer.state_dict(), os.path.join(ckpt_dir, "optimizer.pt"))
            torch.save({"epoch": epoch}, os.path.join(ckpt_dir, "extra.pt"))
            checkpoint = Checkpoint.from_directory(ckpt_dir)
        else:
            checkpoint = None

        # Append metrics to a file (only on rank 0)
        if train.get_context().get_world_rank() == 0:
            with open(
                "/mnt/cluster_storage/food101_lite/results/history.csv", "a"
            ) as f:
                f.write(f"{epoch},{train_loss},{val_loss}\n")
        train.report(metrics, checkpoint=checkpoint)

    correct, total = 0, 0
    model.eval()
    for xb, yb in val_loader:
        xb, yb = xb.cuda(), yb.cuda()
        pred = model(xb).argmax(dim=1)
        correct += (pred == yb).sum().item()
        total += yb.size(0)
    accuracy = correct / total
    print(f"Val Accuracy: {accuracy:.2%}")


# 12. Run Training with Ray Train

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={"lr": 1e-3, "batch_size": 64, "epochs": 10},
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    run_config=RunConfig(
        name="food101_ft_resume",
        storage_path="/mnt/cluster_storage/food101_lite/results",
        checkpoint_config=CheckpointConfig(
            num_to_keep=5,
            checkpoint_frequency=1,
            checkpoint_score_attribute="val_loss",
            checkpoint_score_order="min",
        ),
        failure_config=FailureConfig(max_failures=3),
    ),
)

result = trainer.fit()
print("Final metrics:", result.metrics)
best_ckpt = result.checkpoint  # this is the one with lowest val_loss

# 13. Plot training / validation loss curves
history_path = "/mnt/cluster_storage/food101_lite/results/history.csv"
df = pd.read_csv(history_path, names=["epoch", "train_loss", "val_loss"])

# Plot
plt.figure(figsize=(8, 5))
plt.plot(df["epoch"], df["train_loss"], label="Train Loss", marker="o")
plt.plot(df["epoch"], df["val_loss"], label="Val Loss", marker="o")
plt.xlabel("Epoch")
plt.ylabel("Loss")
plt.title("Train/Val Loss across Epochs")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()

# 14. Run the trainer again to demonstrate resuming from latest checkpoint

result = trainer.fit()
print("Final metrics:", result.metrics)


# 15. Define batch inference function


@ray.remote(num_gpus=1)
def run_inference_from_checkpoint(checkpoint_path, parquet_path, idx=0):

    # === Load model ===
    model = resnet18(num_classes=101)
    checkpoint = Checkpoint.from_directory(checkpoint_path)

    with checkpoint.as_directory() as ckpt_dir:
        state_dict = torch.load(os.path.join(ckpt_dir, "model.pt"), map_location="cuda")

        # Strip "module." prefix from distributed data parallelism trained weights
        state_dict = {k.replace("module.", "", 1): v for k, v in state_dict.items()}

        model.load_state_dict(state_dict)

    model.eval().cuda()

    # === Define transform ===
    transform = T.Compose(
        [
            T.ToTensor(),
            T.Normalize(mean=IMAGENET_MEAN, std=IMAGENET_STD),
        ]
    )

    # === Load dataset ===
    dataset = Food101Dataset(parquet_path, transform=transform)
    img, label = dataset[idx]
    img = img.unsqueeze(0).cuda()  # batch size 1

    with torch.no_grad():
        logits = model(img)
        pred = torch.argmax(logits, dim=1).item()

    return {"predicted_label": pred, "true_label": int(label), "index": idx}


# 16. Perform inference with best trained model (that is, lowest validation loss for a checkpointed model)

checkpoint_root = "/mnt/cluster_storage/food101_lite/results/food101_ft_resume"

checkpoint_dirs = sorted(
    [
        d
        for d in os.listdir(checkpoint_root)
        if d.startswith("checkpoint_")
        and os.path.isdir(os.path.join(checkpoint_root, d))
    ],
    reverse=True,
)

if not checkpoint_dirs:
    raise FileNotFoundError("No checkpoint directories found.")

with result.checkpoint.as_directory() as ckpt_dir:
    print("Best checkpoint contents:", os.listdir(ckpt_dir))
    best_ckpt_path = ckpt_dir
parquet_path = "/mnt/cluster_storage/food101_lite/val.parquet"

# Define which image to use
idx = 2

# Launch GPU inference task with Ray
result = ray.get(
    run_inference_from_checkpoint.remote(best_ckpt_path, parquet_path, idx=idx)
)
print(result)

# Load label map from Hugging Face
ds = load_dataset("food101", split="train[:1%]")  # load just to get label names
label_names = ds.features["label"].names

# Load image from the same dataset locally
dataset = Food101Dataset(parquet_path, transform=None)  # No transform = raw image
img, _ = dataset[idx]

# Plot the image with predicted and true labels
plt.imshow(img)
plt.axis("off")
plt.title(
    f"Pred: {label_names[result['predicted_label']]}\nTrue: {label_names[result['true_label']]}"
)
plt.show()

# 17. Cleanup---delete checkpoints and metrics from model training

# Base directory
BASE_DIR = "/mnt/cluster_storage/food101_lite"

# Paths to clean
paths_to_delete = [
    os.path.join(BASE_DIR, "tmp_checkpoints"),  # custom checkpoints
    os.path.join(BASE_DIR, "results", "history.csv"),  # metrics history file
    os.path.join(BASE_DIR, "results", "food101_ft_resume"),  # ray trainer run dir
    os.path.join(BASE_DIR, "results", "food101_ft_run"),
    os.path.join(BASE_DIR, "results", "food101_single_run"),
]

# Delete each path if it exists
for path in paths_to_delete:
    if os.path.exists(path):
        if os.path.isfile(path):
            os.remove(path)
            print(f"Deleted file: {path}")
        else:
            shutil.rmtree(path)
            print(f"Deleted directory: {path}")
    else:
        print(f"Not found (skipped): {path}")
