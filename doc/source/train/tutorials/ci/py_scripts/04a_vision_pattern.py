# 00. Runtime setup 
import os
import sys
import subprocess

# Non-secret env var 
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# Install Python dependencies 
subprocess.check_call([
    sys.executable, "-m", "pip", "install", "--no-cache-dir",
    "torch==2.8.0",
    "torchvision==0.23.0",
    "matplotlib==3.10.6",
    "pyarrow==14.0.2",
    "datasets==2.19.2",
])

# 01. Imports

# ————————————————————————
# Standard Library Utilities
# ————————————————————————
import os
import io
import tempfile
import shutil  # file I/O and temp dirs
import json                      # reading/writing configs
import random, uuid              # randomness and unique IDs

# ————————————————————————
# Core Data & Storage Libraries
# ————————————————————————
import pandas as pd              # tabular data handling
import numpy as np               # numerical ops
import pyarrow as pa             # in-memory columnar format
import pyarrow.parquet as pq     # reading/writing Parquet files
from tqdm import tqdm            # progress bars

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
from ray.data import DataContext
DataContext.get_current().use_streaming_executor = False


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
        records.append({
            "image_bytes": buf.getvalue(),
            "label": example["label"]
        })
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

table = pa.Table.from_pydict({
    "image_bytes": [r["image_bytes"] for r in records],
    "label": [r["label"] for r in records]
})
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
        table = self.parquet_file.read_row_group(row_group_idx, columns=["image_bytes", "label"])
        row = table.to_pandas().iloc[local_idx]

        img = Image.open(io.BytesIO(row["image_bytes"])).convert("RGB")
        if self.transform:
            img = self.transform(img)
        return img, row["label"]

# 07. Define data preprocessing transform
IMAGENET_MEAN = [0.485, 0.456, 0.406]
IMAGENET_STD  = [0.229, 0.224, 0.225]

transform = T.Compose([
    T.ToTensor(),
    T.Normalize(mean=IMAGENET_MEAN, std=IMAGENET_STD),
])

# 08. Create train/val Parquet splits 
full_path = "/mnt/cluster_storage/food101_lite/parquet_256/shard_0.parquet"

df = (
    pq.read_table(full_path)
    .to_pandas()
    .sample(frac=1.0, random_state=42)  # shuffle for reproducibility
)

df[:-500].to_parquet("/mnt/cluster_storage/food101_lite/train.parquet")   # training
df[-500:].to_parquet("/mnt/cluster_storage/food101_lite/val.parquet")     # validation

# 09. Observe data shape

loader = DataLoader(
    Food101Dataset("/mnt/cluster_storage/food101_lite/train.parquet", transform=transform),
    batch_size=16,
    shuffle=True,
    num_workers=4,
)

for images, labels in loader:
    print(images.shape, labels.shape)
    break

# 10. Define helper to create prepared DataLoader
def build_dataloader(parquet_path: str, batch_size: int, shuffle=True):
    dataset = Food101Dataset(parquet_path, transform=transform)

    # Let Ray handle DistributedSampler and device placement via prepare_data_loader.
    loader = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=shuffle,
        num_workers=2,
    )
    return prepare_data_loader(loader)

# 11. Define Ray Train train_loop_per_worker (tempdir checkpoints + Ray-managed metrics)
def train_loop_per_worker(config):
    import tempfile

    rank = get_context().get_world_rank()

    # === Model ===
    net = resnet18(num_classes=101)
    model = prepare_model(net)

    # === Optimizer / Loss ===
    optimizer = optim.Adam(model.parameters(), lr=config["lr"])
    criterion = nn.CrossEntropyLoss()

    # === Resume from Checkpoint ===
    start_epoch = 0
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as ckpt_dir:
            # Map to CPU is fine; prepare_model will handle device placement.
            model.load_state_dict(torch.load(os.path.join(ckpt_dir, "model.pt"), map_location="cpu"))
            opt_path = os.path.join(ckpt_dir, "optimizer.pt")
            if os.path.exists(opt_path):
                optimizer.load_state_dict(torch.load(opt_path, map_location="cpu"))
            meta_path = os.path.join(ckpt_dir, "meta.pt")
            if os.path.exists(meta_path):
                # Continue from the next epoch after the saved one
                start_epoch = int(torch.load(meta_path).get("epoch", -1)) + 1
        if rank == 0:
            print(f"[Rank {rank}] Resumed from checkpoint at epoch {start_epoch}")

    # === DataLoaders ===
    train_loader = build_dataloader(
        "/mnt/cluster_storage/food101_lite/train.parquet", config["batch_size"], shuffle=True
    )
    val_loader = build_dataloader(
        "/mnt/cluster_storage/food101_lite/val.parquet", config["batch_size"], shuffle=False
    )

    # === Training Loop ===
    for epoch in range(start_epoch, config["epochs"]):
        # Required when using DistributedSampler
        if hasattr(train_loader, "sampler") and hasattr(train_loader.sampler, "set_epoch"):
            train_loader.sampler.set_epoch(epoch)

        model.train()
        train_loss_total, train_batches = 0.0, 0
        for xb, yb in train_loader:
            optimizer.zero_grad()
            loss = criterion(model(xb), yb)
            loss.backward()
            optimizer.step()
            train_loss_total += loss.item()
            train_batches += 1
        train_loss = train_loss_total / max(train_batches, 1)

        # === Validation Loop ===
        model.eval()
        val_loss_total, val_batches = 0.0, 0
        with torch.no_grad():
            for val_xb, val_yb in val_loader:
                val_loss_total += criterion(model(val_xb), val_yb).item()
                val_batches += 1
        val_loss = val_loss_total / max(val_batches, 1)

        metrics = {"epoch": epoch, "train_loss": train_loss, "val_loss": val_loss}
        if rank == 0:
            print(metrics)

        # ---- Save checkpoint to fast local temp dir; Ray persists it via report() ----
        if rank == 0:
            with tempfile.TemporaryDirectory() as tmpdir:
                torch.save(model.state_dict(), os.path.join(tmpdir, "model.pt"))
                torch.save(optimizer.state_dict(), os.path.join(tmpdir, "optimizer.pt"))
                torch.save({"epoch": epoch}, os.path.join(tmpdir, "meta.pt"))
                ckpt_out = Checkpoint.from_directory(tmpdir)
                train.report(metrics, checkpoint=ckpt_out)
        else:
            # Non-zero ranks report metrics only (no checkpoint attachment)
            train.report(metrics)

    # === Final validation accuracy (distributed via TorchMetrics) ===
    from torchmetrics.classification import MulticlassAccuracy

    model.eval()
    device = next(model.parameters()).device
    # Sync across DDP workers when computing the final value
    acc_metric = MulticlassAccuracy(
        num_classes=101, average="micro", sync_on_compute=True
    ).to(device)

    with torch.no_grad():
        for xb, yb in val_loader:
            logits = model(xb)
            preds = torch.argmax(logits, dim=1)
            acc_metric.update(preds, yb)

    dist_val_acc = acc_metric.compute().item()
    if rank == 0:
        print(f"Val Accuracy (distributed): {dist_val_acc:.2%}")

# 12. Run Training with Ray Train 

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={"lr": 1e-3, "batch_size": 64, "epochs": 5},
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    run_config=RunConfig(
        name="food101_ft_resume",
        storage_path="/mnt/cluster_storage/food101_lite/results",
        checkpoint_config=CheckpointConfig(
            num_to_keep=5, 
            checkpoint_score_attribute="val_loss",
            checkpoint_score_order="min"
        ),
        failure_config=FailureConfig(max_failures=3),
    ),
)

result = trainer.fit()
print("Final metrics:", result.metrics)
best_ckpt = result.checkpoint  # this is the one with lowest val_loss

# 13. Plot training / validation loss curves 

# Pull the full metrics history Ray stored for this run
df = result.metrics_dataframe.copy()

# Keep only the columns we need (guard against extra columns)
cols = [c for c in ["epoch", "train_loss", "val_loss"] if c in df.columns]
df = df[cols].dropna()

# If multiple rows per epoch exist, keep the last report per epoch
if "epoch" in df.columns:
    df = df.sort_index().groupby("epoch", as_index=False).last()

# Plot
plt.figure(figsize=(8, 5))
if "train_loss" in df.columns:
    plt.plot(df["epoch"], df["train_loss"], marker="o", label="Train Loss")
if "val_loss" in df.columns:
    plt.plot(df["epoch"], df["val_loss"], marker="o", label="Val Loss")
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

# 15. Batch inference with Ray Data (force GPU actors if available on the cluster)

import ray.data as rdata

class ImageBatchPredictor:
    """Stateful per-actor batch predictor that keeps the model in memory."""
    def __init__(self, checkpoint_path: str):
        # Pick the best available device on the ACTOR (worker), not the driver.
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # === Load model & weights once per actor ===
        model = resnet18(num_classes=101)
        checkpoint = Checkpoint.from_directory(checkpoint_path)
        with checkpoint.as_directory() as ckpt_dir:
            state_dict = torch.load(
                os.path.join(ckpt_dir, "model.pt"),
                map_location=self.device,
            )
            # Strip DDP "module." prefix if present
            state_dict = {k.replace("module.", "", 1): v for k, v in state_dict.items()}
            model.load_state_dict(state_dict)

        self.model = model.eval().to(self.device)
        self.transform = T.Compose([
            T.ToTensor(),
            T.Normalize(mean=IMAGENET_MEAN, std=IMAGENET_STD),
        ])
        torch.set_grad_enabled(False)

    def __call__(self, batch):
        """batch: Pandas DataFrame with columns ['image_bytes', 'label']"""
        imgs = []
        for b in batch["image_bytes"]:
            img = Image.open(io.BytesIO(b)).convert("RGB")
            imgs.append(self.transform(img).numpy())  # (C,H,W) as numpy
        x = torch.from_numpy(np.stack(imgs, axis=0)).to(self.device)  # (N,C,H,W)

        logits = self.model(x)
        preds = torch.argmax(logits, dim=1).cpu().numpy()

        out = batch.copy()
        out["predicted_label"] = preds.astype(int)
        return out[["predicted_label", "label"]]

def build_inference_dataset(
    checkpoint_path: str,
    parquet_path: str,
    *,
    num_actors: int = 1,
    batch_size: int = 64,
    use_gpu_actors: bool = True,   # <— default to GPU actors on the cluster
):
    """
    Create a Ray Dataset pipeline that performs batch inference using
    stateful per-actor model loading. By default, requests 1 GPU per actor
    so each actor runs on a GPU worker (driver may have no GPU).
    """
    ds = rdata.read_parquet(parquet_path, columns=["image_bytes", "label"])

    pred_ds = ds.map_batches(
        ImageBatchPredictor,                     # pass the CLASS (stateful actors)
        fn_constructor_args=(checkpoint_path,),  # ctor args for each actor
        batch_size=batch_size,
        batch_format="pandas",
        concurrency=num_actors,                  # number of actor workers
        num_gpus=1 if use_gpu_actors else 0,     # <— force GPU placement on workers
    )
    return pred_ds

# 16. Perform inference with Ray Data using the best checkpoint

checkpoint_root = "/mnt/cluster_storage/food101_lite/results/food101_ft_resume"

checkpoint_dirs = sorted(
    [
        d for d in os.listdir(checkpoint_root)
        if d.startswith("checkpoint_") and os.path.isdir(os.path.join(checkpoint_root, d))
    ],
    reverse=True,
)

if not checkpoint_dirs:
    raise FileNotFoundError("No checkpoint directories found.")

# Use the best checkpoint from the training result
with result.checkpoint.as_directory() as ckpt_dir:
    print("Best checkpoint contents:", os.listdir(ckpt_dir))
    best_ckpt_path = ckpt_dir

parquet_path = "/mnt/cluster_storage/food101_lite/val.parquet"

# Which item to visualize
idx = 2

import itertools

pred_ds = build_inference_dataset(
    checkpoint_path=best_ckpt_path,
    parquet_path=parquet_path,
    num_actors=1,       # adjust to scale out
    batch_size=64,      # adjust for throughput
)

# Avoid .take() / limit(); stream rows and grab the idx-th one.
row_iter = pred_ds.iter_rows()
inference_row = next(itertools.islice(row_iter, idx, idx + 1))
print(inference_row)   # {"predicted_label": ..., "label": ...}


# Load label map from Hugging Face (for pretty titles)
ds_tmp = load_dataset("food101", split="train[:1%]")  # just to get label names
label_names = ds_tmp.features["label"].names

# Load the raw image locally for visualization
dataset = Food101Dataset(parquet_path, transform=None)
img, _ = dataset[idx]

# Plot the image with predicted and true labels
plt.imshow(img)
plt.axis("off")
plt.title(
    f"Pred: {label_names[int(inference_row['predicted_label'])]}\n"
    f"True: {label_names[int(inference_row['label'])]}"
)
plt.show()

# 17. Cleanup---delete checkpoints and metrics from model training

# Base directory
BASE_DIR = "/mnt/cluster_storage/food101_lite"

# Paths to clean
paths_to_delete = [
    os.path.join(BASE_DIR, "tmp_checkpoints"),           # custom checkpoints
    os.path.join(BASE_DIR, "results", "history.csv"),    # metrics history file
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

