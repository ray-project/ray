# 00. Runtime setup
import os
import sys
import subprocess

# Non-secret env var (safe to set here)
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# Install Python dependencies (same pinned versions as build.sh)
subprocess.check_call([
    sys.executable, "-m", "pip", "install", "--no-cache-dir",
    "torch==2.8.0",
    "torchvision==0.23.0",
    "matplotlib==3.10.6",
    "pyarrow==14.0.2",
    "datasets==2.19.2",
    "lightning==2.5.5",
])

# 01. Imports

# Standard libraries
import os
import io
import json
import shutil
import tempfile
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
from PIL import Image

# Ray
import ray, ray.data
from ray.train import ScalingConfig, get_context, RunConfig, FailureConfig, CheckpointConfig, Checkpoint, get_checkpoint
from ray.train.torch import TorchTrainer
from ray.train.lightning import RayLightningEnvironment

# PyTorch / Lightning
import lightning.pytorch as pl
import torch
from torch import nn

# Dataset
from datasets import load_dataset
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm  
from torchvision.transforms import Compose, Resize, CenterCrop
import random

# 02. Load 10% of food101 (~7,500 images)
hf_ds = load_dataset("food101", split="train[:10%]")

# 03. Resize + encode as JPEG bytes (Ray Data; BYTES-BASED)

# Build Ray items with RAW BYTES (serializable) + label
rows = []
buf = io.BytesIO()
for ex in hf_ds:
    img = ex["image"].convert("RGB")
    buf.seek(0); buf.truncate(0)
    img.save(buf, format="JPEG")
    rows.append({"image_bytes_raw": buf.getvalue(), "label": ex["label"]})

# Create a Ray Dataset from serializable dicts
ds = ray.data.from_items(rows)

# Define preprocessing (runs on Ray workers)
transform = Compose([Resize(256), CenterCrop(224)])

def preprocess_images(batch_df):
    out_img_bytes, out_labels = [], []
    for b, lbl in zip(batch_df["image_bytes_raw"], batch_df["label"]):
        try:
            img = Image.open(io.BytesIO(b)).convert("RGB")
            img = transform(img)
            out = io.BytesIO()
            img.save(out, format="JPEG")
            out_img_bytes.append(out.getvalue())
            out_labels.append(lbl)
        except Exception:
            # Skip unreadable/corrupt rows but don't kill the batch
            continue
    return {"image_bytes": out_img_bytes, "label": out_labels}

# Parallel preprocessing
processed_ds = ds.map_batches(
    preprocess_images,
    batch_format="pandas",
    num_cpus=1,
)

print("✅ Processed records:", processed_ds.count())
processed_ds.show(3)

# 04. Visualize the dataset (Ray Data version)
label_names = hf_ds.features["label"].names  # int -> class name

samples = processed_ds.random_shuffle().take(9)

fig, axs = plt.subplots(3, 3, figsize=(8, 8))
fig.suptitle("Sample Resized Images from food101-lite", fontsize=16)

for ax, rec in zip(axs.flatten(), samples):
    img = Image.open(io.BytesIO(rec["image_bytes"]))
    ax.imshow(img)
    ax.set_title(label_names[rec["label"]])
    ax.axis("off")

plt.tight_layout()
plt.show()

# 05. Persist Ray Dataset to Parquet
import os

output_dir = "/mnt/cluster_storage/food101_lite/parquet_256"
os.makedirs(output_dir, exist_ok=True)

# Write each block as its own Parquet shard
processed_ds.write_parquet(output_dir)

print(f"✅ Wrote {processed_ds.count()} records to {output_dir}")

# 06. Load & Decode Food-101-Lite

# Path to Parquet shards written earlier
PARQUET_PATH = "/mnt/cluster_storage/food101_lite/parquet_256"

# Read the Parquet files (≈7 500 rows with JPEG bytes + label)
ds = ray.data.read_parquet(PARQUET_PATH)
print("Raw rows:", ds.count())

# Decode JPEG → CHW float32 in [‑1, 1]

def decode_and_normalize(batch_df):
    """Decode JPEG bytes and scale to [-1, 1]."""
    images = []
    for b in batch_df["image_bytes"]:
        img = Image.open(io.BytesIO(b)).convert("RGB")
        arr = np.asarray(img, dtype=np.float32) / 255.0       # H × W × 3, 0‑1
        arr = (arr - 0.5) / 0.5                               # ‑1 … 1
        arr = arr.transpose(2, 0, 1)                          # 3 × H × W (CHW)
        images.append(arr)
    return {"image": images}

# Apply in parallel
#   batch_format="pandas" → batch_df is a DataFrame, return dict of lists.
#   default task‑based compute is sufficient for a stateless function.

ds = ds.map_batches(
    decode_and_normalize,
    batch_format="pandas",
    # Use the default (task‑based) compute strategy since `decode_and_normalize` is a plain function.
    num_cpus=1,
)

# Drop the original JPEG column to save memory
if "image_bytes" in ds.schema().names:
    ds = ds.drop_columns(["image_bytes", "label"])

print("Decoded rows:", ds.count())

# 07. Shuffle & Train/Val Split

# Typical 80 / 20 split
TOTAL = ds.count()
train_count = int(TOTAL * 0.8)
ds = ds.random_shuffle()  # expensive operation -- for large datasets, consider file shuffling or local shuffling. Ray offers both options
train_ds, val_ds = ds.split_at_indices([train_count])
print("Train rows:", train_ds.count())
print("Val rows:",   val_ds.count())

# 08. Pixel De-noising Diffusion Model  — final, logging via Lightning/Ray

class PixelDiffusion(pl.LightningModule):
    """Tiny CNN that predicts noise ϵ given noisy image + timestep."""

    def __init__(self, max_t=1000):
        super().__init__()
        self.max_t = max_t

        # Network: (3+1)-channel input → 3-channel noise prediction
        self.net = nn.Sequential(
            nn.Conv2d(4, 32, 3, padding=1), nn.ReLU(),
            nn.Conv2d(32, 32, 3, padding=1), nn.ReLU(),
            nn.Conv2d(32, 3, 3, padding=1),
        )
        self.loss_fn = nn.MSELoss()

    # ---------- forward ----------
    def forward(self, noisy_img, t):
        """noisy_img: Bx3xHxW,  t: B (int) or Bx1 scalar"""
        b, _, h, w = noisy_img.shape
        t_scaled = (t / self.max_t).view(-1, 1, 1, 1).float().to(noisy_img.device)
        t_img = t_scaled.expand(-1, 1, h, w)
        x = torch.cat([noisy_img, t_img], dim=1)  # 4 channels
        return self.net(x)
    
    # ---------- shared loss ----------
    def _shared_step(self, batch):
        clean = batch["image"].to(self.device)             # Bx3xHxW, -1…1
        noise = torch.randn_like(clean)                    # ϵ ~ N(0, 1)
        t = torch.randint(0, self.max_t, (clean.size(0),), device=self.device)
        noisy = clean + noise                              # x_t = x_0 + ϵ
        pred_noise = self(noisy, t)
        return self.loss_fn(pred_noise, noise)

    # ---------- training / validation ----------
    def training_step(self, batch, batch_idx):
        loss = self._shared_step(batch)
        # Let Lightning aggregate + Ray callback report
        self.log("train_loss", loss, on_epoch=True, prog_bar=False, sync_dist=True)
        return loss

    def validation_step(self, batch, batch_idx):
        loss = self._shared_step(batch)
        self.log("val_loss", loss, on_epoch=True, prog_bar=False, sync_dist=True)
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=2e-4)

# 09. Train loop for Ray TorchTrainer (RayDDP + Lightning-native, NEW API aligned)

def train_loop(config):
    """
    Lightning-owned loop with Ray integration:
      - RayDDPStrategy for multi-worker DDP
      - RayLightningEnvironment for ranks/addrs
      - RayTrainReportCallback to forward metrics + checkpoints to Ray
      - Resume from the Ray-provided Lightning checkpoint ("checkpoint.ckpt")
    """
    import warnings
    warnings.filterwarnings(
        "ignore",
        message="barrier.*using the device under current context",
    )
    import os
    import torch
    import lightning.pytorch as pl
    from ray.train import get_checkpoint, get_context
    from ray.train.lightning import (
        RayLightningEnvironment,
        RayDDPStrategy,
        RayTrainReportCallback,
        prepare_trainer,
    )

    # ---- Data shards from Ray Data → iterable loaders ----
    train_ds = ray.train.get_dataset_shard("train")
    val_ds   = ray.train.get_dataset_shard("val")
    train_loader = train_ds.iter_torch_batches(batch_size=config.get("batch_size", 32))
    val_loader   = val_ds.iter_torch_batches(batch_size=config.get("batch_size", 32))

    # ---- Model ----
    model = PixelDiffusion()

    # ---- Lightning Trainer configured for Ray ----
    CKPT_ROOT = os.path.join(tempfile.gettempdir(), "ray_pl_ckpts")
    os.makedirs(CKPT_ROOT, exist_ok=True)

    trainer = pl.Trainer(
        max_epochs=config.get("epochs", 10),
        devices="auto",
        accelerator="auto",
        strategy=RayDDPStrategy(),
        plugins=[RayLightningEnvironment()],
        callbacks=[
            RayTrainReportCallback(),
            pl.callbacks.ModelCheckpoint(
                dirpath=CKPT_ROOT,         # local scratch is fine (or leave None to use default)
                filename="epoch-{epoch:03d}",
                every_n_epochs=1,
                save_top_k=-1,
                save_last=True,
            ),
        ],
        default_root_dir=CKPT_ROOT,        # also local
        enable_progress_bar=False,
        check_val_every_n_epoch=1,
    )

    # Wire up ranks/world size with Ray
    trainer = prepare_trainer(trainer)

    # ---- Resume from latest Ray-provided Lightning checkpoint (if any) ----
    ckpt_path = None
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as d:
            candidate = os.path.join(d, "checkpoint.ckpt")
            if os.path.exists(candidate):
                ckpt_path = candidate
                if get_context().get_world_rank() == 0:
                    print(f"✅ Resuming from Lightning checkpoint: {ckpt_path}")

    # ---- Let Lightning own the loop ----
    trainer.fit(
        model,
        train_dataloaders=train_loader,
        val_dataloaders=val_loader,
        ckpt_path=ckpt_path,
    )

# 10. Launch distributed training (same API, now Lightning-native inside)

trainer = TorchTrainer(
    train_loop_per_worker=train_loop,
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    datasets={"train": train_ds, "val": val_ds},
    run_config=RunConfig(
        name="food101_diffusion_ft",
        storage_path="/mnt/cluster_storage/generative_cv/food101_diffusion_results",
        checkpoint_config=CheckpointConfig(
            num_to_keep=5,
            checkpoint_score_attribute="epoch",
            checkpoint_score_order="max",
        ),
        failure_config=FailureConfig(max_failures=1),
    ),
)

result = trainer.fit()
print("Training complete →", result.metrics)
best_ckpt = result.checkpoint

# 11. Plot train/val loss curves (Ray + Lightning integration)

# Ray stores all metrics emitted by Lightning in a dataframe
df = result.metrics_dataframe

# Display first few rows (optional sanity check)
print(df.head())

# Convert and clean up
if "train_loss" not in df.columns or "val_loss" not in df.columns:
    raise ValueError("Expected train_loss and val_loss in metrics. "
                     "Did you call self.log('train_loss') / self.log('val_loss') in PixelDiffusion?")

plt.figure(figsize=(7, 4))
plt.plot(df["epoch"], df["train_loss"], marker="o", label="Train")
plt.plot(df["epoch"], df["val_loss"], marker="o", label="Val")
plt.xlabel("Epoch")
plt.ylabel("MSE Loss")
plt.title("Pixel Diffusion – Loss per Epoch (Ray Train + Lightning)")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()

# 12. Run the trainer again to demonstrate resuming from latest checkpoint  

result = trainer.fit()
print("Training complete →", result.metrics)

# 13. Reverse diffusion sampling

def sample_image(model, steps=50, device="cpu"):
    """Generate an image by iteratively de-noising random noise."""
    model.eval()
    with torch.no_grad():
        img = torch.randn(1, 3, 224, 224, device=device)
        for step in reversed(range(steps)):
            t = torch.tensor([step], device=device)
            pred_noise = model(img, t)
            img = img - pred_noise * 0.1                      # simple Euler update
        # Rescale back to [0,1]
        img = torch.clamp((img * 0.5 + 0.5), 0.0, 1.0)
        return img.squeeze(0).cpu().permute(1,2,0).numpy()

# 14. Generate and display samples

import glob
from ray.train import Checkpoint

assert best_ckpt is not None, "Checkpoint is missing. Did training run and complete?"

# Restore model weights from Ray Train checkpoint (Lightning-first)
model = PixelDiffusion()

with best_ckpt.as_directory() as ckpt_dir:
    # Prefer Lightning checkpoints (*.ckpt) saved by ModelCheckpoint
    ckpt_files = glob.glob(os.path.join(ckpt_dir, "*.ckpt"))
    if ckpt_files:
        pl_ckpt = torch.load(ckpt_files[0], map_location="cpu")
        state = pl_ckpt.get("state_dict", pl_ckpt)
        model.load_state_dict(state, strict=False)
    elif os.path.exists(os.path.join(ckpt_dir, "model.pt")):
        # Fallback for older/manual checkpoints
        state = torch.load(os.path.join(ckpt_dir, "model.pt"), map_location="cpu")
        model.load_state_dict(state, strict=False)
    else:
        raise FileNotFoundError(
            f"No Lightning .ckpt or model.pt found in: {ckpt_dir}"
        )

# Move to device and sample
device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to(device)

# Generate three images
samples = [sample_image(model, steps=50, device=device) for _ in range(3)]

fig, axs = plt.subplots(1, 3, figsize=(9, 3))
for ax, img in zip(axs, samples):
    ax.imshow(img)
    ax.axis("off")
plt.suptitle("Food-101 Diffusion Samples (unconditional)")
plt.tight_layout()
plt.show()

# 15. Cleanup -- delete checkpoints and metrics from model training

TARGET_PATH = "/mnt/cluster_storage/generative_cv"

if os.path.exists(TARGET_PATH):
    shutil.rmtree(TARGET_PATH)
    print(f"✅ Deleted everything under {TARGET_PATH}")
else:
    print(f"⚠️ Path does not exist: {TARGET_PATH}")

