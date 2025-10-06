# 00. Runtime setup — install same deps and set env vars
import os, sys, subprocess

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
import os, io, json, shutil
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
ds = load_dataset("food101", split="train[:10%]")

# 03. Resize + encode as JPEG bytes
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
ds = ds.random_shuffle()
train_ds, val_ds = ds.split_at_indices([train_count])
print("Train rows:", train_ds.count())
print("Val rows:",   val_ds.count())

# 08. Pixel De-noising Diffusion Model

class PixelDiffusion(pl.LightningModule):
    """Tiny CNN that predicts noise ϵ given noisy image + timestep."""

    def __init__(self, max_t=1000, log_path=None):
        super().__init__()
        self.max_t = max_t
        self.log_path = log_path or "/mnt/cluster_storage/generative_cv/epoch_metrics.json"

        # Network: (3 + 1)‑channel input → 3‑channel noise prediction
        self.net = nn.Sequential(
            nn.Conv2d(4, 32, 3, padding=1), nn.ReLU(),
            nn.Conv2d(32, 32, 3, padding=1), nn.ReLU(),
            nn.Conv2d(32, 3, 3, padding=1),
        )
        self.loss_fn = nn.MSELoss()
        self._train_losses, self._val_losses = [], []

    # ---------- forward ----------
    def forward(self, noisy_img, t):
        """noisy_img: Bx3xHxW,  t: B (int) or Bx1 scalar"""
        b, _, h, w = noisy_img.shape
        t_scaled = (t / self.max_t).view(-1, 1, 1, 1).float().to(noisy_img.device)
        t_img = t_scaled.expand(-1, 1, h, w)
        x = torch.cat([noisy_img, t_img], dim=1)  # 4 channels
        return self.net(x)
    
    # ---------- training / validation steps ----------
    def _shared_step(self, batch):
        clean = batch["image"].to(self.device)             # Bx3xHxW, ‑1…1
        noise = torch.randn_like(clean)                    # ϵ ~ N(0, 1)
        t = torch.randint(0, self.max_t, (clean.size(0),), device=self.device)
        noisy = clean + noise                              # x_t = x_0 + ϵ
        pred_noise = self(noisy, t)
        return self.loss_fn(pred_noise, noise)

    def training_step(self, batch, batch_idx):
        loss = self._shared_step(batch)
        self._train_losses.append(loss.item())
        return loss

    def validation_step(self, batch, batch_idx):
        loss = self._shared_step(batch)
        self._val_losses.append(loss.item())
        return loss

    # ---------- epoch end logging ----------
    def on_train_epoch_end(self):
        rank = get_context().get_world_rank()
        if rank == 0:
            train_avg = np.mean(self._train_losses)
            val_avg   = np.mean(self._val_losses) if self._val_losses else None
            if val_avg is not None:
                print(f"[Epoch {self.current_epoch}] train={train_avg:.4f}  val={val_avg:.4f}")
            else:
                print(f"[Epoch {self.current_epoch}] train={train_avg:.4f}  val=N/A")

            # Append to shared JSON so you can plot later
            if os.path.exists(self.log_path):
                with open(self.log_path, "r") as f: logs = json.load(f)
            else:
                logs = []
            logs.append({"epoch": self.current_epoch+1, "train_loss": train_avg, "val_loss": val_avg})
            with open(self.log_path, "w") as f: json.dump(logs, f)

        # Clear per‑epoch trackers
        self._train_losses.clear(); self._val_losses.clear()

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=2e-4)

# 09. Train loop for Ray TorchTrainer

def train_loop(config):
    """Ray Train per-worker function with checkpointing and resume support."""
    import os, torch, uuid, json
    from ray.train import get_checkpoint, get_context, report, Checkpoint

    # Paths
    LOG_PATH = "/mnt/cluster_storage/generative_cv/epoch_metrics.json"
    CKPT_ROOT = "/mnt/cluster_storage/generative_cv/food101_diffusion_ckpts"

    rank = get_context().get_world_rank()
    if rank == 0:
        os.makedirs(CKPT_ROOT, exist_ok=True)
        if not get_checkpoint() and os.path.exists(LOG_PATH):
            os.remove(LOG_PATH)

    # Data
    train_ds = ray.train.get_dataset_shard("train")
    val_ds   = ray.train.get_dataset_shard("val")
    train_loader = train_ds.iter_torch_batches(batch_size=32)
    val_loader   = val_ds.iter_torch_batches(batch_size=32)

    # Model
    model = PixelDiffusion()
    start_epoch = 0

    # Resume from checkpoint if present
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as d:
            model.load_state_dict(torch.load(os.path.join(d, "model.pt"), map_location="cpu"))
            start_epoch = torch.load(os.path.join(d, "meta.pt")).get("epoch", 0) + 1
        if rank == 0:
            print(f"[Rank {rank}] Resumed from checkpoint at epoch {start_epoch}")

    # Trainer
    trainer = pl.Trainer(
        max_epochs=config.get("epochs", 10),
        accelerator="gpu" if torch.cuda.is_available() else "cpu",
        devices=1,
        plugins=[RayLightningEnvironment()],
        enable_progress_bar=False,
        check_val_every_n_epoch=1,
    )

    # Train loop: run each epoch, checkpoint manually
    for epoch in range(start_epoch, config.get("epochs", 10)):
        trainer.fit_loop.max_epochs = epoch + 1
        trainer.fit_loop.current_epoch = epoch
        trainer.fit(model, train_dataloaders=train_loader, val_dataloaders=val_loader)

        if rank == 0:
            # Save model checkpoint
            out_dir = os.path.join(CKPT_ROOT, f"epoch_{epoch}_{uuid.uuid4().hex}")
            os.makedirs(out_dir, exist_ok=True)
            torch.save(model.state_dict(), os.path.join(out_dir, "model.pt"))
            torch.save({"epoch": epoch}, os.path.join(out_dir, "meta.pt"))
            ckpt_out = Checkpoint.from_directory(out_dir)
        else:
            ckpt_out = None

        # Report with checkpoint so Ray saves it
        report({"epoch": epoch}, checkpoint=ckpt_out)

# 10. Launch distributed training

trainer = TorchTrainer(
    train_loop,
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    datasets={"train": train_ds, "val": val_ds},
    run_config=RunConfig(
        name="food101_diffusion_ft",
        storage_path="/mnt/cluster_storage/generative_cv/food101_diffusion_results",
        checkpoint_config=CheckpointConfig(
            checkpoint_frequency=1,
            num_to_keep=5,
            checkpoint_score_attribute="epoch",
            checkpoint_score_order="max",
        ),
        failure_config=FailureConfig(max_failures=3),
    ),
)

result = trainer.fit()
print("Training complete →", result.metrics)
best_ckpt = result.checkpoint  # checkpoint from highest reported epoch (you can change score attr)

# 11. Plot train/val loss curves

LOG_PATH = "/mnt/cluster_storage/generative_cv/epoch_metrics.json"
with open(LOG_PATH, "r") as f:
    logs = json.load(f)

df = pd.DataFrame(logs)
df["val_loss"] = pd.to_numeric(df["val_loss"], errors="coerce")

plt.figure(figsize=(7,4))
plt.plot(df["epoch"], df["train_loss"], marker="o", label="Train")
plt.plot(df["epoch"], df["val_loss"],   marker="o", label="Val")
plt.xlabel("Epoch"); plt.ylabel("MSE Loss"); plt.title("Pixel Diffusion - Loss per Epoch")
plt.grid(True); plt.legend(); plt.tight_layout(); plt.show()

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

# Load model from Ray Train checkpoint
from ray.train import Checkpoint

assert best_ckpt is not None, "Checkpoint is missing. Did training run and complete?"

with best_ckpt.as_directory() as ckpt_dir:
    model = PixelDiffusion()
    model.load_state_dict(torch.load(os.path.join(ckpt_dir, "model.pt"), map_location="cpu"))

model = model.to("cuda" if torch.cuda.is_available() else "cpu")

# Generate three images
samples = [sample_image(model, steps=50, device=model.device) for _ in range(3)]

fig, axs = plt.subplots(1, 3, figsize=(9, 3))
for ax, img in zip(axs, samples):
    ax.imshow(img)
    ax.axis("off")
plt.suptitle("Food‑101 Diffusion Samples (unconditional)")
plt.tight_layout()
plt.show()

# 15. Cleanup -- delete checkpoints and metrics from model training

TARGET_PATH = "/mnt/cluster_storage/generative_cv"

if os.path.exists(TARGET_PATH):
    shutil.rmtree(TARGET_PATH)
    print(f"✅ Deleted everything under {TARGET_PATH}")
else:
    print(f"⚠️ Path does not exist: {TARGET_PATH}")

