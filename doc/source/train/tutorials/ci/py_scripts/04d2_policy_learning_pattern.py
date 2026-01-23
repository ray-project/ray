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
    "matplotlib==3.10.6",
    "lightning==2.5.5",
    "pyarrow==14.0.2",
])

# 01. Imports

# Standard Python packages for math, plotting, and data handling
import os
import shutil
import glob
import json
import uuid
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import gymnasium as gym

# Ray libraries for distributed data and training
import ray
import ray.data
from ray.train.lightning import RayLightningEnvironment  
from ray.train import ScalingConfig, RunConfig, FailureConfig, CheckpointConfig, get_context, get_checkpoint, report, Checkpoint
from ray.train.torch import TorchTrainer

# PyTorch Lightning and base PyTorch for model definition and training
import lightning.pytorch as pl
import torch
from torch.utils.data import DataLoader
from torch import nn

# 02. Generate Pendulum offline dataset 

def make_pendulum_dataset(n_steps: int = 10_000):
    """
    Roll out a random policy in Pendulum-v1 and log (obs, noisy_action, noise, timestep).
    Returns a Ray Dataset ready for sharding.
    """
    env = gym.make("Pendulum-v1")
    obs, _ = env.reset(seed=0)
    data = []

    for _ in range(n_steps):
        action = env.action_space.sample().astype(np.float32)      # shape (1,)
        noise   = np.random.randn(*action.shape).astype(np.float32)
        noisy_action = action + noise                              # add Gaussian noise
        timestep = np.random.randint(0, 1000, dtype=np.int64)

        data.append(
            {
                "obs":        obs.astype(np.float32),              # shape (3,)
                "noisy_action": noisy_action,                      # shape (1,)
                "noise":        noise,                             # shape (1,)
                "timestep":     timestep,
            }
        )

        # Step environment
        obs, _, terminated, truncated, _ = env.step(action)
        if terminated or truncated:
            obs, _ = env.reset()

    return ray.data.from_items(data)

ds = make_pendulum_dataset()

# 03. Normalize and split (vector obs ∈ [-π, π])

# Normalize pixel values from [0, 1] to [-1, 1] for training
def normalize(batch):
    # Pendulum observations are roughly in [-π, π] → scale to [-1, 1]
    batch["obs"] = batch["obs"] / np.pi
    return batch

# Apply normalization in parallel using Ray Data
ds = ds.map_batches(normalize, batch_format="numpy")

# Count total number of items (triggers actual execution)
total = ds.count()
print("Total dataset size:", total)

# Shuffle and split dataset into 80% training and 20% validation
split_idx = int(total * 0.8)
ds = ds.random_shuffle()
train_ds, val_ds = ds.split_at_indices([split_idx])

print("Train size:", train_ds.count())
print("Val size:", val_ds.count())

# 04. DiffusionPolicy for low-dim observation (3D) and action (1D)

class DiffusionPolicy(pl.LightningModule):
    """Tiny MLP that predicts injected noise ϵ given (obs, noisy_action, timestep)."""

    def __init__(self, obs_dim: int = 3, act_dim: int = 1, max_t: int = 1000):
        super().__init__()
        self.max_t = max_t

        # 3D obs + 1D action + 1 timestep → 1D noise
        self.net = nn.Sequential(
            nn.Linear(obs_dim + act_dim + 1, 128),
            nn.ReLU(),
            nn.Linear(128, act_dim),
        )
        self.loss_fn = nn.MSELoss()

    # ---------- forward ----------
    def forward(self, obs, noisy_action, timestep):
        t = timestep.view(-1, 1).float() / self.max_t
        x = torch.cat([obs, noisy_action, t], dim=1)
        return self.net(x)

    # ---------- shared loss ----------
    def _shared_step(self, batch):
        pred = self.forward(
            batch["obs"].float(),
            batch["noisy_action"],
            batch["timestep"],
        )
        return self.loss_fn(pred, batch["noise"])

    # ---------- training / validation ----------
    def training_step(self, batch, batch_idx):
        loss = self._shared_step(batch)
        self.log("train_loss", loss, on_epoch=True, prog_bar=False, sync_dist=True)
        return loss

    def validation_step(self, batch, batch_idx):
        loss = self._shared_step(batch)
        self.log("val_loss", loss, on_epoch=True, prog_bar=False, sync_dist=True)
        return loss

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=1e-3)

# 05. Ray Train Lightning-native training loop

def train_loop(config):
    import os, tempfile, torch, warnings
    import lightning.pytorch as pl
    from ray.train import get_checkpoint, get_context
    from ray.train.lightning import (
        RayLightningEnvironment,
        RayDDPStrategy,
        RayTrainReportCallback,
        prepare_trainer,
    )

    warnings.filterwarnings(
        "ignore", message="barrier.*using the device under current context"
    )

    # ---- Ray Dataset shards → iterable torch batches ----
    train_ds = ray.train.get_dataset_shard("train")
    val_ds   = ray.train.get_dataset_shard("val")
    train_loader = train_ds.iter_torch_batches(batch_size=config.get("batch_size", 32))
    val_loader   = val_ds.iter_torch_batches(batch_size=config.get("batch_size", 32))

    # ---- Model ----
    model = DiffusionPolicy()

    # ---- Local scratch for PL checkpoints (Ray will persist to storage_path) ----
    CKPT_ROOT = os.path.join(tempfile.gettempdir(), "ray_pl_ckpts")
    os.makedirs(CKPT_ROOT, exist_ok=True)

    # ---- Lightning Trainer configured for Ray ----
    trainer = pl.Trainer(
        max_epochs=config.get("epochs", 10),
        devices="auto",
        accelerator="auto",
        strategy=RayDDPStrategy(),
        plugins=[RayLightningEnvironment()],
        callbacks=[
            RayTrainReportCallback(),       # forwards metrics + ckpt to Ray
            pl.callbacks.ModelCheckpoint(   # local PL checkpoints each epoch
                dirpath=CKPT_ROOT,
                filename="epoch-{epoch:03d}",
                every_n_epochs=1,
                save_top_k=-1,
                save_last=True,
            ),
        ],
        default_root_dir=CKPT_ROOT,
        enable_progress_bar=False,
        check_val_every_n_epoch=1,
    )

    # ---- Prepare trainer for Ray environment ----
    trainer = prepare_trainer(trainer)

    # ---- Resume from Ray checkpoint if available ----
    ckpt_path = None
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as d:
            candidate = os.path.join(d, "checkpoint.ckpt")
            if os.path.exists(candidate):
                ckpt_path = candidate
                if get_context().get_world_rank() == 0:
                    print(f"✅ Resuming from Lightning checkpoint: {ckpt_path}")

    # ---- Run training (Lightning owns the loop) ----
    trainer.fit(model, train_dataloaders=train_loader, val_dataloaders=val_loader, ckpt_path=ckpt_path)

# 06. Launch distributed training with Ray TorchTrainer

trainer = TorchTrainer(
    train_loop,
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    datasets={"train": train_ds, "val": val_ds},
    run_config=RunConfig(
        name="pendulum_diffusion_ft",
        storage_path="/mnt/cluster_storage/pendulum_diffusion/pendulum_diffusion_results",
        checkpoint_config=CheckpointConfig(
            num_to_keep=5,
            checkpoint_score_attribute="epoch",
            checkpoint_score_order="max",
        ),
        failure_config=FailureConfig(max_failures=3),
    ),
)

result = trainer.fit()
print("Training complete →", result.metrics)
best_ckpt = result.checkpoint  # latest Ray-managed Lightning checkpoint

# 07. Plot training and validation loss (Ray + Lightning integration)

df = result.metrics_dataframe
print(df.head())  # optional sanity check

if "train_loss" not in df.columns or "val_loss" not in df.columns:
    raise ValueError("train_loss / val_loss missing. Did you log them via self.log()?")

plt.figure(figsize=(7, 4))
plt.plot(df["epoch"], df["train_loss"], marker="o", label="Train")
plt.plot(df["epoch"], df["val_loss"], marker="o", label="Val")
plt.xlabel("Epoch")
plt.ylabel("MSE Loss")
plt.title("Pendulum Diffusion - Loss per Epoch (Ray Train + Lightning)")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()

# 08. Reverse diffusion sampling for 1-D action

# Function to simulate reverse diffusion process
def sample_action(model, obs, n_steps=50, device="cpu"):
    """
    Runs reverse diffusion starting from noise to generate a Pendulum action.
    obs: torch.Tensor of shape (3,)
    returns: torch.Tensor of shape (1,)
    """
    model.eval()
    with torch.no_grad():
        obs = obs.unsqueeze(0).to(device)      # [1, 3]
        obs = obs / np.pi                      # Same normalization used in training

        x = torch.randn(1, 1).to(device)       # Start from noise in action space

        for step in reversed(range(n_steps)):
            t = torch.tensor([step], device=device)
            pred_noise = model(obs, x, t)
            x = x - pred_noise * 0.1

        return x.squeeze(0)

# 09. In-notebook sampling from trained model (Ray Lightning checkpoint)

# A plausible pendulum state: [cos(theta), sin(theta), theta_dot]
obs_sample = torch.tensor([1.0, 0.0, 0.0], dtype=torch.float32)   # shape (3,)

assert best_ckpt is not None, "No checkpoint found — did training complete successfully?"

# Load the trained model from Ray's latest Lightning checkpoint
model = DiffusionPolicy(obs_dim=3, act_dim=1)

with best_ckpt.as_directory() as ckpt_dir:
    # RayTrainReportCallback saves a file named "checkpoint.ckpt"
    ckpt_file = os.path.join(ckpt_dir, "checkpoint.ckpt")
    if not os.path.exists(ckpt_file):
        # Fallback: search any .ckpt file if name differs
        candidates = glob.glob(os.path.join(ckpt_dir, "*.ckpt"))
        ckpt_file = candidates[0] if candidates else None

    assert ckpt_file is not None, f"No Lightning checkpoint found in {ckpt_dir}"
    state = torch.load(ckpt_file, map_location="cpu")
    model.load_state_dict(state.get("state_dict", state), strict=False)

# Move to device
device = "cuda" if torch.cuda.is_available() else "cpu"
model = model.to(device)

# Run reverse diffusion sampling
action = sample_action(model, obs_sample, n_steps=50, device=device)
print("Sampled action:", action)

# 10. Cleanup -- delete checkpoints and metrics from model training

TARGET_PATH = "/mnt/cluster_storage/pendulum_diffusion"

if os.path.exists(TARGET_PATH):
    shutil.rmtree(TARGET_PATH)
    print(f"✅ Deleted everything under {TARGET_PATH}")
else:
    print(f"⚠️ Path does not exist: {TARGET_PATH}")

