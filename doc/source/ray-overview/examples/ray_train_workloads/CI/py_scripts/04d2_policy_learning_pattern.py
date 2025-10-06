# 00. Runtime setup — install same deps as build.sh and set env vars
import os, sys, subprocess

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
import os, shutil
import json
import uuid
import numpy as np
import matplotlib.pyplot as plt
import pandas as pd
import gymnasium as gym

# Ray libraries for distributed data and training
import ray
import ray.data
from ray.train.lightning import RayLightningEnvironment  # Make sure RAY_TRAIN_V2_ENABLED=1 in "Environment variables"
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
    def __init__(self, obs_dim: int = 3, act_dim: int = 1, max_t: int = 1000,
                 log_path: str = "/mnt/cluster_storage/pendulum_diffusion/epoch_metrics.json"):
        super().__init__()
        self.max_t   = max_t
        self.log_path = log_path

        # 3D obs  + 1-D action  + 1 timestep  → 1-D noise
        self.net = nn.Sequential(
            nn.Linear(obs_dim + act_dim + 1, 128),
            nn.ReLU(),
            nn.Linear(128, act_dim),
        )
        self.loss_fn = nn.MSELoss()
        self._train_losses, self._val_losses = [], []

    # ---------- forward ----------
    def forward(self, obs, noisy_action, timestep):
        t = timestep.view(-1, 1).float() / self.max_t
        x = torch.cat([obs, noisy_action, t], dim=1)
        return self.net(x)

    # ---------- shared step ----------
    def _shared_step(self, batch):
        pred = self.forward(batch["obs"].float(),
                            batch["noisy_action"],
                            batch["timestep"])
        return self.loss_fn(pred, batch["noise"])

    def training_step(self, batch, _):
        loss = self._shared_step(batch)
        self._train_losses.append(loss.item())
        return loss

    def validation_step(self, batch, _):
        loss = self._shared_step(batch)
        self._val_losses.append(loss.item())
        return loss

    # ---------- epoch-end ----------
    def on_train_epoch_end(self):
        rank = get_context().get_world_rank()
        if rank == 0:
            tr_avg = float(np.mean(self._train_losses))
            va_avg = float(np.mean(self._val_losses)) if self._val_losses else None
            print(f"[Epoch {self.current_epoch}] "
                  f"train={tr_avg:.4f}  val={va_avg if va_avg is not None else 'N/A'}")

            os.makedirs(os.path.dirname(self.log_path), exist_ok=True)
            logs = []
            if os.path.exists(self.log_path):
                with open(self.log_path, "r") as f:
                    logs = json.load(f)
            logs.append({"epoch": self.current_epoch+1,
                         "train_loss": tr_avg,
                         "val_loss": va_avg})
            with open(self.log_path, "w") as f:
                json.dump(logs, f)

        self._train_losses.clear(); self._val_losses.clear()

    def configure_optimizers(self):
        return torch.optim.Adam(self.parameters(), lr=1e-3)

# 05. Training loop

# Training function that runs on each Ray worker
def train_loop(config):

    # ---------- Paths for logs & checkpoints ----------
    LOG_PATH  = "/mnt/cluster_storage/pendulum_diffusion/epoch_metrics.json"
    CKPT_ROOT = "/mnt/cluster_storage/pendulum_diffusion/pendulum_diffusion_ckpts"

    rank = get_context().get_world_rank()

    # Create log/checkpoint dirs on rank 0
    if rank == 0:
        os.makedirs(CKPT_ROOT, exist_ok=True)
        if not get_checkpoint() and os.path.exists(LOG_PATH):
            os.remove(LOG_PATH)

    # ---------- Load Ray Dataset shards ----------
    train_ds = ray.train.get_dataset_shard("train")
    val_ds   = ray.train.get_dataset_shard("val")

    # ---------- Instantiate model ----------
    model = DiffusionPolicy()
    start_epoch = 0

    # ---------- Resume from checkpoint ----------
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as d:
            model.load_state_dict(torch.load(os.path.join(d, "model.pt"), map_location="cpu"))
            start_epoch = torch.load(os.path.join(d, "meta.pt")).get("epoch", 0) + 1
        if rank == 0:
            print(f"[Rank {rank}] Resumed from checkpoint at epoch {start_epoch}")

    # ---------- Lightning Trainer ----------
    trainer = pl.Trainer(
        max_epochs=config.get("epochs", 10),
        accelerator="gpu" if torch.cuda.is_available() else "cpu",
        devices=1,
        plugins=[RayLightningEnvironment()],
        enable_progress_bar=False,
        check_val_every_n_epoch=1,
    )

    # ---------- Training Loop ----------
    for epoch in range(start_epoch, config.get("epochs", 10)):

        # Re-materialize fresh batches each epoch (avoid stale iterator errors)
        train_data = list(train_ds.iter_torch_batches(
            batch_size=32,
            local_shuffle_buffer_size=1024,
            prefetch_batches=1,
            drop_last=True
        ))
        val_data = list(val_ds.iter_torch_batches(
            batch_size=32,
            local_shuffle_buffer_size=1024,
            prefetch_batches=1,
            drop_last=True
        ))

        # Wrap lists in PyTorch DataLoaders
        train_loader = DataLoader(train_data, batch_size=None)
        val_loader   = DataLoader(val_data, batch_size=None)

        # Run one epoch (advance trainer manually)
        trainer.fit_loop.max_epochs = epoch + 1
        trainer.fit_loop.current_epoch = epoch
        trainer.fit(model, train_dataloaders=train_loader, val_dataloaders=val_loader)

        # ---------- Save checkpoint ----------
        if rank == 0:
            out_dir = os.path.join(CKPT_ROOT, f"epoch_{epoch}_{uuid.uuid4().hex}")
            os.makedirs(out_dir, exist_ok=True)
            torch.save(model.state_dict(), os.path.join(out_dir, "model.pt"))
            torch.save({"epoch": epoch}, os.path.join(out_dir, "meta.pt"))
            ckpt_out = Checkpoint.from_directory(out_dir)
        else:
            ckpt_out = None

        # Report metrics + checkpoint back to Ray Train
        report({"epoch": epoch}, checkpoint=ckpt_out)

# 06. Launch Ray Trainer

# Configure Ray TorchTrainer to run the distributed training job
trainer = TorchTrainer(
    train_loop,
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    datasets={"train": train_ds, "val": val_ds},
    run_config=RunConfig(
        name="pendulum_diffusion_ft",
        storage_path="/mnt/cluster_storage/pendulum_diffusion/pendulum_diffusion_results",
        checkpoint_config=CheckpointConfig(
            checkpoint_frequency=1,       # save every epoch
            num_to_keep=5,
            checkpoint_score_attribute="epoch",
            checkpoint_score_order="max",
        ),
        failure_config=FailureConfig(max_failures=3),
    ),
)

result = trainer.fit()
print("Training complete →", result.metrics)
best_ckpt = result.checkpoint        # Checkpoint from last reported epoch

# 07. Plot training and validation loss

# Load training logs from shared file and plot losses
log_path = "/mnt/cluster_storage/pendulum_diffusion/epoch_metrics.json"

with open(log_path, "r") as f:
    logs = json.load(f)

# Convert logs to pandas DataFrame
df = pd.DataFrame(logs)
df["val_loss"] = pd.to_numeric(df["val_loss"], errors="coerce")  # handle None
df_grouped = df.groupby("epoch", as_index=False).mean(numeric_only=True)


# Plot training and validation loss curves
plt.figure(figsize=(8, 5))
plt.plot(df_grouped["epoch"], df_grouped["train_loss"], marker="o", label="Train Loss")
plt.plot(df_grouped["epoch"], df_grouped["val_loss"], marker="o", label="Val Loss")
plt.title("Training & Validation Loss per Epoch")
plt.xlabel("Epoch")
plt.ylabel("Loss")
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

# 09. In-notebook sampling from trained model

# A plausible pendulum state: [cos(theta), sin(theta), theta_dot]
obs_sample = torch.tensor([1.0, 0.0, 0.0], dtype=torch.float32)   # shape (3,)

# Load the most recent model checkpoint from the checkpoint directory
CKPT_DIR = "/mnt/cluster_storage/pendulum_diffusion/pendulum_diffusion_ckpts"

# Pick latest by sorted creation time (or filename if using uuid naming)
latest = sorted(os.listdir(CKPT_DIR))[-1]
model_path = os.path.join(CKPT_DIR, latest, "model.pt")

model = DiffusionPolicy(obs_dim=3, act_dim=1)
model.load_state_dict(torch.load(model_path, map_location="cpu"))
model = model.to("cuda" if torch.cuda.is_available() else "cpu")

# Run reverse diffusion sampling
action = sample_action(model, obs_sample, n_steps=50, device="cpu")
print("Sampled action:", action)

# 10. Cleanup -- delete checkpoints and metrics from model training

TARGET_PATH = "/mnt/cluster_storage/pendulum_diffusion"

if os.path.exists(TARGET_PATH):
    shutil.rmtree(TARGET_PATH)
    print(f"✅ Deleted everything under {TARGET_PATH}")
else:
    print(f"⚠️ Path does not exist: {TARGET_PATH}")

