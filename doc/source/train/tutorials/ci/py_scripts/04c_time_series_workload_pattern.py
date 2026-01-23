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
    "pyarrow==14.0.2",
    "datasets==2.19.2",
])

# 01. Imports
import os
import io
import math
import uuid
import shutil
import random
import requests
import sys
from pathlib import Path
from datetime import datetime, timedelta
from datasets import load_dataset   

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import matplotlib.pyplot as plt

import torch
import torch.nn as nn
from torch.utils.data import Dataset, DataLoader
import torch.optim as optim

import ray
import ray.data as rdata
import ray.train as train
from ray.train import (
    ScalingConfig, RunConfig, FailureConfig,
    CheckpointConfig, Checkpoint, get_checkpoint, get_context
)
from ray.train.torch import prepare_model, prepare_data_loader, TorchTrainer

# 02. Load NYC taxi passenger counts (30-min) from GitHub raw – no auth, ~1 MB

DATA_DIR = "/mnt/cluster_storage/nyc_taxi_ts"
os.makedirs(DATA_DIR, exist_ok=True)

url = "https://raw.githubusercontent.com/numenta/NAB/master/data/realKnownCause/nyc_taxi.csv"
csv_path = os.path.join(DATA_DIR, "nyc_taxi.csv")

if not os.path.exists(csv_path):
    print("Downloading nyc_taxi.csv …")
    df = pd.read_csv(url)
    df.to_csv(csv_path, index=False)
else:
    print("File already present.")
    df = pd.read_csv(csv_path)

# Parse timestamp and tidy
df["timestamp"] = pd.to_datetime(df["timestamp"])
df = df.set_index("timestamp").rename(columns={"value": "passengers"})

print("Rows:", len(df), "| Time span:", df.index.min(), "→", df.index.max())
df.head()

# 03. Resample to hourly, then normalize
hourly = df.resample("30min").mean()

mean, std = hourly["passengers"].mean(), hourly["passengers"].std()
hourly["norm"] = (hourly["passengers"] - mean) / std

print(f"Half-Hourly rows: {len(hourly)}  |  mean={mean:.1f}, std={std:.1f}")
hourly.head()

# 04. Quick visual sanity-check — first two weeks
plt.figure(figsize=(10, 4))
hourly["passengers"].iloc[:24*14].plot()
plt.title("NYC-Taxi passengers - first 2 weeks of 2014")
plt.ylabel("# trips in hour")
plt.grid(True)
plt.tight_layout()
plt.show()

# 05. Build sliding-window dataset and write to Parquet
# ----------------------------------------------------
INPUT_WINDOW = 24 * 7   # 1/2 week history (in 30-min steps = 168)
HORIZON      = 48       # predict next 24 h
STRIDE       = 12       # slide 6 hours at a time

values = hourly["norm"].to_numpy(dtype="float32")  # already normalised

# ---- Time-aware split to avoid leakage between train and val ----
cut = int(0.9 * len(values))  # split by time index on the original series
train_records, val_records = [], []

for s in range(0, len(values) - INPUT_WINDOW - HORIZON + 1, STRIDE):
    past   = values[s : s + INPUT_WINDOW]
    future = values[s + INPUT_WINDOW : s + INPUT_WINDOW + HORIZON]
    end    = s + INPUT_WINDOW + HORIZON  # last index consumed by this window

    rec = {
        "series_id": 0,
        "past":  past.tolist(),
        "future": future.tolist(),
    }

    if end <= cut:         # Entire window ends before the cut to train
        train_records.append(rec)
    elif s >= cut:         # Window starts after the cut to val
        val_records.append(rec)
    # else: window crosses the cut to drop to prevent leakage

print(f"Windows → train: {len(train_records)}, val: {len(val_records)}")

# Write to Parquet
DATA_DIR     = "/mnt/cluster_storage/nyc_taxi_ts"
PARQUET_DIR  = os.path.join(DATA_DIR, "parquet")
os.makedirs(PARQUET_DIR, exist_ok=True)

schema = pa.schema([
    ("series_id", pa.int32()),
    ("past",  pa.list_(pa.float32())),
    ("future", pa.list_(pa.float32()))
])

def write_parquet(records, fname):
    pq.write_table(pa.Table.from_pylist(records, schema=schema), fname, version="2.6")

write_parquet(train_records, os.path.join(PARQUET_DIR, "train.parquet"))
write_parquet(val_records,   os.path.join(PARQUET_DIR, "val.parquet"))
print("Parquet shards written →", PARQUET_DIR)

# 06. PyTorch Dataset that reads the Parquet shards

class TaxiWindowDataset(Dataset):
    def __init__(self, parquet_path):
        self.table  = pq.read_table(parquet_path)
        self.past   = self.table.column("past").to_pylist()
        self.future = self.table.column("future").to_pylist()

    def __len__(self):
        return len(self.past)

    def __getitem__(self, idx):
        past   = torch.tensor(self.past[idx],   dtype=torch.float32).unsqueeze(-1)   # (T, 1)
        future = torch.tensor(self.future[idx], dtype=torch.float32)                 # (H,)
        return past, future

# 07. Inspect one random batch
loader = DataLoader(TaxiWindowDataset(os.path.join(PARQUET_DIR, "train.parquet")),
                    batch_size=4, shuffle=True)
xb, yb = next(iter(loader))
print("Past:", xb.shape, "Future:", yb.shape)

# 08. Helper to build Ray-prepared DataLoader
from ray.train.torch import prepare_data_loader

def build_dataloader(parquet_path, batch_size, shuffle=True):
    ds = TaxiWindowDataset(parquet_path)
    loader = DataLoader(
        ds, batch_size=batch_size, shuffle=shuffle, num_workers=2, drop_last=False,
    )
    return prepare_data_loader(loader)

# 09. PositionalEncoding and Transformer model (univariate)

class PositionalEncoding(nn.Module):
    def __init__(self, d_model, dropout=0.2, max_len=1024):
        super().__init__()
        self.dropout = nn.Dropout(dropout)
        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float32).unsqueeze(1)
        div_term = torch.exp(
            torch.arange(0, d_model, 2, dtype=torch.float32) * -(math.log(10000.0) / d_model)
        )
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        self.register_buffer("pe", pe.unsqueeze(0))

    def forward(self, x):
        return self.dropout(x + self.pe[:, : x.size(1)])

class TimeSeriesTransformer(nn.Module):
    def __init__(self, input_window, horizon, d_model=64, nhead=8, num_layers=2):
        super().__init__()
        self.horizon  = horizon
        self.d_model  = d_model

        self.in_proj  = nn.Linear(1, d_model)
        self.pos_enc  = PositionalEncoding(d_model)
        self.tr_model = nn.Transformer(
            d_model=d_model,
            nhead=nhead,
            num_encoder_layers=num_layers,
            num_decoder_layers=num_layers,
            batch_first=True,
        )
        self.out_proj = nn.Linear(d_model, 1)

    def forward(self, past, decoder_input=None):
        """
        Args:
            past           : (B, T, 1)    — encoder input
            decoder_input  : (B, F, 1)    — optional decoder input (teacher forcing)
        Returns:
            preds          : (B, F)       — predicted future values
        """
        B = past.size(0)

        # Encoder input
        src = self.in_proj(past) * math.sqrt(self.d_model)
        src = self.pos_enc(src)

        # Decoder input
        if decoder_input is None:
            decoder_input = past[:, -1:, :].repeat(1, self.horizon, 1)

        tgt = self.in_proj(decoder_input) * math.sqrt(self.d_model)
        tgt = self.pos_enc(tgt)

        # Transformer forward
        output = self.tr_model(src, tgt)  # shape: (B, F, d_model)
        return self.out_proj(output).squeeze(-1)  # shape: (B, F)

# 10. Ray Train train_loop_per_worker with checkpointing, teacher forcing, and clean structure

def train_loop_per_worker(config):
    import tempfile
    import torch
    import torch.nn as nn
    import torch.optim as optim
    from ray import train
    from ray.train import Checkpoint, get_context
    from ray.train import get_checkpoint

    torch.manual_seed(0)

    # ─────────────────────────────────────────────────────────────
    # 1) Model (DDP-prepared)
    # ─────────────────────────────────────────────────────────────
    model = TimeSeriesTransformer(
        input_window=INPUT_WINDOW,
        horizon=HORIZON,
        d_model=config["d_model"],
        nhead=config["nhead"],
        num_layers=config["num_layers"],
    )
    model = train.torch.prepare_model(model)

    # ─────────────────────────────────────────────────────────────
    # 2) Optimizer / Loss
    # ─────────────────────────────────────────────────────────────
    optimizer = optim.Adam(model.parameters(), lr=config["lr"])
    loss_fn = nn.SmoothL1Loss()

    # ─────────────────────────────────────────────────────────────
    # 3) Resume from checkpoint (if provided by Ray)
    # ─────────────────────────────────────────────────────────────
    rank = get_context().get_world_rank()
    start_epoch = 0
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as ckpt_dir:
            # Safe CPU load in case of device mismatch on resume
            model.load_state_dict(torch.load(os.path.join(ckpt_dir, "model.pt"), map_location="cpu"))
            opt_state_path = os.path.join(ckpt_dir, "optim.pt")
            if os.path.exists(opt_state_path):
                optimizer.load_state_dict(torch.load(opt_state_path, map_location="cpu"))
            meta = torch.load(os.path.join(ckpt_dir, "meta.pt"))
            start_epoch = int(meta.get("epoch", -1)) + 1
        if rank == 0:
            print(f"[Rank {rank}] ✅ Resumed from checkpoint at epoch {start_epoch}")

    # ─────────────────────────────────────────────────────────────
    # 4) Dataloaders for this worker
    # ─────────────────────────────────────────────────────────────
    train_loader = build_dataloader(
        os.path.join(PARQUET_DIR, "train.parquet"),
        batch_size=config["bs"],
        shuffle=True,
    )
    val_loader = build_dataloader(
        os.path.join(PARQUET_DIR, "val.parquet"),
        batch_size=config["bs"],
        shuffle=False,
    )

    # ─────────────────────────────────────────────────────────────
    # 5) Epoch loop
    # ─────────────────────────────────────────────────────────────
    for epoch in range(start_epoch, config["epochs"]):
        # ---- Train ----
        model.train()
        train_loss_sum = 0.0
        for past, future in train_loader:
            optimizer.zero_grad()

            # Teacher forcing: shift future targets to use as decoder input
            future = future.unsqueeze(-1)                                # (B, F, 1)
            start_token = torch.zeros_like(future[:, :1])                # (B, 1, 1)
            decoder_input = torch.cat([start_token, future[:, :-1]], 1)  # (B, F, 1)

            pred = model(past, decoder_input)                            # (B, F)
            loss = loss_fn(pred, future.squeeze(-1))                     # (B, F) vs (B, F)

            loss.backward()
            optimizer.step()
            train_loss_sum += float(loss.item())

        avg_train_loss = train_loss_sum / max(1, len(train_loader))

        # ---- Validate ----
        model.eval()
        val_loss_sum = 0.0
        with torch.no_grad():
            for past, future in val_loader:
                pred = model(past)                                       # zeros-as-decoder-input path
                loss = loss_fn(pred, future)
                val_loss_sum += float(loss.item())
        avg_val_loss = val_loss_sum / max(1, len(val_loader))

        if rank == 0:
            print({"epoch": epoch, "train_loss": avg_train_loss, "val_loss": avg_val_loss})

        metrics = {
            "epoch": epoch,
            "train_loss": avg_train_loss,
            "val_loss": avg_val_loss,
        }

        # ─────────────────────────────────────────────────────────────
        # 6) Report + temp checkpoint (rank 0 attaches; others metrics-only)
        # ─────────────────────────────────────────────────────────────
        if rank == 0:
            with tempfile.TemporaryDirectory() as tmpdir:
                torch.save(model.state_dict(), os.path.join(tmpdir, "model.pt"))
                torch.save(optimizer.state_dict(), os.path.join(tmpdir, "optim.pt"))
                torch.save({"epoch": epoch}, os.path.join(tmpdir, "meta.pt"))
                ckpt_out = Checkpoint.from_directory(tmpdir)
                train.report(metrics, checkpoint=ckpt_out)
        else:
            train.report(metrics, checkpoint=None)

# 11. Launch training

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={"lr": 1e-3, "bs": 4, "epochs": 20,
                       "d_model": 128, "nhead": 4, "num_layers": 3},
    scaling_config=ScalingConfig(num_workers=8, use_gpu=True),
    run_config=RunConfig(
        name="nyc_taxi_transformer",
        storage_path=os.path.join(DATA_DIR, "results"),  
        checkpoint_config=CheckpointConfig(
            num_to_keep=20,
            # Let your loop decide when to checkpoint (each epoch). Scoring still applies.
            checkpoint_score_attribute="val_loss",
            checkpoint_score_order="min",
            # (Optional) If you want the last epoch’s checkpoint regardless of score:
            # checkpoint_at_end=True,
        ),
        failure_config=FailureConfig(max_failures=3),
    ),
)

result = trainer.fit()
print("Final metrics:", result.metrics)

# Best checkpoint (by val_loss) thanks to checkpoint_score_* above:
best_ckpt = result.checkpoint

# 12. Plot train/val loss curves (from Ray Train results)

# Pull full metrics history Ray stored for this run
df = result.metrics_dataframe.copy()

# Keep only relevant columns (defensive in case Ray adds extras)
cols = [c for c in ["epoch", "train_loss", "val_loss"] if c in df.columns]
df = df[cols].dropna()

# If multiple reports per epoch exist, keep the latest one
if "epoch" in df.columns:
    df = df.sort_index().groupby("epoch", as_index=False).last()

# Plot
plt.figure(figsize=(7, 4))
if "train_loss" in df.columns:
    plt.plot(df["epoch"], df["train_loss"], marker="o", label="Train")
if "val_loss" in df.columns:
    plt.plot(df["epoch"], df["val_loss"], marker="o", label="Val")

plt.xlabel("Epoch")
plt.ylabel("SmoothL1 Loss")
plt.title("TimeSeriesTransformer — Train vs. Val Loss")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()

# 13. Demonstrate fault-tolerant resume
result = trainer.fit()
print("Metrics after resume run:", result.metrics)

# 14. Ray Data inference helper — stateful per-actor predictor

class TimeSeriesBatchPredictor:
    """
    Keeps the TimeSeriesTransformer in memory per actor (GPU if available).
    Expects a Pandas batch with a 'past' column containing np.ndarray of shape (INPUT_WINDOW,).
    Returns a batch with a 'pred' column (np.ndarray of shape (HORIZON,)).
    """
    def __init__(self, checkpoint_path: str, model_kwargs: dict):
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        # Recreate model with the *same* hyperparams used during training
        self.model = TimeSeriesTransformer(
            input_window=model_kwargs["input_window"],
            horizon=model_kwargs["horizon"],
            d_model=model_kwargs["d_model"],
            nhead=model_kwargs["nhead"],
            num_layers=model_kwargs["num_layers"],
        ).to(self.device).eval()

        # Load checkpoint weights once per actor
        ckpt = Checkpoint.from_directory(checkpoint_path)
        with ckpt.as_directory() as ckpt_dir:
            state_dict = torch.load(os.path.join(ckpt_dir, "model.pt"), map_location="cpu")
            # Strip DDP prefix if present
            state_dict = {k.replace("module.", "", 1): v for k, v in state_dict.items()}
            self.model.load_state_dict(state_dict)

        torch.set_grad_enabled(False)

    def __call__(self, batch):
        import pandas as pd

        past_list = batch["past"]  # each entry: np.ndarray shape (INPUT_WINDOW,)
        # Stack into (B, T, 1)
        x = np.stack([p.astype(np.float32) for p in past_list], axis=0)
        x = torch.from_numpy(x).unsqueeze(-1).to(self.device)  # (B, INPUT_WINDOW, 1)

        # Inference path uses the model's "zeros as decoder input" forward
        preds = self.model(x).detach().cpu().numpy()  # (B, HORIZON)

        out = batch.copy()
        out["pred"] = list(preds)  # each row: np.ndarray (HORIZON,)
        return out[["pred"]]

# 15. Run inference on the latest window with Ray Data and plot

# 1) Prepare the latest window on the driver
past_norm = hourly["norm"].iloc[-INPUT_WINDOW:].to_numpy().astype(np.float32)
future_true = hourly["passengers"].iloc[-HORIZON:].to_numpy()  # for visualization only

# 2) Get the best checkpoint directory selected by Ray
with result.checkpoint.as_directory() as ckpt_dir:
    best_ckpt_path = ckpt_dir  # path visible to workers

# 3) Build a tiny Ray Dataset and run inference on a GPU actor
model_kwargs = {
    "input_window": INPUT_WINDOW,
    "horizon": HORIZON,
    "d_model": 128,
    "nhead": 4,
    "num_layers": 3,
}

ds = rdata.from_items([{"past": past_norm}])
pred_ds = ds.map_batches(
    TimeSeriesBatchPredictor,
    fn_constructor_args=(best_ckpt_path, model_kwargs),
    batch_size=1,
    batch_format="pandas",
    concurrency=1,
    num_gpus=1,  # force placement on a GPU worker if available
)

pred_row = pred_ds.take(1)[0]
pred_norm = pred_row["pred"]  # np.ndarray (HORIZON,)

# 4) De-normalize on the driver
mean, std = hourly["passengers"].mean(), hourly["passengers"].std()
pred = pred_norm * std + mean
past = past_norm * std + mean

# 5) Plot

t_past   = np.arange(-INPUT_WINDOW, 0)
STEP_SIZE_HOURS = 0.5  # you mentioned 30-min data
t_future = np.arange(0, HORIZON) * STEP_SIZE_HOURS

plt.figure(figsize=(10, 4))
plt.plot(t_past, past, label="History", marker="o")
plt.plot(t_future, future_true, "--", label="Ground Truth")
plt.plot(t_future, pred, "-.", label="Forecast")
plt.axvline(0)
plt.xlabel("Hours relative")
plt.ylabel("# trips")
plt.title("NYC-Taxi Forecast (Ray Data Inference)")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()

# 16. Cleanup – optionally remove all artifacts to free space
if os.path.exists(DATA_DIR):
    shutil.rmtree(DATA_DIR)
    print(f"Deleted {DATA_DIR}")

