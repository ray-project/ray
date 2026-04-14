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
])

# 01. Imports

# Standard libraries
import os
import uuid
import json
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import zipfile
import shutil
import tempfile

# PyTorch
import torch
from torch import nn
import torch.nn.functional as F

# Ray
import ray
import ray.data
from ray.train import ScalingConfig, RunConfig, CheckpointConfig, FailureConfig, Checkpoint, get_checkpoint, get_context,  get_dataset_shard, report
from ray.train.torch import TorchTrainer, prepare_model

# Other
from tqdm import tqdm

import subprocess
# 02. Load MovieLens 100K Dataset and store in /mnt/cluster_storage/ as CSV + Parquet

# Define clean working paths
DATA_URL = "http://files.grouplens.org/datasets/movielens/ml-100k.zip"
LOCAL_ZIP = "/mnt/cluster_storage/rec_sys_tutorial/ml-100k.zip"
EXTRACT_DIR = "/mnt/cluster_storage/rec_sys_tutorial/ml-100k"
OUTPUT_CSV = "/mnt/cluster_storage/rec_sys_tutorial/raw/ratings.csv"
PARQUET_DIR = "/mnt/cluster_storage/rec_sys_tutorial/raw/ratings_parquet"

# Ensure target directories exist
os.makedirs("/mnt/cluster_storage/rec_sys_tutorial/raw", exist_ok=True)

# Download only if not already done
if not os.path.exists(LOCAL_ZIP):
    subprocess.run(
        ["wget", "-q", DATA_URL, "-O", LOCAL_ZIP],
        check=True,
    )

# Extract cleanly
if not os.path.exists(EXTRACT_DIR):
    import zipfile
    with zipfile.ZipFile(LOCAL_ZIP, 'r') as zip_ref:
        zip_ref.extractall("/mnt/cluster_storage/rec_sys_tutorial")

# Load raw file
raw_path = os.path.join(EXTRACT_DIR, "u.data")
df = pd.read_csv(raw_path, sep="\t", names=["user_id", "item_id", "rating", "timestamp"])

# Persist CSV (kept for later inference cell that expects CSV)
df.to_csv(OUTPUT_CSV, index=False)

# Persist a Parquet *dataset* (multiple files) to simulate blob storage layout
if os.path.exists(PARQUET_DIR):
    shutil.rmtree(PARQUET_DIR)
os.makedirs(PARQUET_DIR, exist_ok=True)

NUM_PARQUET_SHARDS = 8
for i, shard in enumerate(np.array_split(df, NUM_PARQUET_SHARDS)):
    shard.to_parquet(os.path.join(PARQUET_DIR, f"part-{i:02d}.parquet"), index=False)

print(f"✅ Loaded {len(df):,} ratings → CSV: {OUTPUT_CSV}")
print(f"✅ Wrote Parquet dataset with {NUM_PARQUET_SHARDS} shards → {PARQUET_DIR}")

# 03. Point to Parquet dataset URI 
DATASET_URI = os.environ.get(
    "RATINGS_PARQUET_URI",
    "/mnt/cluster_storage/rec_sys_tutorial/raw/ratings_parquet",
)

print("Parquet dataset URI:", DATASET_URI)

# 04. Visualize dataset: ratings, user and item activity

# Use encoded indices if present; otherwise fall back to raw IDs
user_col = "user_idx" if "user_idx" in df.columns else "user_id"
item_col = "item_idx" if "item_idx" in df.columns else "item_id"

plt.figure(figsize=(12, 4))

# Rating distribution
plt.subplot(1, 3, 1)
df["rating"].hist(bins=[0.5,1.5,2.5,3.5,4.5,5.5], edgecolor='black')
plt.title("Rating Distribution")
plt.xlabel("Rating"); plt.ylabel("Frequency")

# Number of ratings per user
plt.subplot(1, 3, 2)
df[user_col].value_counts().hist(bins=30, edgecolor='black')
plt.title("Ratings per User")
plt.xlabel("# Ratings"); plt.ylabel("Users")

# Number of ratings per item
plt.subplot(1, 3, 3)
df[item_col].value_counts().hist(bins=30, edgecolor='black')
plt.title("Ratings per Item")
plt.xlabel("# Ratings"); plt.ylabel("Items")

plt.tight_layout()
plt.show()

# 05. Create Ray Dataset by reading Parquet, then encode IDs via Ray

# Read Parquet dataset directly
ratings_ds = ray.data.read_parquet(DATASET_URI)
print("✅ Parquet dataset loaded (streaming, non-materialized)")
ratings_ds.show(3)

# ---- Build global ID mappings on the driver ----
user_ids = sorted([r["user_id"] for r in ratings_ds.groupby("user_id").count().take_all()])
item_ids = sorted([r["item_id"] for r in ratings_ds.groupby("item_id").count().take_all()])

user2idx = {uid: j for j, uid in enumerate(user_ids)}
item2idx = {iid: j for j, iid in enumerate(item_ids)}

NUM_USERS = len(user2idx)
NUM_ITEMS = len(item2idx)
print(f"Users: {NUM_USERS:,} | Items: {NUM_ITEMS:,}")

# ---- Encode to contiguous indices within Ray (keeps everything distributed) ----
def encode_batch(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf["user_idx"] = pdf["user_id"].map(user2idx).astype("int64")
    pdf["item_idx"] = pdf["item_id"].map(item2idx).astype("int64")
    return pdf[["user_idx", "item_idx", "rating", "timestamp"]]

ratings_ds = ratings_ds.map_batches(encode_batch, batch_format="pandas")
print("✅ Encoded Ray Dataset schema:", ratings_ds.schema())
ratings_ds.show(3)

# 06. Train/val split using Ray Data (lazy, avoids materialization)

TRAIN_FRAC = 0.8
SEED = 42  # for reproducibility

# Block-level shuffle + proportional split (approximate by block, lazy)
train_ds, val_ds = (
    ratings_ds
    .randomize_block_order(seed=SEED)   # lightweight; no row-level materialization
    .split_proportionately([TRAIN_FRAC])  # returns [train, remainder]
)

print("✅ Train/Val Split:")
print(f"  Train → {train_ds.count():,} rows")
print(f"  Val   → {val_ds.count():,} rows")

# 07. Define matrix factorization model

class MatrixFactorizationModel(nn.Module):
    def __init__(self, num_users: int, num_items: int, embedding_dim: int = 64):
        super().__init__()
        self.user_embedding = nn.Embedding(num_users, embedding_dim)
        self.item_embedding = nn.Embedding(num_items, embedding_dim)

    def forward(self, user_idx, item_idx):
        user_vecs = self.user_embedding(user_idx)
        item_vecs = self.item_embedding(item_idx)
        dot_product = (user_vecs * item_vecs).sum(dim=1)
        return dot_product

# 08. Define Ray Train loop (with val loss, checkpointing, and Ray-managed metrics)

def train_loop_per_worker(config):
    import tempfile
    # ---------------- Dataset shards -> PyTorch-style iterators ---------------- #
    train_ds = get_dataset_shard("train")
    val_ds   = get_dataset_shard("val")
    train_loader = train_ds.iter_torch_batches(batch_size=512, dtypes=torch.float32)
    val_loader   = val_ds.iter_torch_batches(batch_size=512, dtypes=torch.float32)

    # ---------------- Model / Optimizer ---------------- #
    model = MatrixFactorizationModel(
        num_users=config["num_users"],
        num_items=config["num_items"],
        embedding_dim=config.get("embedding_dim", 64),
    )
    model = prepare_model(model)
    optimizer = torch.optim.Adam(model.parameters(), lr=config.get("lr", 1e-3))

    # ---------------- Checkpointing setup ---------------- #
    rank = get_context().get_world_rank()
    start_epoch = 0

    # If a checkpoint exists (auto-resume), load it
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as ckpt_dir:
            model.load_state_dict(
                torch.load(os.path.join(ckpt_dir, "model.pt"), map_location="cpu")
            )
            start_epoch = torch.load(os.path.join(ckpt_dir, "meta.pt")).get("epoch", 0) + 1
        if rank == 0:
            print(f"[Rank {rank}] ✅ Resumed from checkpoint at epoch {start_epoch}")

    # ---------------- Training loop ---------------- #
    for epoch in range(start_epoch, config.get("epochs", 5)):
        # ---- Train ----
        model.train()
        train_losses = []
        for batch in train_loader:
            user = batch["user_idx"].long()
            item = batch["item_idx"].long()
            rating = batch["rating"].float()

            pred = model(user, item)
            loss = F.mse_loss(pred, rating)

            optimizer.zero_grad()
            loss.backward()
            optimizer.step()

            train_losses.append(loss.item())

        avg_train_loss = sum(train_losses) / max(1, len(train_losses))

        # ---- Validate ----
        model.eval()
        val_losses = []
        with torch.no_grad():
            for batch in val_loader:
                user = batch["user_idx"].long()
                item = batch["item_idx"].long()
                rating = batch["rating"].float()

                pred = model(user, item)
                loss = F.mse_loss(pred, rating)
                val_losses.append(loss.item())

        avg_val_loss = sum(val_losses) / max(1, len(val_losses))

        # Console log (optional)
        if rank == 0:
            print(f"[Epoch {epoch}] Train MSE: {avg_train_loss:.4f} | Val MSE: {avg_val_loss:.4f}")

        metrics = {
            "epoch": epoch,
            "train_loss": avg_train_loss,
            "val_loss": avg_val_loss,
        }

        # ---- Save checkpoint & report (rank 0 attaches checkpoint; others report metrics only) ----
        if rank == 0:
            with tempfile.TemporaryDirectory() as tmpdir:
                torch.save(model.state_dict(), os.path.join(tmpdir, "model.pt"))
                torch.save({"epoch": epoch}, os.path.join(tmpdir, "meta.pt"))
                ckpt_out = Checkpoint.from_directory(tmpdir)
                report(metrics, checkpoint=ckpt_out)
        else:
            report(metrics, checkpoint=None)

# 09. Launch distributed training with Ray TorchTrainer

# Define config params (use Ray-derived counts)
train_config = {
    "num_users": NUM_USERS,
    "num_items": NUM_ITEMS,
    "embedding_dim": 64,
    "lr": 1e-3,
    "epochs": 20,
}

trainer = TorchTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config=train_config,
    scaling_config=ScalingConfig(
        num_workers=8,       # Increase as needed
        use_gpu=True         # Set to True if training on GPUs
    ),
    datasets={"train": train_ds, "val": val_ds},
    run_config=RunConfig(
        name="mf_ray_train",
        storage_path="/mnt/cluster_storage/rec_sys_tutorial/results",
        checkpoint_config=CheckpointConfig(num_to_keep=20),
        failure_config=FailureConfig(max_failures=2)
    )
)

# Run distributed training
result = trainer.fit()

# 10. Plot train/val loss curves (from Ray Train results)

# Pull the full metrics history Ray stored for this run
df = result.metrics_dataframe.copy()

# Keep only the columns we need (guard against extra columns)
cols = [c for c in ["epoch", "train_loss", "val_loss"] if c in df.columns]
df = df[cols].dropna()

# If multiple rows per epoch exist, keep the last report per epoch
if "epoch" in df.columns:
    df = df.sort_index().groupby("epoch", as_index=False).last()

# Plot
plt.figure(figsize=(7, 4))
if "train_loss" in df.columns:
    plt.plot(df["epoch"], df["train_loss"], marker="o", label="Train")
if "val_loss" in df.columns:
    plt.plot(df["epoch"], df["val_loss"], marker="o", label="Val")
plt.xlabel("Epoch")
plt.ylabel("MSE Loss")
plt.title("Matrix Factorization - Loss per Epoch")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()

# 11. Run trainer.fit() again to resume from last checkpoint

result = trainer.fit()

# 12. Inference: recommend top-N items for a user

# ---------------------------------------------
# Step 1: Reload original ratings CSV and mappings
# ---------------------------------------------
df = pd.read_csv("/mnt/cluster_storage/rec_sys_tutorial/raw/ratings.csv")

# Recompute ID mappings (same as during preprocessing)
unique_users = sorted(df["user_id"].unique())
unique_items = sorted(df["item_id"].unique())

user2idx = {uid: j for j, uid in enumerate(unique_users)}
item2idx = {iid: j for j, iid in enumerate(unique_items)}
idx2item = {v: k for k, v in item2idx.items()}

# ---------------------------------------------
# Step 2: Load model from checkpoint
# ---------------------------------------------
model = MatrixFactorizationModel(
    num_users=len(user2idx),
    num_items=len(item2idx),
    embedding_dim=train_config["embedding_dim"]
)

with result.checkpoint.as_directory() as ckpt_dir:
    state_dict = torch.load(os.path.join(ckpt_dir, "model.pt"), map_location="cpu")

    # Remove 'module.' prefix if using DDP-trained model
    if any(k.startswith("module.") for k in state_dict):
        state_dict = {k.replace("module.", ""): v for k, v in state_dict.items()}

    model.load_state_dict(state_dict)

model.eval()

# ---------------------------------------------
# Step 3: Select a user and generate recommendations
# ---------------------------------------------
# Choose a random user from the original dataset
original_user_id = df["user_id"].sample(1).iloc[0]
user_idx = user2idx[original_user_id]

print(f"Generating recommendations for user_id={original_user_id} (internal idx={user_idx})")

# Compute scores for all items for this user
with torch.no_grad():
    user_vector = model.user_embedding(torch.tensor([user_idx]))           # [1, D]
    item_vectors = model.item_embedding.weight                             # [num_items, D]
    scores = torch.matmul(user_vector, item_vectors.T).squeeze(0)          # [num_items]

    topk = torch.topk(scores, k=10)
    top_item_ids = [idx2item[j.item()] for j in topk.indices]
    top_scores = topk.values.tolist()

# ---------------------------------------------
# Step 4: Print top-N recommendations
# ---------------------------------------------
print("\nTop 10 Recommended Item IDs:")
for i, (item_id, score) in enumerate(zip(top_item_ids, top_scores), 1):
    print(f"{i:2d}. Item ID: {item_id} | Score: {score:.2f}")

# 13. Join top-N item IDs with movie titles from u.item

item_metadata = pd.read_csv(
    "/mnt/cluster_storage/rec_sys_tutorial/ml-100k/u.item",
    sep="|",
    encoding="latin-1",
    header=None,
    usecols=[0, 1],  # Only item_id and title
    names=["item_id", "title"]
)

# Join with top-N items
top_items_df = pd.DataFrame({
    "item_id": top_item_ids,
    "score": top_scores
})

merged = top_items_df.merge(item_metadata, on="item_id", how="left")

print("\nTop 10 Recommended Movies:")
for j, row in merged.iterrows():
    print(f"{j+1:2d}. {row['title']} | Score: {row['score']:.2f}")

# 14. Cleanup -- delete checkpoints and metrics from model training

TARGET_PATH = "/mnt/cluster_storage/rec_sys_tutorial"  # please note, that /mnt/cluster_storage/ only exists on Anyscale

if os.path.exists(TARGET_PATH):
    shutil.rmtree(TARGET_PATH)
    print(f"✅ Deleted everything under {TARGET_PATH}")
else:
    print(f"⚠️ Path does not exist: {TARGET_PATH}")

