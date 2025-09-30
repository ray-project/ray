# 00. Runtime setup — install same deps as build.sh and set env vars
import os, sys, subprocess

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
# 02. Load MovieLens 100K Dataset and store in /mnt/cluster_storage/

# Define clean working paths
DATA_URL = "http://files.grouplens.org/datasets/movielens/ml-100k.zip"
LOCAL_ZIP = "/mnt/cluster_storage/rec_sys_tutorial/ml-100k.zip"
EXTRACT_DIR = "/mnt/cluster_storage/rec_sys_tutorial/ml-100k"
OUTPUT_CSV = "/mnt/cluster_storage/rec_sys_tutorial/raw/ratings.csv"

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
    with zipfile.ZipFile(LOCAL_ZIP, 'r') as zip_ref:
        zip_ref.extractall("/mnt/cluster_storage/rec_sys_tutorial")

# Load raw file
raw_path = os.path.join(EXTRACT_DIR, "u.data")
df = pd.read_csv(raw_path, sep="\t", names=["user_id", "item_id", "rating", "timestamp"])

# Save cleaned version
df.to_csv(OUTPUT_CSV, index=False)

print(f"✅ Loaded {len(df):,} ratings → {OUTPUT_CSV}")
df.head()

# 03. Preprocess IDs and create Ray Dataset in parallel

# Load CSV
df = pd.read_csv("/mnt/cluster_storage/rec_sys_tutorial/raw/ratings.csv")

# Encode user_id and item_id
user2idx = {uid: j for j, uid in enumerate(sorted(df["user_id"].unique()))}
item2idx = {iid: j for j, iid in enumerate(sorted(df["item_id"].unique()))}

df["user_idx"] = df["user_id"].map(user2idx)
df["item_idx"] = df["item_id"].map(item2idx)
df = df[["user_idx", "item_idx", "rating", "timestamp"]]

# Split into multiple chunks for parallel ingestion
NUM_SPLITS = 64  # adjust based on cluster size
dfs = np.array_split(df, NUM_SPLITS)
object_refs = [ray.put(split) for split in dfs]

# 04. Visualize Dataset: Ratings, User & Item Activity

# Plot rating distribution
plt.figure(figsize=(12, 4))

plt.subplot(1, 3, 1)
df["rating"].hist(bins=[0.5,1.5,2.5,3.5,4.5,5.5], edgecolor='black')
plt.title("Rating Distribution")
plt.xlabel("Rating"); plt.ylabel("Frequency")

# Plot number of ratings per user
plt.subplot(1, 3, 2)
df["user_idx"].value_counts().hist(bins=30, edgecolor='black')
plt.title("Ratings per User")
plt.xlabel("# Ratings"); plt.ylabel("Users")

# Plot number of ratings per item
plt.subplot(1, 3, 3)
df["item_idx"].value_counts().hist(bins=30, edgecolor='black')
plt.title("Ratings per Item")
plt.xlabel("# Ratings"); plt.ylabel("Items")

plt.tight_layout()
plt.show()

# 05. Create Ray Dataset from refs (uses multiple blocks/workers)

ratings_ds = ray.data.from_pandas_refs(object_refs)
print("✅ Ray Dataset created with", ratings_ds.num_blocks(), "blocks")
ratings_ds.show(3)

# 06. Train/Val Split using Ray Data

# Parameters
TRAIN_FRAC = 0.8
SEED = 42  # for reproducibility

# Shuffle + split by index
total_rows = ratings_ds.count()
train_size = int(total_rows * TRAIN_FRAC)

ratings_ds = ratings_ds.random_shuffle(seed=SEED)
train_ds, val_ds = ratings_ds.split_at_indices([train_size])

print(f"✅ Train/Val Split:")
print(f"  Train → {train_ds.count():,} rows")
print(f"  Val   → {val_ds.count():,} rows")

# 07. Define Matrix Factorization Model

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

# 08. Define Ray Train Loop (with Val Loss + Checkpointing + Logging)

def train_loop_per_worker(config):

    # Get dataset shards for this worker
    train_ds = get_dataset_shard("train")
    val_ds   = get_dataset_shard("val")
    train_loader = train_ds.iter_torch_batches(batch_size=512, dtypes=torch.float32)
    val_loader   = val_ds.iter_torch_batches(batch_size=512, dtypes=torch.float32)

    # Create model and optimizer
    model = MatrixFactorizationModel(
        num_users=config["num_users"],
        num_items=config["num_items"],
        embedding_dim=config.get("embedding_dim", 64)
    )
    model = prepare_model(model)
    optimizer = torch.optim.Adam(model.parameters(), lr=config.get("lr", 1e-3))

    # Paths for checkpointing and logging
    CKPT_DIR = "/mnt/cluster_storage/rec_sys_tutorial/checkpoints"
    LOG_PATH = "/mnt/cluster_storage/rec_sys_tutorial/epoch_metrics.json"
    os.makedirs(CKPT_DIR, exist_ok=True)
    rank = int(os.environ.get("RANK", "0"))  # Worker rank
    start_epoch = 0

    # Resume from checkpoint if available
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as ckpt_dir:
            model.load_state_dict(torch.load(os.path.join(ckpt_dir, "model.pt"), map_location="cpu"))
            start_epoch = torch.load(os.path.join(ckpt_dir, "meta.pt")).get("epoch", 0) + 1
        if rank == 0:
            print(f"[Rank {rank}] ✅ Resumed from checkpoint at epoch {start_epoch}")

    # Clean up log file on first run (only if not resuming)
    if rank == 0 and start_epoch == 0 and os.path.exists(LOG_PATH):
        os.remove(LOG_PATH)

    # ----------------- Training Loop ----------------- #
    for epoch in range(start_epoch, config.get("epochs", 5)):
        model.train()
        train_losses = []

        # Train over each batch
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

        avg_train_loss = sum(train_losses) / len(train_losses)

        # ---------- Validation Pass ----------
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

        avg_val_loss = sum(val_losses) / len(val_losses)

        # Log to stdout
        print(f"[Epoch {epoch}] Train MSE: {avg_train_loss:.4f} | Val MSE: {avg_val_loss:.4f}")

        # ---------- Save Checkpoint (Rank 0 Only) ----------
        if rank == 0:
            out_dir = os.path.join(CKPT_DIR, f"epoch_{epoch}_{uuid.uuid4().hex}")
            os.makedirs(out_dir, exist_ok=True)
            torch.save(model.state_dict(), os.path.join(out_dir, "model.pt"))
            torch.save({"epoch": epoch}, os.path.join(out_dir, "meta.pt"))
            ckpt_out = Checkpoint.from_directory(out_dir)
        else:
            ckpt_out = None

        # ---------- Append Metrics to JSON Log (Rank 0) ----------
        if rank == 0:
            logs = []
            if os.path.exists(LOG_PATH):
                try:
                    with open(LOG_PATH, "r") as f:
                        logs = json.load(f)
                except json.JSONDecodeError:
                    print("⚠️  JSON log unreadable. Starting fresh.")
                    logs = []

            logs.append({
                "epoch": epoch,
                "train_loss": avg_train_loss,
                "val_loss": avg_val_loss
            })
            with open(LOG_PATH, "w") as f:
                json.dump(logs, f)

        # ---------- Report to Ray Train ----------
        report({
            "epoch": epoch,
            "train_loss": avg_train_loss,
            "val_loss": avg_val_loss
        }, checkpoint=ckpt_out)

# 09. Launch Distributed Training with Ray TorchTrainer

# Define config params
train_config = {
    "num_users": df["user_idx"].nunique(),
    "num_items": df["item_idx"].nunique(),
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
        checkpoint_config=CheckpointConfig(num_to_keep=3),
        failure_config=FailureConfig(max_failures=2)
    )
)

# Run distributed training
result = trainer.fit()

# 10. Plot Train/Val Loss Curves

# Path to training metrics log
LOG_PATH = "/mnt/cluster_storage/rec_sys_tutorial/epoch_metrics.json"

# Load and convert to DataFrame
with open(LOG_PATH, "r") as f:
    logs = json.load(f)

df = pd.DataFrame(logs)
df["train_loss"] = pd.to_numeric(df["train_loss"], errors="coerce")
df["val_loss"] = pd.to_numeric(df["val_loss"], errors="coerce")

# Plot
plt.figure(figsize=(7, 4))
plt.plot(df["epoch"], df["train_loss"], marker="o", label="Train")
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

# 12. Inference: Recommend Top-N Items for a User

# ---------------------------------------------
# Step 1: Reload original ratings CSV + mappings
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
# Step 4: Print Top-N Recommendations
# ---------------------------------------------
print("\nTop 10 Recommended Item IDs:")
for i, (item_id, score) in enumerate(zip(top_item_ids, top_scores), 1):
    print(f"{i:2d}. Item ID: {item_id} | Score: {score:.2f}")

# 13. Join Top-N Item IDs with Movie Titles from u.item

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

TARGET_PATH = "/mnt/cluster_storage/rec_sys_tutorial"

if os.path.exists(TARGET_PATH):
    shutil.rmtree(TARGET_PATH)
    print(f"✅ Deleted everything under {TARGET_PATH}")
else:
    print(f"⚠️ Path does not exist: {TARGET_PATH}")

