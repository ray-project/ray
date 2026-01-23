# 00. Runtime setup 
import os
import sys
import subprocess

# Non-secret env var 
os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

# Install Python dependencies 
subprocess.check_call([
    sys.executable, "-m", "pip", "install", "--no-cache-dir",
    "matplotlib==3.10.6",
    "scikit-learn==1.7.2",
    "pyarrow==14.0.2",    
    "xgboost==3.0.5",
    "seaborn==0.13.2",
])

# 01. Imports
import os
import shutil
import json
import uuid
import tempfile
import random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.datasets import fetch_covtype
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.model_selection import train_test_split
import xgboost as xgb
import pyarrow as pa

import ray
import ray.data as rd
from ray.data import ActorPoolStrategy
from ray.train import RunConfig, ScalingConfig, CheckpointConfig, FailureConfig, get_dataset_shard, get_checkpoint, get_context
from ray.train.xgboost import XGBoostTrainer, RayTrainReportCallback

# 02. Load the UCI Cover type dataset (~580k rows, 54 features)
data = fetch_covtype(as_frame=True)
df = data.frame
df.rename(columns={"Cover_Type": "label"}, inplace=True)   # Ray expects "label"
df["label"] = df["label"] - 1          # 1-7  →  0-6
assert df["label"].between(0, 6).all()
print(df.shape, df.label.value_counts(normalize=True).head())

# 03. Visualize class distribution
df.label.value_counts().plot(kind="bar", figsize=(6,3), title="Cover Type distribution")
plt.ylabel("Frequency"); plt.show()

# 04. Write separate train/val Parquets to /mnt/cluster_storage/covtype/

PARQUET_DIR = "/mnt/cluster_storage/covtype/parquet"
os.makedirs(PARQUET_DIR, exist_ok=True)

TRAIN_PARQUET = os.path.join(PARQUET_DIR, "train.parquet")
VAL_PARQUET   = os.path.join(PARQUET_DIR, "val.parquet")

# Stratified 80/20 split for reproducibility
train_df, val_df = train_test_split(
    df, test_size=0.2, random_state=42, stratify=df["label"]
)

train_df.to_parquet(TRAIN_PARQUET, index=False)
val_df.to_parquet(VAL_PARQUET, index=False)

print(f"Wrote Train → {TRAIN_PARQUET} ({len(train_df):,} rows)")
print(f"Wrote Val   → {VAL_PARQUET}   ({len(val_df):,} rows)")

# 05. Load the two splits as Ray Datasets (lazy, columnar)
train_ds = rd.read_parquet(TRAIN_PARQUET).random_shuffle()
val_ds   = rd.read_parquet(VAL_PARQUET)

print(train_ds)
print(val_ds)

print(f"Train rows: {train_ds.count():,},  Val rows: {val_ds.count():,}")  # Note that this will materialize the dataset (skip at scale)

# 07. Look into one batch to confirm feature dimensionality
batch = train_ds.take_batch(batch_size=5, batch_format="pandas")
print(batch.head())
feature_columns = [c for c in batch.columns if c != "label"]

INDEX_COLS = {"__index_level_0__"}  # extend if needed

def _arrow_table_from_shard(name: str) -> pa.Table:
    """Collect this worker's Ray Dataset shard into one pyarrow. Table and
    drop accidental index columns (e.g., from pandas Parquet)."""
    ds_iter = get_dataset_shard(name)
    arrow_refs = ds_iter.materialize().to_arrow_refs()
    tables = [ray.get(r) for r in arrow_refs]
    tbl = pa.concat_tables(tables, promote_options="none") if tables else pa.table({})
    # Drop index columns if present
    keep = [c for c in tbl.column_names if c not in INDEX_COLS]
    if len(keep) != len(tbl.column_names):
        tbl = tbl.select(keep)
    return tbl

def _dmat_from_arrow(table: pa.Table, feature_cols, label_col: str):
    """Build XGBoost DMatrix from pyarrow.Table with explicit feature_names."""
    X = np.column_stack([table[c].to_numpy(zero_copy_only=False) for c in feature_cols])
    y = table[label_col].to_numpy(zero_copy_only=False)
    return xgb.DMatrix(X, label=y, feature_names=feature_cols)

def train_func(config):
    label_col = config["label_column"]

    # Arrow tables 
    train_arrow = _arrow_table_from_shard("train")
    eval_arrow  = _arrow_table_from_shard("evaluation")

    # Use the SAME ordered feature list for both splits
    feature_cols = [c for c in train_arrow.column_names if c != label_col]

    dtrain = _dmat_from_arrow(train_arrow, feature_cols, label_col)
    deval  = _dmat_from_arrow(eval_arrow,  feature_cols, label_col)

    # -------- 2) Optional resume from checkpoint ------------------------------
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as d:
            model_path = os.path.join(d, RayTrainReportCallback.CHECKPOINT_NAME)
            booster = xgb.Booster()
            booster.load_model(model_path)
            print(f"[Rank {get_context().get_world_rank()}] Resumed from checkpoint")
    else:
        booster = None

    # -------- 3) Train with per-round reporting & checkpointing ---------------
    evals_result = {}
    xgb.train(
        params          = config["params"],
        dtrain          = dtrain,
        evals           = [(dtrain, "train"), (deval, "validation")],
        num_boost_round = config["num_boost_round"],
        xgb_model       = booster,
        evals_result    = evals_result,
        callbacks       = [RayTrainReportCallback()],
    )

# 09. XGBoost config and Trainer (full-node CPU workers)

# Adjust this to your node size if different (e.g., 16, 32, etc.)
CPUS_PER_WORKER = 4

xgb_params = {
    "objective": "multi:softprob",
    "num_class": 7,
    "eval_metric": "mlogloss",
    "tree_method": "hist",
    "eta": 0.3,
    "max_depth": 8,
    "nthread": CPUS_PER_WORKER,  
}

trainer = XGBoostTrainer(
    train_func,
    scaling_config=ScalingConfig(
        num_workers=2,
        use_gpu=False,
        resources_per_worker={"CPU": CPUS_PER_WORKER},
    ),
    datasets={"train": train_ds, "evaluation": val_ds},
    train_loop_config={
        "label_column": "label",
        "params": xgb_params,
        "num_boost_round": 50,
    },
    run_config=RunConfig(
        name="covtype_xgb_cpu",
        storage_path="/mnt/cluster_storage/covtype/results",
        checkpoint_config=CheckpointConfig(
            num_to_keep=1,
            checkpoint_score_attribute="validation-mlogloss",  # score by val loss
            checkpoint_score_order="min",
        ),
        failure_config=FailureConfig(max_failures=1),
    ),
)

# 10. Fit the trainer (reports eval metrics every boosting round)
result = trainer.fit()
best_ckpt = result.checkpoint            # saved automatically by Trainer

# 11. Retrieve Booster object from Ray checkpoint
booster = RayTrainReportCallback.get_model(best_ckpt)

# Convert Ray Dataset to pandas for quick local scoring
val_pd = val_ds.to_pandas()
dmatrix = xgb.DMatrix(val_pd[feature_columns])
pred_prob = booster.predict(dmatrix)
pred_labels = np.argmax(pred_prob, axis=1)

acc = accuracy_score(val_pd.label, pred_labels)
print(f"Validation accuracy: {acc:.3f}")

# 12. Confusion matrix

cm = confusion_matrix(val_pd.label, pred_labels)  # or sample_batch.label if used

sns.heatmap(cm, annot=True, fmt="d", cmap="viridis")
plt.title("Confusion Matrix with Counts")
plt.xlabel("Predicted")
plt.ylabel("True")
plt.show()

cm_norm = cm.astype("float") / cm.sum(axis=1)[:, np.newaxis]
sns.heatmap(cm_norm, annot=True, fmt=".2f", cmap="viridis")
plt.title("Normalized Confusion Matrix")
plt.xlabel("Predicted")
plt.ylabel("True")
plt.show()

# 13. CPU batch inference with Ray Data 

# Assumes: val_ds, feature_columns, best_ckpt already defined.

class XGBPredictor:
    """Stateful actor: load Booster once, reuse across batches."""
    def __init__(self, ckpt, feature_cols):
        self.model = RayTrainReportCallback.get_model(ckpt)
        self.feature_cols = feature_cols

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        dmatrix = xgb.DMatrix(batch[self.feature_cols])
        probs = self.model.predict(dmatrix)
        preds = np.argmax(probs, axis=1)
        return pd.DataFrame(
            {"pred": preds.astype(np.int32), "label": batch["label"].astype(np.int32)}
        )

# Use an ActorPoolStrategy instead of compute="actors"
pred_ds = val_ds.map_batches(
    XGBPredictor,
    fn_constructor_args=(best_ckpt, feature_columns),
    batch_format="pandas",
    compute=ActorPoolStrategy(),   
    num_cpus=1,                    # per-actor CPU; tune as needed
)

# Aggregate accuracy without collecting to driver
stats_ds = pred_ds.map_batches(
    lambda df: pd.DataFrame({
        "correct": [int((df["pred"].to_numpy() == df["label"].to_numpy()).sum())],
        "n": [int(len(df))]
    }),
    batch_format="pandas",
)

correct = int(stats_ds.sum("correct"))
n = int(stats_ds.sum("n"))
print(f"Validation accuracy (Ray Data inference): {correct / n:.3f}")

# 14. Gain‑based feature importance
importances = booster.get_score(importance_type="gain")
keys, gains = zip(*sorted(importances.items(), key=lambda kv: kv[1], reverse=True)[:15])

plt.barh(range(len(gains)), gains)
plt.yticks(range(len(gains)), keys)
plt.gca().invert_yaxis()
plt.title("Top-15 Feature Importances (gain)"); plt.xlabel("Average gain"); plt.show()

# 15. Run 50 more training iterations from the last saved checkpoint
result = trainer.fit()
best_ckpt = result.checkpoint            # Saved automatically by Trainer

# 16. Rerun Ray Data inference to verify improved accuracy after continued training

# Reuse the existing Ray Data inference setup with the latest checkpoint
pred_ds = val_ds.map_batches(
    XGBPredictor,
    fn_constructor_args=(best_ckpt, feature_columns),
    batch_format="pandas",
    compute=ActorPoolStrategy(),
    num_cpus=1,
)

# Aggregate accuracy across all batches
stats_ds = pred_ds.map_batches(
    lambda df: pd.DataFrame({
        "correct": [int((df["pred"] == df["label"]).sum())],
        "n": [int(len(df))]
    }),
    batch_format="pandas",
)

correct = int(stats_ds.sum("correct"))
n = int(stats_ds.sum("n"))
print(f"Validation accuracy after continued training: {correct / n:.3f}")

# 17. Optional cleanup to free space
ARTIFACT_DIR = "/mnt/cluster_storage/covtype"
if os.path.exists(ARTIFACT_DIR):
    shutil.rmtree(ARTIFACT_DIR)
    print(f"Deleted {ARTIFACT_DIR}")

