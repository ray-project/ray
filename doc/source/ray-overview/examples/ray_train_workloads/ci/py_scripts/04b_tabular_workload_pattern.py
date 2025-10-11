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
        "matplotlib==3.10.6",
        "scikit-learn==1.7.2",
        "pyarrow==14.0.2",
        "xgboost==3.0.5",
        "seaborn==0.13.2",
    ]
)

# 01. Imports
import os, shutil, json, uuid, tempfile, random
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.datasets import fetch_covtype
from sklearn.metrics import accuracy_score, confusion_matrix
from sklearn.model_selection import train_test_split
import xgboost as xgb

import ray
import ray.data as rd
from ray.train import (
    RunConfig,
    ScalingConfig,
    CheckpointConfig,
    FailureConfig,
    get_dataset_shard,
    get_checkpoint,
    get_context,
)
from ray.train.xgboost import XGBoostTrainer, RayTrainReportCallback

# 02. Load the UCI Cover type dataset (~580k rows, 54 features)
data = fetch_covtype(as_frame=True)
df = data.frame
df.rename(columns={"Cover_Type": "label"}, inplace=True)  # Ray expects "label"
df["label"] = df["label"] - 1  # 1-7  →  0-6
assert df["label"].between(0, 6).all()
print(df.shape, df.label.value_counts(normalize=True).head())

# 03. Visualize class distribution
df.label.value_counts().plot(
    kind="bar", figsize=(6, 3), title="Cover Type distribution"
)
plt.ylabel("Frequency")
plt.show()

# 04. Write to /mnt/cluster_storage/covtype/
PARQUET_DIR = "/mnt/cluster_storage/covtype/parquet"
os.makedirs(PARQUET_DIR, exist_ok=True)
file_path = os.path.join(PARQUET_DIR, "covtype.parquet")
df.to_parquet(file_path)
print(f"Wrote Parquet -> {file_path}")

# 05. Load dataset into a Ray Dataset (lazy, columnar)
ds_full = rd.read_parquet(file_path).random_shuffle()
print(ds_full)

# 06. Split to train and validation Ray Datasets
train_ds, val_ds = ds_full.split_proportionately([0.8])
print(f"Train rows: {train_ds.count()},  Val rows: {val_ds.count()}")

# 07. Look into one batch to confirm feature dimensionality
batch = train_ds.take_batch(batch_size=5, batch_format="pandas")
print(batch.head())
feature_columns = [c for c in batch.columns if c != "label"]

# 08. Custom Ray Train loop for XGBoost (CPU)


def train_func(config):
    """Per-worker training loop executed by Ray Train."""

    # --------------------------------------------------------
    # 1. Pull this worker’s data shard from Ray Datasets
    # --------------------------------------------------------
    label_col = config["label_column"]
    train_df = get_dataset_shard("train").materialize().to_pandas()
    eval_df = get_dataset_shard("evaluation").materialize().to_pandas()
    feature_cols = [c for c in train_df.columns if c != label_col]

    # Convert pandas to DMatrix (fast CSR format used by XGBoost)
    dtrain = xgb.DMatrix(train_df[feature_cols], label=train_df[label_col])
    deval = xgb.DMatrix(eval_df[feature_cols], label=eval_df[label_col])

    # --------------------------------------------------------
    # 2. Train booster — RayTrainReportCallback handles:
    #       • per-round ray.train.report(...)
    #       • checkpoint upload to Ray storage
    # --------------------------------------------------------

    # Optional resume from checkpoint. Ray sets this automatically if resuming.
    ckpt = get_checkpoint()
    if ckpt:
        with ckpt.as_directory() as d:
            model_path = os.path.join(d, RayTrainReportCallback.CHECKPOINT_NAME)
            booster = xgb.Booster()
            booster.load_model(model_path)
            print(f"[Rank {get_context().get_world_rank()}] Resumed from checkpoint")
    else:
        booster = None

    evals_result = {}  # XGBoost fills this with per-iteration metrics

    xgb.train(
        params=config["params"],
        dtrain=dtrain,
        evals=[(dtrain, "train"), (deval, "validation")],  # CHANGED label only
        num_boost_round=config["num_boost_round"],
        xgb_model=booster,  # Resumes if booster is not None
        evals_result=evals_result,  # NEW: capture metrics per round
        callbacks=[RayTrainReportCallback()],  # CHANGED: let it auto-collect metrics
    )
    # --------------------------------------------------------
    # 3. Rank-0 writes metrics JSON to the shared path
    # --------------------------------------------------------

    if get_context().get_world_rank() == 0:
        out_json_path = config["out_json_path"]

        # Optionally add a quick “best” summary for convenience
        v_hist = evals_result.get("validation", {}).get("mlogloss", [])
        best_idx = int(np.argmin(v_hist)) if len(v_hist) else None
        payload = {
            "evals_result": evals_result,
            "best": {
                "iteration": (best_idx + 1) if best_idx is not None else None,
                "validation-mlogloss": (
                    float(v_hist[best_idx]) if best_idx is not None else None
                ),
            },
        }

        os.makedirs(os.path.dirname(out_json_path), exist_ok=True)
        with open(out_json_path, "w") as f:
            json.dump(payload, f)
        print(f"[Rank 0] Wrote metrics JSON → {out_json_path}")


# 09. XGBoost config and Trainer (uses train_func above)
xgb_params = {
    "objective": "multi:softprob",
    "num_class": 7,
    "eval_metric": "mlogloss",
    "tree_method": "hist",  # CPU histogram algorithm
    "eta": 0.3,
    "max_depth": 8,
}

trainer = XGBoostTrainer(
    train_func,
    scaling_config=ScalingConfig(num_workers=16, use_gpu=False),
    datasets={"train": train_ds, "evaluation": val_ds},
    train_loop_config={
        "label_column": "label",
        "params": xgb_params,
        "num_boost_round": 50,  # Increase or decrease to adjust training iterations
        "out_json_path": "/mnt/cluster_storage/covtype/results/covtype_xgb_cpu/metrics.json",
    },
    run_config=RunConfig(
        name="covtype_xgb_cpu",
        storage_path="/mnt/cluster_storage/covtype/results",
        checkpoint_config=CheckpointConfig(checkpoint_frequency=10, num_to_keep=1),
        failure_config=FailureConfig(max_failures=1),  # Resume up to 3 times
    ),
)

# 10. Fit the trainer (reports eval metrics every boosting round)
result = trainer.fit()
best_ckpt = result.checkpoint  # saved automatically by Trainer

# 11. Plot evaluation history from saved JSON

with open("/mnt/cluster_storage/covtype/results/covtype_xgb_cpu/metrics.json") as f:
    payload = json.load(f)

hist = payload["evals_result"]
train = hist["train"]["mlogloss"]
val = hist["validation"]["mlogloss"]

xs = np.arange(1, len(val) + 1)
plt.figure(figsize=(7, 4))
plt.plot(xs, train, label="Train")
plt.plot(xs, val, label="Val")
plt.xlabel("Boosting round")
plt.ylabel("Log-loss")
plt.title("XGBoost log-loss")
plt.grid(True)
plt.legend()
plt.tight_layout()
plt.show()

best = payload["best"]["validation-mlogloss"]
print("Best validation log-loss:", best)

# 12. Retrieve Booster object from Ray checkpoint
booster = RayTrainReportCallback.get_model(best_ckpt)

# Convert Ray Dataset to pandas for quick local scoring
val_pd = val_ds.to_pandas()
dmatrix = xgb.DMatrix(val_pd[feature_columns])
pred_prob = booster.predict(dmatrix)
pred_labels = np.argmax(pred_prob, axis=1)

acc = accuracy_score(val_pd.label, pred_labels)
print(f"Validation accuracy: {acc:.3f}")

# 13. Confusion matrix

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

# 14. Example: Run batch inference using Ray remote task on a CPU worker

# This remote function is scheduled on a CPU-enabled Ray worker.
# It loads a trained XGBoost model from a Ray checkpoint and runs predictions on a pandas DataFrame.
@ray.remote(num_cpus=1)
def predict_batch(ckpt, batch_pd):
    # Load the trained XGBoost Booster model from the checkpoint.
    model = RayTrainReportCallback.get_model(ckpt)

    # Convert the input batch (pandas DataFrame) to DMatrix, required by XGBoost for inference.
    dmatrix = xgb.DMatrix(batch_pd[feature_columns])

    # Predict class probabilities for each row in the batch.
    preds = model.predict(dmatrix)

    # Select the class with highest predicted probability for each row.
    return np.argmax(preds, axis=1)


# Take a random sample of 1024 rows from the validation set to use as input.
sample_batch = val_pd.sample(1024, random_state=0)

# Submit the batch inference task to a Ray worker and block until it finishes.
preds = ray.get(predict_batch.remote(best_ckpt, sample_batch))

# Compute and print classification accuracy by comparing predictions to true labels.
print("Sample batch accuracy:", accuracy_score(sample_batch.label, preds))

# 15. Gain‑based feature importance
importances = booster.get_score(importance_type="gain")
keys, gains = zip(*sorted(importances.items(), key=lambda kv: kv[1], reverse=True)[:15])

plt.barh(range(len(gains)), gains)
plt.yticks(range(len(gains)), keys)
plt.gca().invert_yaxis()
plt.title("Top-15 Feature Importances (gain)")
plt.xlabel("Average gain")
plt.show()

# 16. Run 50 more training iterations from the last saved checkpoint
result = trainer.fit()
best_ckpt = result.checkpoint  # Saved automatically by Trainer

# 17. Rerun example batch inference from before to verify improved accuracy:

# Take a random sample of 1024 rows from the validation set to use as input.
sample_batch = val_pd.sample(1024, random_state=0)

# Submit the batch inference task to a Ray worker and block until it finishes.
preds = ray.get(predict_batch.remote(best_ckpt, sample_batch))

# Compute and print classification accuracy by comparing predictions to true labels.
print("Sample batch accuracy:", accuracy_score(sample_batch.label, preds))

# 18. Optional cleanup to free space
ARTIFACT_DIR = "/mnt/cluster_storage/covtype"
if os.path.exists(ARTIFACT_DIR):
    shutil.rmtree(ARTIFACT_DIR)
    print(f"Deleted {ARTIFACT_DIR}")
