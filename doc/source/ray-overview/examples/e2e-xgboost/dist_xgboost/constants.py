import os

# on Anyscale, /mnt/cluster_storage/ is persisted for the cluster
# across jobs and clusters
# https://docs.anyscale.com/configuration/storage/#storage-shared-across-nodes
if os.path.exists("/mnt/cluster_storage/"):
    storage_path = "/mnt/cluster_storage/"
    print(f"Using Anyscale storage path: {storage_path}")
else:
    storage_path = "/tmp/"
    print(
        f"/mnt/cluster_storage/ not available, using local storage path: {storage_path}"
    )

preprocessor_fname = "preprocessor.pkl"
preprocessor_path = os.path.join(storage_path, preprocessor_fname)
model_fname = "model.ubj"  # name used by XGBoost
model_registry = os.path.join(storage_path, "mlflow")
experiment_name = "breast_cancer_all_features"
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
