import os

# on Anyscale, /mnt/cluster_storage/ is persisted for the cluster
# across jobs and clusters
# https://docs.anyscale.com/configuration/storage/#storage-shared-across-nodes
if os.path.exists("/mnt/cluster_storage/"):
    local_storage_path = "/mnt/cluster_storage/"
else:
    local_storage_path = "/tmp/"

preprocessor_fname = "preprocessor.pkl"
preprocessor_path = os.path.join(local_storage_path, preprocessor_fname)
model_fname = "model.ubj"  # name used by XGBoost
model_registry = os.path.join(local_storage_path, "mlflow")
experiment_name = "breast_cancer_all_features"
root_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
