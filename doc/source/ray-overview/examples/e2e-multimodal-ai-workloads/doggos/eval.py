from doggos.infer import TorchPredictor
from doggos.utils import add_class
from urllib.parse import urlparse
from sklearn.metrics import multilabel_confusion_matrix
import mlflow
import ray

def batch_metric(batch):
    labels = batch["label"]
    preds = batch["prediction"]
    mcm = multilabel_confusion_matrix(labels, preds)
    tn, fp, fn, tp = [], [], [], []
    for i in range(mcm.shape[0]):
        tn.append(mcm[i, 0, 0])  # True negatives
        fp.append(mcm[i, 0, 1])  # False positives
        fn.append(mcm[i, 1, 0])  # False negatives
        tp.append(mcm[i, 1, 1])  # True positives
    return {"TN": tn, "FP": fp, "FN": fn, "TP": tp}

if __name__ == "__main__":

    # Load the best run.
    model_registry = "/mnt/user_storage/mlflow/doggos"
    experiment_name = "doggos"
    mlflow.set_tracking_uri(f"file:{model_registry}")
    sorted_runs = mlflow.search_runs(
        experiment_names=[experiment_name], 
        order_by=["metrics.val_loss ASC"])
    best_run = sorted_runs.iloc[0]

    # Load and preproces eval dataset.
    artifacts_dir = urlparse(best_run.artifact_uri).path
    predictor = TorchPredictor.from_artifacts_dir(artifacts_dir=artifacts_dir)
    test_ds = ray.data.read_images("s3://doggos-dataset/test", include_paths=True)
    test_ds = test_ds.map(add_class)
    test_ds = predictor.preprocessor.transform(ds=test_ds)

    # y_pred (batch inference).
    pred_ds = test_ds.map_batches(
        predictor,
        fn_kwargs={"device": "cuda"},
        concurrency=4,
        batch_size=64,
        num_gpus=1,
        accelerator_type="L4",
    )

    # Aggregated metrics after processing all batches.
    metrics_ds = pred_ds.map_batches(batch_metric)
    aggregate_metrics = metrics_ds.sum(["TN", "FP", "FN", "TP"])

    # Aggregate the confusion matrix components across all batches.
    tn = aggregate_metrics["sum(TN)"]
    fp = aggregate_metrics["sum(FP)"]
    fn = aggregate_metrics["sum(FN)"]
    tp = aggregate_metrics["sum(TP)"]

    # Calculate metrics.
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    accuracy = (tp + tn) / (tp + tn + fp + fn)

    print(f"Precision: {precision:.2f}")
    print(f"Recall: {recall:.2f}")
    print(f"F1: {f1:.2f}")
    print(f"Accuracy: {accuracy:.2f}")