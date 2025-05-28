# Note: requires train.py to be run first for the model and preprocessor to be saved to MLFlow

import os

os.environ["RAY_TRAIN_V2_ENABLED"] = "1"

import pandas as pd
import xgboost
from sklearn.metrics import confusion_matrix

from dist_xgboost.data import load_model_and_preprocessor, prepare_data


def transform_with_preprocessor(batch_df, preprocessor):
    # The preprocessor does not know about the `target` column,
    # so we need to remove it temporarily then add it back
    target = batch_df.pop("target")
    transformed_features = preprocessor.transform_batch(batch_df)
    transformed_features["target"] = target
    return transformed_features


class Validator:
    def __init__(self, loader):
        # pass in loader function from the outer scope to
        # make it easier to mock during testing
        _, self.model = loader()

    def __call__(self, batch: pd.DataFrame) -> pd.DataFrame:
        # remove the target column for inference
        target = batch.pop("target")
        dmatrix = xgboost.DMatrix(batch)
        predictions = self.model.predict(dmatrix)

        results = pd.DataFrame({"prediction": predictions, "target": target})
        return results


def confusion_matrix_batch(batch, threshold=0.5):
    # apply a threshold to the predictions to get binary labels
    batch["prediction"] = (batch["prediction"] > threshold).astype(int)

    result = {}
    cm = confusion_matrix(batch["target"], batch["prediction"], labels=[0, 1])
    result["TN"] = cm[0, 0]
    result["FP"] = cm[0, 1]
    result["FN"] = cm[1, 0]
    result["TP"] = cm[1, 1]
    return pd.DataFrame(result, index=[0])


def main():
    _, _, test_dataset = prepare_data()

    preprocessor, _ = load_model_and_preprocessor()

    # Apply the transformation to each batch
    test_dataset = test_dataset.map_batches(
        transform_with_preprocessor,
        fn_kwargs={"preprocessor": preprocessor},
        batch_format="pandas",
        batch_size=1000,
    )

    # Make predictions
    test_predictions = test_dataset.map_batches(
        Validator,
        fn_constructor_kwargs={"loader": load_model_and_preprocessor},
        concurrency=4,  # Number of model replicas
        batch_format="pandas",
    )

    # Calculate confusion matrix
    test_results = test_predictions.map_batches(
        confusion_matrix_batch, batch_format="pandas", batch_size=1000
    )

    # Calculate metrics
    # Sum all confusion matrix values across batches
    cm_sums = test_results.sum(["TN", "FP", "FN", "TP"])

    # Extract confusion matrix components
    tn = cm_sums["sum(TN)"]
    fp = cm_sums["sum(FP)"]
    fn = cm_sums["sum(FN)"]
    tp = cm_sums["sum(TP)"]

    # Calculate metrics
    accuracy = (tp + tn) / (tp + tn + fp + fn)
    precision = tp / (tp + fp) if (tp + fp) > 0 else 0
    recall = tp / (tp + fn) if (tp + fn) > 0 else 0
    f1 = (
        2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0
    )

    metrics = {"precision": precision, "recall": recall, "f1": f1, "accuracy": accuracy}

    print("Validation results:")
    for key, value in metrics.items():
        print(f"{key}: {value:.4f}")


if __name__ == "__main__":
    main()
