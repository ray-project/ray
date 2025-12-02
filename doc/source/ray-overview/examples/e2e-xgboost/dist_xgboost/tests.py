from unittest.mock import patch, MagicMock

import ray
import numpy as np

from dist_xgboost.serve import main as serve_main
from dist_xgboost.train import main as train_main
from dist_xgboost.infer import main as inference_main


def mock_load_model_and_preprocessor():
    mock_preprocessor = MagicMock()
    mock_preprocessor.transform_batch.side_effect = lambda x: x
    mock_model = MagicMock()
    mock_model.predict.side_effect = lambda dmatrix: np.random.random(
        size=(dmatrix.num_row(),)
    )
    return mock_preprocessor, mock_model


@patch("dist_xgboost.train.log_run_to_mlflow")
@patch("dist_xgboost.train.save_preprocessor")
# @patch("dist_xgboost.train.NUM_WORKERS", new=1)  # uncomment to run the test locally
# @patch("dist_xgboost.train.USE_GPU", new=False)  # uncomment to run the test locally
def test_train_main(mock_log_run_to_mlflow, mock_save_preprocessor):
    ray.data.DataContext.log_internal_stack_trace_to_stdout = True
    train_main()

    mock_save_preprocessor.assert_called_once()
    mock_log_run_to_mlflow.assert_called_once()


@patch(
    "dist_xgboost.serve.load_model_and_preprocessor", mock_load_model_and_preprocessor
)
def test_serve_main():
    serve_main()


@patch(
    "dist_xgboost.infer.load_model_and_preprocessor", mock_load_model_and_preprocessor
)
def test_infer_main():
    inference_main()


if __name__ == "__main__":
    test_train_main()
    test_serve_main()
    test_infer_main()
