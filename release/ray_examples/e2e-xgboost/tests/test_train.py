from unittest.mock import patch

import ray

from dist_xgboost.train import main


@patch("dist_xgboost.train.log_run_to_mlflow")
@patch("dist_xgboost.train.save_preprocessor")
@patch("dist_xgboost.train.local_storage_path", new=None)
# @patch("dist_xgboost.train.NUM_WORKERS", new=1)  # uncomment to run the test locally
# @patch("dist_xgboost.train.USE_GPU", new=False)  # uncomment to run the test locally
def test_main_execution(mock_log_run_to_mlflow, mock_save_preprocessor):
    ray.data.DataContext.log_internal_stack_trace_to_stdout = True
    main()

    mock_save_preprocessor.assert_called_once()
    mock_log_run_to_mlflow.assert_called_once()
