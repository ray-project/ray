from unittest.mock import patch

from dist_xgboost.infer import main
from tests.utils import mock_load_model_and_preprocessor


@patch("dist_xgboost.infer.load_model_and_preprocessor", mock_load_model_and_preprocessor)
def test_infer_main():
    main()
