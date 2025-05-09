from unittest.mock import patch

from dist_xgboost.serve import main
from tests.utils import mock_load_model_and_preprocessor


@patch(
    "dist_xgboost.serve.load_model_and_preprocessor", mock_load_model_and_preprocessor
)
def test_serve_main():
    main()
