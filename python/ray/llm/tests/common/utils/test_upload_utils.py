import os
import tempfile
from unittest.mock import MagicMock, patch, ANY

from ray.llm._internal.common.utils.upload_utils import upload_model_files


@patch("pyarrow.fs.copy_files")
def test_upload_undownloaded_model(mock_copy_files):
    mock_curated_transformers_models_module = MagicMock()
    mock_curated_transformers_tokenizers_module = MagicMock()
    mock_from_hf_hub_to_cache = MagicMock()
    mock_curated_transformers_models_module.FromHF.from_hf_hub_to_cache = (
        mock_from_hf_hub_to_cache
    )
    mock_curated_transformers_tokenizers_module.FromHF.from_hf_hub_to_cache = (
        MagicMock()
    )
    model_id = "fake-model-id"

    # Mock the modules to avoid adding as dependencies.
    with patch.dict(
        "sys.modules",
        {
            "curated_transformers.models": mock_curated_transformers_models_module,
            "curated_transformers.tokenizers": mock_curated_transformers_tokenizers_module,
        },
    ):
        with tempfile.TemporaryDirectory() as tempdir:
            model_dir = os.path.join(tempdir, model_id)
            # mock the from_hf_hub_to_cache to create a subdir in the model_dir
            def create_subdir_side_effect(*args, **kwargs):
                os.makedirs(model_dir, exist_ok=True)

            mock_from_hf_hub_to_cache.side_effect = create_subdir_side_effect

            with patch(
                "ray.llm._internal.common.utils.upload_utils.get_model_entrypoint",
                return_value=model_dir,
            ):
                # upload the model
                upload_model_files(model_id, "gs://bucket/model-id")

            # check that the model was uploaded
            assert mock_copy_files.call_count == 1
            assert mock_copy_files.called_with(model_dir, "bucket/model-id/", ANY, ANY)


@patch("pyarrow.fs.copy_files")
def test_upload_downloaded_hf_model(mock_copy_files):
    model_id = "fake-model-id"
    hash = "0123456789abcdef"
    with tempfile.TemporaryDirectory() as tempdir:
        model_dir = os.path.join(tempdir, model_id)
        os.makedirs(model_dir, exist_ok=True)
        os.makedirs(os.path.join(model_dir, "snapshots", hash), exist_ok=True)
        model_rev_path = os.path.join(model_dir, "refs")
        os.makedirs(model_rev_path, exist_ok=True)
        with open(os.path.join(model_rev_path, "main"), "w") as f:
            f.write(hash)

        with patch(
            "ray.llm._internal.common.utils.upload_utils.get_model_entrypoint",
            return_value=model_dir,
        ):
            upload_model_files(model_id, "s3://bucket/model-id")

        assert mock_copy_files.call_count == 2
        assert mock_copy_files.called_with(
            os.path.join(model_dir, "snapshots", hash), "bucket/model-id/", ANY, ANY
        )
        assert mock_copy_files.called_with(
            os.path.join(model_rev_path, "main"), "bucket/model-id/hash", ANY, ANY
        )


@patch("pyarrow.fs.copy_files")
def test_upload_custom_model(mock_copy_files):
    model_id = "fake-model-id"
    with tempfile.TemporaryDirectory() as tempdir:
        model_dir = os.path.join(tempdir, model_id)
        os.makedirs(model_dir, exist_ok=True)

        with patch(
            "ray.llm._internal.common.utils.upload_utils.get_model_entrypoint",
            return_value=model_dir,
        ):
            upload_model_files(model_id, "s3://bucket/model-id")

        assert mock_copy_files.call_count == 1
        assert mock_copy_files.called_with(model_dir, "bucket/model-id/", ANY, ANY)
