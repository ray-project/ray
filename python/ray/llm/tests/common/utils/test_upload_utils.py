import os
import sys
import tempfile
from pathlib import Path
from unittest.mock import ANY, call, patch

import pytest

from ray.llm._internal.common.utils.upload_utils import upload_model_files


@patch("pyarrow.fs.copy_files")
def test_upload_undownloaded_model(mock_copy_files):
    model_id = "fake-model-id"

    with tempfile.TemporaryDirectory() as tempdir:
        model_dir = os.path.join(tempdir, model_id)
        # mock snapshot_download to create a subdir in the model_dir
        def create_subdir_side_effect(*args, **kwargs):
            os.makedirs(model_dir, exist_ok=True)

        with patch(
            "ray.llm._internal.common.utils.upload_utils.get_model_entrypoint",
            return_value=model_dir,
        ), patch(
            "huggingface_hub.snapshot_download", side_effect=create_subdir_side_effect
        ):
            # upload the model
            upload_model_files(model_id, "gs://bucket/model-id")

        # check that the model was uploaded1
        mock_copy_files.assert_called_once_with(
            source=Path(model_dir),
            destination="bucket/model-id",
            source_filesystem=ANY,
            destination_filesystem=ANY,
        )


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
        mock_copy_files.assert_has_calls(
            [
                call(
                    source=os.path.join(model_dir, "snapshots", hash),
                    destination="bucket/model-id",
                    source_filesystem=ANY,
                    destination_filesystem=ANY,
                ),
                call(
                    source=os.path.join(model_rev_path, "main"),
                    destination="bucket/model-id/hash",
                    source_filesystem=ANY,
                    destination_filesystem=ANY,
                ),
            ],
            any_order=True,
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

        mock_copy_files.assert_called_once_with(
            source=Path(model_dir),
            destination="bucket/model-id",
            source_filesystem=ANY,
            destination_filesystem=ANY,
        )


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
