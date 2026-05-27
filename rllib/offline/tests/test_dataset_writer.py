import tempfile
import unittest
from unittest.mock import patch

from ray.rllib.offline import IOContext
from ray.rllib.offline.dataset_writer import DatasetWriter


class _DummyDataset:
    def repartition(self, *args, **kwargs):
        return self

    def write_json(self, *args, **kwargs):
        pass

    def write_parquet(self, *args, **kwargs):
        pass


class TestDatasetWriter(unittest.TestCase):
    @patch(
        "ray.rllib.offline.dataset_writer.data.from_items",
        return_value=_DummyDataset(),
    )
    def test_unsupported_output_type_error_message(self, _mock_from_items):
        with tempfile.TemporaryDirectory() as tmp_dir:
            ioctx = IOContext(
                config={
                    "output_config": {
                        "format": "csv",
                        "path": tmp_dir,
                        "max_num_samples_per_file": 1,
                    }
                }
            )
            writer = DatasetWriter(ioctx=ioctx)

            with self.assertRaisesRegex(ValueError, "Unknown output type: csv"):
                writer.write({"obs": [1], "actions": [0]})


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
