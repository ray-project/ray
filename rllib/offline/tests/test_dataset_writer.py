import os
import tempfile
import unittest

import itertools
import boto3
from moto import mock_s3
from ray.rllib.offline.io_context import IOContext
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.offline.dataset_writer import DatasetWriter


class TestDatasetWriter(unittest.TestCase):
    def setUp(self):
        self.ioctx = IOContext()
        self.sample_batch = SampleBatch({"obs": [0, 1], "new_obs": [1, 2]})

    def test_writer_local(self):
        formats = ["json", "parquet"]
        is_multiagents = [False, True]
        for format, is_multiagent in itertools.product(formats, is_multiagents):
            with tempfile.TemporaryDirectory() as tmpdir:
                self.ioctx.config["output_config"] = {"format": format, "path": tmpdir}

                with DatasetWriter(self.ioctx) as writer:
                    if is_multiagent:
                        writer.write(self.sample_batch.as_multi_agent())
                    else:
                        writer.write(self.sample_batch)

                # check if there is at least one json file in the directory
                self.assertTrue(any([
                    filename.endswith(f".{format}")
                    for filename in os.listdir(tmpdir)
                ]))
    @mock_s3
    def test_writer_remote(self):
        bucket_name = "test-bucket"
        s3_resource = boto3.resource("s3")
        s3_resource.create_bucket(Bucket=bucket_name)
        s3_client = boto3.client("s3")


        formats = ["json", "parquet"]
        is_multiagents = [False, True]
        
        for format, is_multiagent in itertools.product(formats, is_multiagents):
            print("format: ", format)
            print("is_multiagent: ", is_multiagent)
            s3_path = f"s3://{bucket_name}/{format}-{is_multiagent}/"
            self.ioctx.config["output_config"] = {"format": format, "path": s3_path}

            with DatasetWriter(self.ioctx) as writer:
                if is_multiagent:
                    writer.write(self.sample_batch.as_multi_agent())
                else:
                    writer.write(self.sample_batch)

            # check if there is at least one file with the correct format in the S3 bucket
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=f"{format}-{is_multiagent}")
            self.assertIn("Contents", response)
            self.assertTrue(any([key.endswith(f".{format}") for key in [item["Key"] for item in response["Contents"]]]))

    


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))