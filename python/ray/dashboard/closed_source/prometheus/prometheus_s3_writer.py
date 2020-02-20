"""S3 Writer Process

NOTE: This process should run in the same container where Prometheus
    server is running.
"""
import logging
import os
import time
import traceback
import zipfile

# TODO(sang) pip install boto3 awscli
import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__file__)


class PrometheusS3Writer:
    """This class writes prometheus DB to S3 after zipping it.

    Arguments:
        prometheus_db_path (str): Location where prometheus db is
            directory is stored.
        bucket (str): S3 bucket name
        key (str): S3 key to store the file.
        zip_temp_dir (str): Path to directory where writer will
            temporarily create a zip file before uploading to s3.
    """

    def __init__(self, prometheus_db_path, bucket, key, zip_temp_dir):
        self.prometheus_db_path = prometheus_db_path
        self.bucket = bucket
        self.key = key
        self.zip_temp_dir = zip_temp_dir
        self.zip_file_path = "{}/{}.zip".format(zip_temp_dir, key)

        self.s3 = boto3.client("s3")

    def _zip_prometheus_db(self):
        """Zip Prometheus DB (which is a directory)

        Zip Directory Structure:
        - [user_key]/[db]/[db_contents]
        """
        # TODO(sang): Find tes best compression method
        with zipfile.ZipFile(self.zip_file_path, mode="w") as zf:
            for folder_name, _, filenames in os.walk(self.prometheus_db_path):
                for filename in filenames:
                    file_path = os.path.join(folder_name, filename)
                    parent_path = os.path.relpath(file_path,
                                                  self.prometheus_db_path)
                    arcname = os.path.join(self.key, "db", parent_path)

                    zf.write(file_path, arcname=arcname)

    def write(self):
        try:
            logger.info("Zipping Prometheus DB")
            self._zip_prometheus_db()
            logger.info("Upload a zip file to S3")
            self.s3.upload_file(self.zip_file_path, self.bucket, self.key)
        except ClientError as e:
            logger.error(e)
            logger.error("S3 Client Error occured")
            logger.error(traceback.format_exc())
        except Exception as e:
            logger.error(e)
            logger.error(traceback.format_exc())
        finally:
            if os.path.exists(self.zip_file_path):
                os.remove(self.zip_file_path)


if __name__ == "__main__":
    # TODO(sang): Change these values to be configurable
    prometheus_db_path = "/tmp/prometheus/prometheus"
    update_frequency = 600
    bucket = "hosted-dashboard-test"
    key = "sangcho_user_session_id"
    zip_temp_dir = "/tmp/prometheus"

    s3_writer = PrometheusS3Writer(prometheus_db_path, bucket, key,
                                   zip_temp_dir)
    zip_file_path = "{}/{}.zip".format(zip_temp_dir, key)

    while True:
        s3_writer.write()
        logger.info(
            "Writing done. Waiting for {} seconds.".format(update_frequency))
        time.sleep(update_frequency)
