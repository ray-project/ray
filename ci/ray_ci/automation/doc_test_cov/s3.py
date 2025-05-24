import boto3
from typing import List
class S3DataSource:

    def __init__(self, bucket, commit, job_ids: List[str]):
        # boto3 will automatically use credentials from ~/.aws/credentials or environment variables
        self.s3_client = boto3.client("s3")
        self.bucket = bucket
        self.paths = [f"bazel_events/master/{commit}/{job_id}/" for job_id in job_ids]
        self.file_names = []

    def list_all_bazel_events(self):
        for path in self.paths:
            response = self.s3_client.list_objects_v2(Bucket=self.bucket, Prefix=path)
            if "Contents" in response:
                self.file_names.extend(response["Contents"])
                print(f"Found {len(response['Contents'])} files in {path}")
            else:
                print(f"No files found in {path}")

    def download_all_bazel_events(self, download_dir) -> List[str]:
        bazel_file_names = []
        for file_info in self.file_names:
            print(f"downloading file {file_info['Key']}")
            fn = file_info["Key"].split("/")[-1]
            self.s3_client.download_file(Bucket=self.bucket, Key=file_info["Key"], Filename=f"{download_dir}/{fn}")
            bazel_file_names.append(f"{download_dir}/{fn}")
        print(f"Downloaded {len(self.file_names)} files to {download_dir}")
        return bazel_file_names
