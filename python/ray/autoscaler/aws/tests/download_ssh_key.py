import os

import boto3

# Create a Boto3 client to interact with S3
s3_client = boto3.client("s3", region_name="us-west-2")

# Set the name of the S3 bucket and the key to download
bucket_name = "oss-release-test-ssh-keys"
key_name = "ray-autoscaler_59_us-west-2.pem"

# Download the key from the S3 bucket to a local file
local_key_path = os.path.expanduser(f"~/.ssh/{key_name}")
s3_client.download_file(bucket_name, key_name, local_key_path)

# Set permissions on the key file
os.chmod(local_key_path, 0o400)
