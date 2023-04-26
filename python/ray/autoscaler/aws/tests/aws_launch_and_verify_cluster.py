"""
This script automates the process of launching and verifying a Ray cluster using a given
cluster configuration file. It also handles cluster cleanup before and after the
verification process. The script requires two command-line arguments: the path to the
cluster configuration file and an optional number of retries for the verification step.

Usage:
    python aws_launch_and_verify_cluster.py <cluster_configuration_file_path> [retries]

Example:
    python aws_launch_and_verify_cluster.py /path/to/cluster_config.yaml 5
"""
import os
import subprocess
import sys
import time
from pathlib import Path

import boto3


def check_arguments(args):
    """
    Check command line arguments and return the cluster configuration file path and the
    number of retries.

    Args:
        args: The list of command line arguments.

    Returns:
        A tuple containing the cluster config file path and the number of retries.

    Raises:
        SystemExit: If an incorrect number of command line arguments is provided.
    """
    if len(args) < 2:
        print(
            "Error: Please provide a path to the cluster configuration file as a "
            "command line argument."
        )
        sys.exit(1)
    return args[1], int(args[2]) if len(args) >= 3 else 3


def check_file(file_path):
    """
    Check if the provided file path is valid and readable.

    Args:
        file_path: The path of the file to check.

    Raises:
        SystemExit: If the file is not readable or does not exist.
    """
    if not file_path.is_file() or not os.access(file_path, os.R_OK):
        print(f"Error: Cannot read cluster configuration file: {file_path}")
        sys.exit(1)


def download_ssh_key():
    """Download the ssh key from the S3 bucket to the local machine."""
    print("======================================")
    print("Downloading ssh key...")
    # Create a Boto3 client to interact with S3
    s3_client = boto3.client("s3", region_name="us-west-2")

    # Set the name of the S3 bucket and the key to download
    bucket_name = "aws-cluster-launcher-test"
    key_name = "ray-autoscaler_59_us-west-2.pem"

    # Download the key from the S3 bucket to a local file
    local_key_path = os.path.expanduser(f"~/.ssh/{key_name}")
    if not os.path.exists(os.path.dirname(local_key_path)):
        os.makedirs(os.path.dirname(local_key_path))
    s3_client.download_file(bucket_name, key_name, local_key_path)

    # Set permissions on the key file
    os.chmod(local_key_path, 0o400)


def cleanup_cluster(cluster_config):
    """
    Clean up the cluster using the given cluster configuration file.

    Args:
        cluster_config: The path of the cluster configuration file.
    """
    print("======================================")
    print("Cleaning up cluster...")
    subprocess.run(["ray", "down", "-v", "-y", str(cluster_config)], check=True)


def run_ray_commands(cluster_config, retries):
    """
    Run the necessary Ray commands to start a cluster, verify Ray is running, and clean
    up the cluster.

    Args:
        cluster_config: The path of the cluster configuration file.
        retries: The number of retries for the verification step.
    """
    print("======================================")
    cleanup_cluster(cluster_config)

    print("======================================")
    print("Starting new cluster...")
    subprocess.run(["ray", "up", "-v", "-y", str(cluster_config)], check=True)

    print("======================================")
    print("Verifying Ray is running...")

    success = False
    count = 0
    while count < retries:
        try:
            subprocess.run(
                [
                    "ray",
                    "exec",
                    "-v",
                    str(cluster_config),
                    "python -c 'import ray; ray.init(\"localhost:6379\")'",
                ],
                check=True,
            )
            success = True
            break
        except subprocess.CalledProcessError:
            count += 1
            print(f"Verification failed. Retry attempt {count} of {retries}...")
            time.sleep(5)

    if not success:
        print("======================================")
        print(
            f"Error: Verification failed after {retries} attempts. Cleaning up cluster "
            "before exiting..."
        )
        cleanup_cluster(cluster_config)
        print("======================================")
        print("Exiting script.")
        sys.exit(1)

    print("======================================")
    print("Ray verification successful.")

    cleanup_cluster(cluster_config)

    print("======================================")
    print("Finished executing script.")


if __name__ == "__main__":
    cluster_config, retries = check_arguments(sys.argv)
    cluster_config = Path(cluster_config)
    check_file(cluster_config)

    print(f"Using cluster configuration file: {cluster_config}")
    print(f"Number of retries for 'verify ray is running' step: {retries}")

    download_ssh_key()
    run_ray_commands(cluster_config, retries)
