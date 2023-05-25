"""
This script automates the process of launching and verifying a Ray cluster using a given
cluster configuration file. It also handles cluster cleanup before and after the
verification process. The script requires one command-line argument: the path to the
cluster configuration file.

Usage:
    python launch_and_verify_cluster.py [--no-config-cache] [--retries NUM_RETRIES]
        <cluster_configuration_file_path>

Example:
    python launch_and_verify_cluster.py --retries 5 --no-config-cache
        /path/to/cluster_config.yaml
"""
import argparse
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import boto3
import yaml


def check_arguments():
    """
    Check command line arguments and return the cluster configuration file path, the
    number of retries, and the value of the --no-config-cache flag.

    Returns:
        A tuple containing the cluster config file path, the number of retries, and the
        value of the --no-config-cache flag.
    """
    parser = argparse.ArgumentParser(description="Launch and verify a Ray cluster")
    parser.add_argument(
        "--no-config-cache",
        action="store_true",
        help="Pass the --no-config-cache flag to Ray CLI commands",
    )
    parser.add_argument(
        "--retries",
        type=int,
        default=3,
        help="Number of retries for verifying Ray is running (default: 3)",
    )
    parser.add_argument(
        "cluster_config", type=str, help="Path to the cluster configuration file"
    )
    args = parser.parse_args()

    return args.cluster_config, args.retries, args.no_config_cache


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


def run_ray_commands(cluster_config, retries, no_config_cache):
    """
    Run the necessary Ray commands to start a cluster, verify Ray is running, and clean
    up the cluster.

    Args:
        cluster_config: The path of the cluster configuration file.
        retries: The number of retries for the verification step.
        no_config_cache: Whether to pass the --no-config-cache flag to the ray CLI
            commands.
    """
    print("======================================")
    cleanup_cluster(cluster_config)

    print("======================================")
    print("Starting new cluster...")
    cmd = ["ray", "up", "-v", "-y"]
    if no_config_cache:
        cmd.append("--no-config-cache")
    cmd.append(str(cluster_config))
    subprocess.run(cmd, check=True)

    print("======================================")
    print("Verifying Ray is running...")

    success = False
    count = 0
    while count < retries:
        try:
            cmd = [
                "ray",
                "exec",
                "-v",
                str(cluster_config),
                "python -c 'import ray; ray.init(\"localhost:6379\")'",
            ]
            if no_config_cache:
                cmd.append("--no-config-cache")
            subprocess.run(cmd, check=True)
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
    print("Finished executing script successfully.")


if __name__ == "__main__":
    cluster_config, retries, no_config_cache = check_arguments()
    cluster_config = Path(cluster_config)
    check_file(cluster_config)

    print(f"Using cluster configuration file: {cluster_config}")
    print(f"Number of retries for 'verify ray is running' step: {retries}")
    print(f"Using --no-config-cache flag: {no_config_cache}")

    config_yaml = yaml.safe_load(cluster_config.read_text())
    provider_type = config_yaml.get("provider", {}).get("type")
    if provider_type == "aws":
        download_ssh_key()
        run_ray_commands(cluster_config, retries, no_config_cache)
    elif provider_type == "gcp":
        print("======================================")
        print("GCP provider detected. Skipping ssh key download step.")
        # Get the active account email
        account_email = (
            subprocess.run(
                ["gcloud", "config", "get-value", "account"],
                stdout=subprocess.PIPE,
                check=True,
            )
            .stdout.decode("utf-8")
            .strip()
        )
        print("Active account email:", account_email)
        # Get the current project ID
        project_id = (
            subprocess.run(
                ["gcloud", "config", "get-value", "project"],
                stdout=subprocess.PIPE,
                check=True,
            )
            .stdout.decode("utf-8")
            .strip()
        )
        print(
            f"Injecting GCP project '{project_id}' into cluster configuration file..."
        )
        config_yaml["provider"]["project_id"] = project_id

        # Create a new temporary file and dump the updated configuration into it
        with tempfile.NamedTemporaryFile(suffix=".yaml") as temp:
            temp.write(yaml.dump(config_yaml).encode("utf-8"))
            temp.flush()
            cluster_config = Path(temp.name)
            run_ray_commands(cluster_config, retries, no_config_cache)

    else:
        print("======================================")
        print("Provider type not recognized. Exiting script.")
        sys.exit(1)
