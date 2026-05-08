"""
This script automates the process of launching and verifying a Ray cluster using a given
cluster configuration file. It also handles cluster cleanup before and after the
verification process. The script requires one command-line argument: the path to the
cluster configuration file.

Usage:
    python launch_and_verify_cluster.py [--no-config-cache] [--retries NUM_RETRIES]
        [--num-expected-nodes NUM_NODES] [--docker-override DOCKER_OVERRIDE]
        [--wheel-override WHEEL_OVERRIDE]
        <cluster_configuration_file_path>
"""
import argparse
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

import boto3
import botocore
import yaml
from google.cloud import storage

import ray
from ray.autoscaler._private.aws.config import RAY


def check_arguments():
    """
    Check command line arguments and return the cluster configuration file path, the
    number of retries, the number of expected nodes, and the value of the
    --no-config-cache flag.
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
        "--num-expected-nodes",
        type=int,
        default=1,
        help="Number of nodes for verifying Ray is running (default: 1)",
    )
    parser.add_argument(
        "--docker-override",
        choices=["disable", "latest", "nightly", "commit"],
        default="disable",
        help="Override the docker image used for the head node and worker nodes",
    )
    parser.add_argument(
        "--wheel-override",
        type=str,
        default="",
        help="Override the wheel used for the head node and worker nodes",
    )
    parser.add_argument(
        "cluster_config", type=str, help="Path to the cluster configuration file"
    )
    args = parser.parse_args()

    assert not (
        args.docker_override != "disable" and args.wheel_override != ""
    ), "Cannot override both docker and wheel"

    return (
        args.cluster_config,
        args.retries,
        args.no_config_cache,
        args.num_expected_nodes,
        args.docker_override,
        args.wheel_override,
    )


def get_docker_image(docker_override):
    """
    Get the docker image to use for the head node and worker nodes.

    Args:
        docker_override: The value of the --docker-override flag.

    Returns:
        The docker image to use for the head node and worker nodes, or None if not
        applicable.
    """
    if docker_override == "latest":
        return "rayproject/ray:latest-py310"
    elif docker_override == "nightly":
        return "rayproject/ray:nightly-py310"
    elif docker_override == "commit":
        if re.match("^[0-9]+.[0-9]+.[0-9]+$", ray.__version__):
            return f"rayproject/ray:{ray.__version__}.{ray.__commit__[:6]}-py310"
        else:
            print(
                "Error: docker image is only available for "
                f"release version, but we get: {ray.__version__}"
            )
            sys.exit(1)
    return None


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


def override_wheels_url(config_yaml, wheel_url):
    setup_commands = config_yaml.get("setup_commands", [])
    setup_commands.append(
        f'pip3 uninstall -y ray && pip3 install -U "ray[default] @ {wheel_url}"'
    )
    config_yaml["setup_commands"] = setup_commands


def override_docker_image(config_yaml, docker_image):
    docker_config = config_yaml.get("docker", {})
    docker_config["image"] = docker_image
    docker_config["container_name"] = "ray_container"
    assert docker_config.get("head_image") is None, "Cannot override head_image"
    assert docker_config.get("worker_image") is None, "Cannot override worker_image"
    config_yaml["docker"] = docker_config


def azure_authenticate():
    """Get Azure service principal credentials from AWS Secrets Manager and authenticate."""
    print("======================================")
    print("Getting Azure credentials from AWS Secrets Manager...")

    # Initialize AWS Secrets Manager client
    secrets_client = boto3.client("secretsmanager", region_name="us-west-2")

    # Get service principal credentials
    secret_response = secrets_client.get_secret_value(
        SecretId="azure-service-principal-oss-release"
    )
    secret = secret_response["SecretString"]

    client_id = json.loads(secret)["client_id"]
    tenant_id = json.loads(secret)["tenant_id"]

    # Get certificate
    cert_response = secrets_client.get_secret_value(
        SecretId="azure-service-principal-certificate"
    )
    cert = cert_response["SecretString"]

    # Write certificate to temp file
    tmp_dir = tempfile.mkdtemp()
    cert_path = os.path.join(tmp_dir, "azure_cert.pem")
    with open(cert_path, "w") as f:
        f.write(cert)

    # Login to Azure
    subprocess.check_call(
        [
            "az",
            "login",
            "--service-principal",
            "--username",
            client_id,
            "--certificate",
            cert_path,
            "--tenant",
            tenant_id,
        ]
    )


def download_ssh_key_aws():
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


def download_ssh_key_gcp():
    """Download the ssh key from the google cloud bucket to the local machine."""
    print("======================================")
    print("Downloading ssh key from GCP...")

    # Initialize the GCP storage client
    client = storage.Client()

    # Set the name of the GCS bucket and the blob (key) to download
    bucket_name = "gcp-cluster-launcher-release-test-ssh-keys"
    key_name = "ray-autoscaler_gcp_us-west1_anyscale-bridge-cd812d38_ubuntu_0.pem"

    # Get the bucket and blob
    bucket = client.get_bucket(bucket_name)
    blob = bucket.get_blob(key_name)

    # Download the blob to a local file
    local_key_path = os.path.expanduser(f"~/.ssh/{key_name}")
    if not os.path.exists(os.path.dirname(local_key_path)):
        os.makedirs(os.path.dirname(local_key_path))
    blob.download_to_filename(local_key_path)

    # Set permissions on the key file
    os.chmod(local_key_path, 0o400)


def cleanup_cluster(config_yaml, cluster_config):
    """
    Clean up the cluster using the given cluster configuration file.

    Args:
        cluster_config: The path of the cluster configuration file.
    """
    print("======================================")
    print("Cleaning up cluster...")

    # We do multiple retries here because sometimes the cluster
    # fails to clean up properly, resulting in a non-zero exit code (e.g.
    # when processes have to be killed forcefully).

    last_error = None
    num_tries = 3
    for i in range(num_tries):
        try:
            env = os.environ.copy()
            env.pop("PYTHONPATH", None)
            subprocess.run(
                ["ray", "down", "-v", "-y", str(cluster_config)],
                check=True,
                capture_output=True,
                env=env,
            )
            cleanup_security_groups(config_yaml)
            # Final success
            return
        except subprocess.CalledProcessError as e:
            print(f"ray down fails[{i+1}/{num_tries}]: ")
            print(e.output.decode("utf-8"))

            # Print full traceback
            traceback.print_exc()

            # Print stdout and stderr from ray down
            print(f"stdout:\n{e.stdout.decode('utf-8')}")
            print(f"stderr:\n{e.stderr.decode('utf-8')}")

            last_error = e

    raise last_error


def cleanup_security_group(ec2_client, id):
    retry = 0
    while retry < 10:
        try:
            ec2_client.delete_security_group(GroupId=id)
            return
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "DependencyViolation":
                sleep_time = 2**retry
                print(
                    f"Waiting {sleep_time}s for the instance to be terminated before deleting the security group {id}"  # noqa E501
                )
                time.sleep(sleep_time)
                retry += 1
            else:
                print(f"Error deleting security group: {e}")
                return


def ensure_ssh_keys_azure():
    """
    Ensure that the SSH keys for Azure tests exist, and create them if they don't.
    """
    print("======================================")
    print("Ensuring Azure SSH keys exist...")
    private_key_path = os.path.expanduser("~/.ssh/ray-autoscaler-tests-ssh-key")
    public_key_path = os.path.expanduser("~/.ssh/ray-autoscaler-tests-ssh-key.pub")

    if os.path.exists(private_key_path) and os.path.exists(public_key_path):
        print("Azure SSH keys already exist.")
        return

    print("Azure SSH keys not found. Creating new keys...")
    ssh_dir = os.path.dirname(private_key_path)
    if not os.path.exists(ssh_dir):
        os.makedirs(ssh_dir, exist_ok=True)

    try:
        subprocess.run(
            [
                "ssh-keygen",
                "-t",
                "rsa",
                "-b",
                "4096",
                "-f",
                private_key_path,
                "-N",
                "",
                "-C",
                "ray-autoscaler-azure",
            ],
            check=True,
            capture_output=True,
        )
        print("Successfully created Azure SSH keys.")
    except subprocess.CalledProcessError as e:
        print("Error creating SSH keys:")
        print(f"stdout:\n{e.stdout.decode('utf-8')}")
        print(f"stderr:\n{e.stderr.decode('utf-8')}")
        sys.exit(1)


def cleanup_security_groups(config):
    provider_type = config.get("provider", {}).get("type")
    if provider_type != "aws":
        return

    try:
        ec2_client = boto3.client("ec2", region_name="us-west-2")
        response = ec2_client.describe_security_groups(
            Filters=[
                {
                    "Name": "tag-key",
                    "Values": [RAY],
                },
                {
                    "Name": "tag:ray-cluster-name",
                    "Values": [config["cluster_name"]],
                },
            ]
        )
        for security_group in response["SecurityGroups"]:
            cleanup_security_group(ec2_client, security_group["GroupId"])
    except Exception as e:
        print(f"Error cleaning up security groups: {e}")


def run_ray_commands(
    config_yaml,
    cluster_config,
    retries,
    no_config_cache,
    num_expected_nodes=1,
):
    """
    Run the necessary Ray commands to start a cluster, verify Ray is running, and clean
    up the cluster.

    Args:
        cluster_config: The path of the cluster configuration file.
        retries: The number of retries for the verification step.
        no_config_cache: Whether to pass the --no-config-cache flag to the ray CLI
            commands.
    """
    provider_type = config_yaml.get("provider", {}).get("type")
    if provider_type == "azure":
        azure_authenticate()
    print("======================================")
    print("Starting new cluster...")
    cmd = ["ray", "up", "-v", "-y"]
    if no_config_cache:
        cmd.append("--no-config-cache")
    cmd.append(str(cluster_config))
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    try:
        subprocess.run(cmd, check=True, capture_output=True, env=env)
    except subprocess.CalledProcessError as e:
        print(e.output)
        # print stdout and stderr
        print(f"stdout:\n{e.stdout.decode('utf-8')}")
        print(f"stderr:\n{e.stderr.decode('utf-8')}")
        raise e

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
                (
                    'python -c \'import ray; ray.init("localhost:6379");'
                    + f" assert len(ray.nodes()) >= {num_expected_nodes}'"
                ),
            ]
            if no_config_cache:
                cmd.append("--no-config-cache")
            subprocess.run(cmd, check=True, env=env)
            success = True
            break
        except subprocess.CalledProcessError:
            count += 1
            print(f"Verification failed. Retry attempt {count} of {retries}...")
            time.sleep(60)

    if not success:
        print("======================================")
        print(
            f"Error: Verification failed after {retries} attempts. Cleaning up cluster "
            "before exiting..."
        )
        cleanup_cluster(config_yaml, cluster_config)
        print("======================================")
        print("Exiting script.")
        sys.exit(1)

    print("======================================")
    print("Ray verification successful.")

    cleanup_cluster(config_yaml, cluster_config)

    print("======================================")
    print("Finished executing script successfully.")


if __name__ == "__main__":
    (
        cluster_config,
        retries,
        no_config_cache,
        num_expected_nodes,
        docker_override,
        wheel_override,
    ) = check_arguments()
    cluster_config = Path(cluster_config)
    check_file(cluster_config)

    print(f"Using cluster configuration file: {cluster_config}")
    print(f"Number of retries for 'verify ray is running' step: {retries}")
    print(f"Using --no-config-cache flag: {no_config_cache}")
    print(f"Number of expected nodes for 'verify ray is running': {num_expected_nodes}")

    config_yaml = yaml.safe_load(cluster_config.read_text())
    # Make the cluster name unique
    config_yaml["cluster_name"] = (
        config_yaml["cluster_name"] + "-" + str(int(time.time()))
    )

    print("======================================")
    print(f"Overriding ray wheel...: {wheel_override}")
    if wheel_override:
        override_wheels_url(config_yaml, wheel_override)

    print("======================================")
    print(f"Overriding docker image...: {docker_override}")
    docker_override_image = get_docker_image(docker_override)
    print(f"Using docker image: {docker_override_image}")
    if docker_override_image:
        override_docker_image(config_yaml, docker_override_image)

    provider_type = config_yaml.get("provider", {}).get("type")
    config_yaml["provider"]["cache_stopped_nodes"] = False
    if provider_type == "azure":
        ensure_ssh_keys_azure()
    elif provider_type == "aws":
        download_ssh_key_aws()
        config_yaml["provider"].pop("availability_zone", None)
    elif provider_type == "gcp":
        download_ssh_key_gcp()
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
    elif provider_type == "vsphere":
        print("======================================")
        print("VSPHERE provider detected.")
    else:
        print("======================================")
        print("Provider type not recognized. Exiting script.")
        sys.exit(1)

    # Create a new temporary file and dump the updated configuration into it
    with tempfile.NamedTemporaryFile(suffix=".yaml") as temp:
        temp.write(yaml.dump(config_yaml).encode("utf-8"))
        temp.flush()
        cluster_config = Path(temp.name)
        run_ray_commands(
            config_yaml, cluster_config, retries, no_config_cache, num_expected_nodes
        )
