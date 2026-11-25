"""This script automates the process of launching and verifying a Ray cluster using a given
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
import glob
import json
import os
import re
import shlex
import shutil
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
from ray.autoscaler._private.util import fillout_defaults

# Where this job's Ray wheel goes on each node, relative to the home directory.
REMOTE_WHEEL_DIR = "release_test_ray_wheel"
# Where a node gets a virtualenv of this job's Python version when its own
# Python is another version, relative to the home directory.
REMOTE_VENV_DIR = "release_test_venv"
# How long printing the failure logs may take, so that a hung `ray exec` can't
# keep the cluster from being torn down.
FAILURE_LOGS_TIMEOUT_S = 300


def check_arguments():
    """Check command line arguments and return the cluster configuration file path, the
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
        choices=["disable", "latest", "latest-gpu", "nightly", "release"],
        default="disable",
        help=(
            "Override the docker image used for the head node and worker nodes "
            "with a public image of this job's Python version. `latest` and "
            "`latest-gpu` use the last Ray release's CPU or GPU image, and need "
            "--wheel-override to install the Ray under test on it. `nightly` "
            "uses the nightly image, which only a master build matches. "
            "`release` uses the published image of this release commit."
        ),
    )
    parser.add_argument(
        "--wheel-override",
        type=str,
        default="",
        help=(
            "Override the wheel used for the head node and worker nodes: a URL, "
            "or `commit` for the Ray wheel this release test runs."
        ),
    )
    parser.add_argument(
        "cluster_config", type=str, help="Path to the cluster configuration file"
    )
    args = parser.parse_args()

    # The latest image holds the last Ray release, not the Ray under test that
    # runs `ray up`, and `ray up` refuses a node with another Ray version.
    if args.docker_override.startswith("latest") and not args.wheel_override:
        parser.error(
            f"--docker-override {args.docker_override} needs --wheel-override, "
            "to install the Ray under test on the image."
        )

    return (
        args.cluster_config,
        args.retries,
        args.no_config_cache,
        args.num_expected_nodes,
        args.docker_override,
        args.wheel_override,
    )


def python_image_tag() -> str:
    """Return the Python part of a `rayproject/ray` image tag, like `py310`,
    for the Python that runs this job, which the Ray wheel under test is
    built for."""
    return f"py{sys.version_info.major}{sys.version_info.minor}"


def get_docker_image(docker_override: str):
    """Get the docker image to use for the head node and worker nodes.

    Every image has this job's Python version, so that the Ray wheel under
    test installs on it.

    Args:
        docker_override: The value of the --docker-override flag.

    Returns:
        The docker image to use for the head node and worker nodes, or None if not
        applicable.
    """
    py = python_image_tag()
    if docker_override == "latest":
        return f"rayproject/ray:latest-{py}"
    elif docker_override == "latest-gpu":
        return f"rayproject/ray:latest-{py}-gpu"
    elif docker_override == "nightly":
        # The nightly image holds master's Ray, whose version only a master
        # build shares. `ray up` refuses the mismatch on a release branch.
        if "dev" not in ray.__version__:
            print(
                "Error: the nightly image only matches a master build of Ray, but "
                f"this test runs Ray {ray.__version__}."
            )
            sys.exit(1)
        return f"rayproject/ray:nightly-{py}"
    elif docker_override == "release":
        # Published to Docker Hub for release branch commits only.
        if not re.match(r"^[0-9]+\.[0-9]+\.[0-9]+$", ray.__version__):
            print(
                "Error: docker image is only available for "
                f"release version, but we get: {ray.__version__}"
            )
            sys.exit(1)
        return f"rayproject/ray:{ray.__version__}.{ray.__commit__[:6]}-{py}"
    return None


def check_file(file_path: Path):
    """Check if the provided file path is valid and readable.

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


def find_local_ray_wheel() -> str:
    """Return the path of the wheel this job's Ray was installed from.

    The release test images keep the wheel they install in the home directory.

    Returns:
        The path of the wheel.

    Raises:
        SystemExit: No wheel of this job's Ray and Python version is there.
    """
    cp = f"cp{sys.version_info.major}{sys.version_info.minor}"
    pattern = os.path.expanduser(f"~/ray-{ray.__version__}-{cp}-*.whl")
    wheels = glob.glob(pattern)
    if len(wheels) != 1:
        print(f"Error: expected one Ray wheel matching {pattern}, found {wheels}")
        sys.exit(1)
    return wheels[0]


def use_job_python_command() -> str:
    """Return a setup command that puts a Python of this job's version, which
    the Ray wheel under test is built for, first on the node's PATH.

    It does nothing if the node's Python is that version already, as in an
    image picked by `get_docker_image`. Otherwise, like when a release test
    moves to a newer Python than a VM image ships, it creates a virtualenv
    of that version with uv, which downloads the Python, and puts it first on
    PATH in ~/.bashrc, as the providers' default setup commands do for their
    Python. The later setup and start commands run in new login shells, so
    they use it.
    """
    major, minor = sys.version_info[:2]
    venv = f"$HOME/{REMOTE_VENV_DIR}"
    return (
        f"python -c 'import sys; sys.exit(sys.version_info[:2] != ({major}, {minor}))'"
        f" || {{ pip install -U uv && "
        f"python -m uv venv --seed --python {major}.{minor} {venv} && "
        f"echo 'export PATH=\"{venv}/bin:$PATH\"' >> ~/.bashrc; }}"
    )


def override_wheel_with_local(
    config_yaml: dict, wheel_path: str, files_dir: str
) -> None:
    """Install the given local Ray wheel on every node.

    The provider's default setup commands install Ray from PyPI or the
    nightly wheel when a node has no Ray, so this fills in the defaults and
    replaces that step with installing the given wheel, in a Python of this
    job's version.

    Args:
        config_yaml: The cluster config, changed in place.
        wheel_path: The local path of the wheel.
        files_dir: A local directory that lives until the cluster is down. The
            wheel is copied to a directory in it, which is mounted on the
            nodes: Docker doesn't always update a bind mounted file, so
            `ray up` warns about mounting a file.
    """
    wheel_name = os.path.basename(wheel_path)
    local_wheel_dir = os.path.join(files_dir, REMOTE_WHEEL_DIR)
    os.makedirs(local_wheel_dir, exist_ok=True)
    shutil.copy(wheel_path, local_wheel_dir)
    filled_config = fillout_defaults(config_yaml)
    # Drops the default Ray install, which costs time, and which on a release
    # branch can ask PyPI for a version that isn't published yet.
    setup_commands = [
        cmd
        for cmd in filled_config.get("setup_commands", [])
        if not cmd.startswith("which ray ||")
    ]
    # Plain `pip`, as in the defaults, whose PATH setup picks the Python.
    setup_commands.append(use_job_python_command())
    setup_commands.append(
        f"pip uninstall -y ray; "
        f'pip install -U "$HOME/{REMOTE_WHEEL_DIR}/{wheel_name}[default]"'
    )
    config_yaml["setup_commands"] = setup_commands
    # An empty `file_mounts:` key in the YAML holds None.
    file_mounts = config_yaml.get("file_mounts") or {}
    file_mounts[f"~/{REMOTE_WHEEL_DIR}"] = local_wheel_dir
    config_yaml["file_mounts"] = file_mounts


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


def cleanup_cluster(config_yaml: dict, cluster_config: Path) -> None:
    """Clean up the cluster using the given cluster configuration file.

    Args:
        config_yaml: The parsed cluster configuration.
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
    """Ensure that the SSH keys for Azure tests exist, and create them if they don't."""
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


# Holds no characters that are special in a grep regex or need shell quoting.
RAYLET_LOGS_MARKER = "=== ray start failed; raylet logs on this worker ==="


def print_raylet_logs_when_worker_start_fails(config_yaml: dict) -> None:
    """Make a worker print its raylet logs when its `ray start` fails.

    The autoscaler terminates a worker whose start commands fail, so its logs
    are gone by the time the test fails. The output goes to the autoscaler
    logs on the head, which the test prints.
    """
    logs = "/tmp/ray/session_latest/logs"
    # Fill in provider defaults so configs that omit `worker_start_ray_commands`
    # keep the default start commands instead of replacing them with `[]`.
    worker_cmds = fillout_defaults(config_yaml).get("worker_start_ray_commands") or []
    if not worker_cmds:
        return
    config_yaml["worker_start_ray_commands"] = [
        (
            f"{{ {cmd.strip().rstrip(';')}; }} || {{ echo '{RAYLET_LOGS_MARKER}'; "
            f"tail -n 60 {logs}/raylet.out {logs}/raylet.err; false; }}"
        )
        if "ray start" in cmd
        else cmd
        for cmd in worker_cmds
    ]


def describe_aws_instances(config: dict) -> str:
    """Return the cluster's EC2 instances, including terminated ones.

    The state reason of a terminated worker tells whether AWS reclaimed a spot
    instance, which the autoscaler doesn't log.
    """
    if config.get("provider", {}).get("type") != "aws":
        return ""
    try:
        ec2_client = boto3.client("ec2", region_name=config["provider"]["region"])
        response = ec2_client.describe_instances(
            Filters=[
                {"Name": "tag:ray-cluster-name", "Values": [config["cluster_name"]]}
            ]
        )
    except Exception as e:
        return f"Couldn't describe the cluster's instances: {e}"
    lines = []
    for reservation in response["Reservations"]:
        for instance in reservation["Instances"]:
            tags = {t["Key"]: t["Value"] for t in instance.get("Tags", [])}
            lines.append(
                " ".join(
                    [
                        instance["LaunchTime"].isoformat(),
                        instance["InstanceId"],
                        tags.get("ray-node-type", "?"),
                        instance.get("InstanceLifecycle", "on-demand"),
                        instance["State"]["Name"],
                        tags.get("ray-node-status", "?"),
                        instance.get("StateReason", {}).get("Message", ""),
                    ]
                )
            )
    return "\n".join(sorted(lines))


def get_failure_logs(
    config_yaml: dict, cluster_config: Path, no_config_cache: bool, env: dict
) -> str:
    """Return the logs that show why a worker didn't join the cluster.

    They are the autoscaler logs on the head, which include the setup output
    of the workers, the head's GCS log, and the raylet logs of each worker.
    """
    logs = "/tmp/ray/session_latest/logs"
    ssh_user = (
        config_yaml.get("auth", {}).get("ssh_user")
        or fillout_defaults(config_yaml)["auth"]["ssh_user"]
    )
    container = config_yaml.get("docker", {}).get("container_name")
    on_worker = f"docker exec {shlex.quote(container)} " if container else ""
    script = (
        f"echo '--- Worker launches and failures:'; "
        f"grep -hiE 'Launched instance|Failed to launch|Attempt failed|capacity|"
        f"Terminating instance|Stopping instance|commands failed|update-failed' "
        f"{logs}/monitor* | grep -v 'Full command is' | tail -n 40; "
        f"echo '--- Raylet logs of workers whose ray start failed:'; "
        # Matches the marker at the end of a line, after any prefix, but not
        # the logged copies of the worker start command that echoes it, where
        # the rest of the command follows the marker. A carriage return can
        # end the line, as the command runs in a terminal.
        f"grep -hE -A 125 '{RAYLET_LOGS_MARKER}[[:space:]]*$' {logs}/monitor* "
        f"| tail -n 250; "
        f"echo '--- Autoscaler errors:'; "
        f"grep -hiE 'error|exception|failed|denied|unauthorized|no space' "
        f"{logs}/monitor* | grep -v 'Full command is' | tail -n 30; "
        f"echo '--- Autoscaler status:'; tail -n 25 {logs}/monitor.log; "
        f"echo '--- GCS on the head:'; "
        f"grep -hiE 'Registering new node|unauthenticated|token|error' "
        f"{logs}/gcs_server.out | grep -v 'Debug state' | tail -n 30; "
        f"for ip in $(ray get-worker-ips ~/ray_bootstrap_config.yaml 2>/dev/null "
        f"| grep -E '^[0-9]+([.][0-9]+){{3}}$'); do "
        f'echo "--- Raylet on worker $ip:"; '
        f"ssh -i ~/ray_bootstrap_key.pem -o StrictHostKeyChecking=no "
        f"-o UserKnownHostsFile=/dev/null -o ConnectTimeout=10 "
        f"{shlex.quote(ssh_user)}@$ip "
        f'"{on_worker}tail -n 40 {logs}/raylet.out {logs}/raylet.err"; '
        f"done"
    )
    cmd = ["ray", "exec", str(cluster_config), script]
    if no_config_cache:
        cmd.append("--no-config-cache")
    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            env=env,
            timeout=FAILURE_LOGS_TIMEOUT_S,
        )
    except subprocess.TimeoutExpired as e:
        partial = (e.stdout or b"").decode("utf-8", errors="replace")
        return f"{partial}\nTimed out after {FAILURE_LOGS_TIMEOUT_S}s."
    except Exception as e:
        return f"Couldn't fetch the failure logs: {e}"
    return result.stdout.decode("utf-8", errors="replace")


def run_ray_commands(
    config_yaml: dict,
    cluster_config: Path,
    retries: int,
    no_config_cache: bool,
    num_expected_nodes: int = 1,
):
    """Run the necessary Ray commands to start a cluster, verify Ray is running, and clean
    up the cluster.

    Args:
        config_yaml: The parsed cluster configuration.
        cluster_config: The path of the cluster configuration file.
        retries: The number of retries for the verification step.
        no_config_cache: Whether to pass the --no-config-cache flag to the ray CLI
            commands.
        num_expected_nodes: The number of nodes that should be running before
            verification is considered successful.
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
        # print stdout and stderr
        print(f"stdout:\n{e.stdout.decode('utf-8', errors='replace')}")
        print(f"stderr:\n{e.stderr.decode('utf-8', errors='replace')}")
        # `ray up` can fail after it launched the head node, for example when
        # the head node runs another Ray version. Don't mask the original error
        # if cleanup itself fails.
        try:
            cleanup_cluster(config_yaml, cluster_config)
        except Exception as cleanup_err:
            print(f"Warning: cleanup after `ray up` failure failed: {cleanup_err}")
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
        # Printed before the cleanup, whose output is short: the release test
        # harness keeps only the end of the output, and can miss the last lines.
        try:
            print("======================================")
            print("Logs that show why workers didn't join:")
            print(get_failure_logs(config_yaml, cluster_config, no_config_cache, env))
            aws_instances = describe_aws_instances(config_yaml)
            if aws_instances:
                print("--- The cluster's instances (launch time, id, node type, ")
                print("lifecycle, state, Ray node status, state reason):")
                print(aws_instances)
            print("======================================")
        finally:
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

    # Holds files shipped to the nodes, like the Ray wheel, until the script
    # exits.
    with tempfile.TemporaryDirectory() as files_dir:
        print("======================================")
        print(f"Overriding docker image...: {docker_override}")
        docker_override_image = get_docker_image(docker_override)
        print(f"Using docker image: {docker_override_image}")
        if docker_override_image:
            override_docker_image(config_yaml, docker_override_image)

        # After the image, as the wheel's setup commands run in its container.
        print("======================================")
        print(f"Overriding ray wheel...: {wheel_override}")
        if wheel_override == "commit":
            wheel_path = find_local_ray_wheel()
            print(f"Using this job's Ray wheel: {wheel_path}")
            override_wheel_with_local(config_yaml, wheel_path, files_dir)
        elif wheel_override:
            override_wheels_url(config_yaml, wheel_override)

        print_raylet_logs_when_worker_start_fails(config_yaml)

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
                config_yaml,
                cluster_config,
                retries,
                no_config_cache,
                num_expected_nodes,
            )
