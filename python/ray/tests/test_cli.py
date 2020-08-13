"""
Some instructions on writing CLI tests:
1. Look at test_ray_start for a simple output test example.
2. To get a valid regex, start with copy-pasting your output from a captured
   version (no formatting). Then escape ALL regex characters (parenthesis,
   brackets, dots, etc.). THEN add ".+" to all the places where info might
   change run to run.
3. Look at test_ray_up for an example of how to mock AWS, commands,
   and autoscaler config.

WARNING: IF YOU MOCK AWS, DON'T FORGET THE AWS_CREDENTIALS FIXTURE.
         THIS IS REQUIRED SO BOTO3 DOES NOT ACCESS THE ACTUAL AWS SERVERS.
"""

import sys
import re
import yaml
import os

import pytest

import moto
from moto import mock_ec2, mock_iam
from click.testing import CliRunner

from testfixtures import Replacer
from testfixtures.popen import MockPopen, PopenBehaviour

import ray.autoscaler.aws.config as aws_config
import ray.scripts.scripts as scripts

def _debug_check_line_by_line(result, expected_lines):
    output_lines = result.output.split("\n")
    for exp, out in zip(expected_lines, output_lines):
        matched = re.fullmatch(exp + r" *", out) is not None
        print(out)
        if not matched:
            print("!!!!!!! Expected (regex):")
            print(repr(exp))
    assert False

def test_ray_start():
    runner = CliRunner()
    result = runner.invoke(
        scripts.start,
        ["--head", "--log-new-style"])
    assert runner.invoke(scripts.stop).exit_code == 0

    expected_lines = [
        r"Local node IP: .+",
        r"Available RAM",
        r"  Workers: .+ GiB",
        r"  Objects: .+ GiB",
        r"",
        r"  To adjust these values, use",
        r"    ray\.init\(memory=<bytes>, object_store_memory=<bytes>\)",
        r"Dashboard URL: .+",
        r"",
        r"--------------------",
        r"Ray runtime started.",
        r"--------------------",
        r"",
        r"Next steps",
        r"  To connect to this Ray runtime from another node, run",
        r"    ray start --address='.+' --redis-password='.+'",
        r"",
        r"  Alternatively, use the following Python code:",
        r"    import ray",
        r"    ray\.init\(address='auto', redis_password='.+'\)",
        r"",
        r"  If connection fails, check your firewall settings other " +\
          r"network configuration.",
        r"",
        r"  To terminate the Ray runtime, run",
        r"    ray stop"
    ]

    expected = r" *\n".join(expected_lines) + "\n?"
    if re.fullmatch(expected, result.output) is None:
        _debug_check_line_by_line(result, expected_lines)
    assert result.exit_code == 0

    if result.exception is not None:
        raise result.exception

@mock_ec2
@mock_iam
def test_ray_up(aws_credentials, tmp_path):
    #
    # Get a config file
    #

    config = {
        "cluster_name": "test-cli",

        "min_workers": 1,
        "max_workers": 2,
        "initial_workers": 1,

        "target_utilization_fraction": 0.9,

        "idle_timeout_minutes": 5,

        "provider": {
            "type": "aws",
            "region": "us-west-2",
            "availability_zone": "us-west-2a",
            "key_pair": {
                "key_name": "test-cli"
            }
        },
        "auth": {
            "ssh_user": "ubuntu",
        },
        "head_node": {
            "InstanceType": "t3a.small",
            "ImageId": "latest_dlami"
        },
        "worker_nodes": {
            "InstanceType": "t3a.small",
            "ImageId": "latest_dlami"
        },
        "file_mounts": {
            "~/tests": "."
        },

        "initialization_commands": ["echo init"],
        "setup_commands": [
            "echo a",
            "echo b",
            "echo ${echo hi}"
        ],
        "head_setup_commands": ["echo head"],
        "worker_setup_commands": ["echo worker"],
        "head_start_ray_commands": [
            "ray stop",
            "ray start --head --autoscaling-config=~/ray_bootstrap_config.yaml"
        ],
        "worker_start_ray_commands": [
            "ray stop",
            "ray start --address=$RAY_HEAD_IP"
        ]
    }

    config_path = tmp_path / "config.yaml"
    config_path.write_text(yaml.dump(config))

    #
    # configure the AWS mock
    #

    # moto (boto3 mock) only allows a hardcoded set of AMIs
    dlami = moto.ec2.ec2_backends["us-west-2"].describe_images(
                        filters={"name": "Deep Learning AMI Ubuntu*"})[0].id
    aws_config.DEFAULT_AMI["us-west-2"] = dlami

    # moto.settings.INITIAL_NO_AUTH_ACTION_COUNT = 0

    #
    # Mock Popen
    #

    def commands_mock(command, stdin):
        return PopenBehaviour(stdout="MOCKED")

    Popen = MockPopen()
    Popen.set_default(stdout="MOCKED", behaviour=commands_mock)
    with Replacer() as replacer:
        replacer.replace("subprocess.Popen", Popen)

        #
        # Run the test
        #

        expected_lines = [
            r"Commands running under a login shell can produce more " +\
                r"output than special processing can handle\.",
            r"Thus, the output from subcommands will be logged as is\.",
            r"Consider using --use-normal-shells, if you tested your " +\
                r"workflow and it is compatible\.",
            r"",
            r"Cluster configuration valid",
            r"",
            r"Cluster: test-cli",
            r"",
            r"Bootstraping AWS config",
            r"AWS config",
            r"  IAM Profile: .+ \[default\]",
            r"  EC2 Key pair \(head & workers\): .+ \[default\]",
            r"  VPC Subnets \(head & workers\): subnet-.+ \[default\]",
            r"  EC2 Security groups \(head & workers\): sg-.+ \[default\]",
            r"  EC2 AMI \(head & workers\): ami-.+ \[dlami\]",
            r"",
            r"No head node found\. Launching a new cluster\. " +\
                r"Confirm \[y/N\]: y \[automatic, due to --yes\]",
            r"",
            r"Acquiring an up-to-date head node",
            r"  Launched 1 nodes \[subnet_id=subnet-.+\]",
            r"    Launched instance i-.+ \[state=pending, info=pending\]",
            r"Done listing",
            r"  Launched a new head node",
            r"  Fetching the new head node",
            r"",
            r"<1/1> Setting up head node",
            r"  Prepared bootstrap config",
            r"  New status: waiting-for-ssh",
            r"  \[1/6\] Waiting for SSH to become available",
            r"    Running `uptime` as a test\.",
            r"    Fetched IP: .+",
            r"    Success\.",
            r"  Updating cluster configuration\. \[hash=.+\]",
            r"  New status: syncing-files",
            r"  \[3/6\] Processing file mounts",
            r"    ~/tests/ from ./",
            r"  \[4/6\] No worker file mounts to sync",
            r"  New status: setting-up",
            r"  \[3/5\] Running initialization commands",
            r"  \[4/6\] Running setup commands",
            r"    \(0/4\) echo a",
            r"    \(1/4\) echo b",
            r"    \(2/4\) echo \${echo hi}",
            r"    \(3/4\) echo head",
            r"  \[6/6\] Starting the Ray runtime",
            r"  New status: up-to-date",
            r"",
            r"Useful commands",
            r"  Monitor autoscaling with",
            r"    ray exec .+\.yaml " +\
                r"'tail -n 100 -f /tmp/ray/session_\*/logs/monitor\*'",
            r"  Connect to a terminal on the cluster head",
            r"    ray attach .+\.yaml"
        ]

        # config cache does not work with mocks
        runner = CliRunner()
        result = runner.invoke(
            scripts.up,
            [str(config_path), "--no-config-cache", "-y", "--log-new-style"])

        if result.exception is not None:
            raise result.exception from None

        expected = r" *\n".join(expected_lines) + "\n?"
        if re.fullmatch(expected, result.output) is None:
            _debug_check_line_by_line(result, expected_lines)

        assert result.exit_code == 0


@pytest.fixture(scope="function")
def aws_credentials():
    """Mocked AWS Credentials for moto."""
    os.environ["AWS_ACCESS_KEY_ID"] = "testing"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "testing"
    os.environ["AWS_SECURITY_TOKEN"] = "testing"
    os.environ["AWS_SESSION_TOKEN"] = "testing"

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
