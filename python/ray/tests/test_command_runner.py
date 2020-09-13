import pytest
from ray.tests.test_autoscaler import MockProvider, MockProcessRunner
from ray.autoscaler.command_runner import SSHCommandRunner, \
    _with_environment_variables, DockerCommandRunner, KubernetesCommandRunner
from ray.autoscaler.docker import DOCKER_MOUNT_PREFIX
from getpass import getuser
import hashlib

auth_config = {
    "ssh_user": "ray",
    "ssh_private_key": "8265.pem",
}


def test_environment_variable_encoder_strings():
    env_vars = {"var1": "quote between this \" and this", "var2": "123"}
    res = _with_environment_variables("echo hello", env_vars)
    expected = """export var1='"quote between this \\" and this"';export var2='"123"';echo hello"""  # noqa: E501
    assert res == expected


def test_environment_variable_encoder_dict():
    env_vars = {"value1": "string1", "value2": {"a": "b", "c": 2}}
    res = _with_environment_variables("echo hello", env_vars)

    expected = """export value1='"string1"';export value2='{"a":"b","c":2}';echo hello"""  # noqa: E501
    assert res == expected


def test_ssh_command_runner():
    process_runner = MockProcessRunner()
    provider = MockProvider()
    provider.create_node({}, {}, 1)
    cluster_name = "cluster"
    ssh_control_hash = hashlib.md5(cluster_name.encode()).hexdigest()
    ssh_user_hash = hashlib.md5(getuser().encode()).hexdigest()
    ssh_control_path = "/tmp/ray_ssh_{}/{}".format(ssh_user_hash[:10],
                                                   ssh_control_hash[:10])
    args = {
        "log_prefix": "prefix",
        "node_id": 0,
        "provider": provider,
        "auth_config": auth_config,
        "cluster_name": cluster_name,
        "process_runner": process_runner,
        "use_internal_ip": False,
    }
    cmd_runner = SSHCommandRunner(**args)

    env_vars = {"var1": "quote between this \" and this", "var2": "123"}
    cmd_runner.run(
        "echo helloo",
        port_forward=[(8265, 8265)],
        environment_variables=env_vars)

    expected = [
        "ssh",
        "-tt",
        "-L",
        "8265:localhost:8265",
        "-i",
        "8265.pem",
        "-o",
        "StrictHostKeyChecking=no",
        "-o",
        "UserKnownHostsFile=/dev/null",
        "-o",
        "IdentitiesOnly=yes",
        "-o",
        "ExitOnForwardFailure=yes",
        "-o",
        "ServerAliveInterval=5",
        "-o",
        "ServerAliveCountMax=3",
        "-o",
        "ControlMaster=auto",
        "-o",
        "ControlPath={}/%C".format(ssh_control_path),
        "-o",
        "ControlPersist=10s",
        "-o",
        "ConnectTimeout=120s",
        "ray@1.2.3.4",
        "bash",
        "--login",
        "-c",
        "-i",
        """'true && source ~/.bashrc && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && (export var1='"'"'"quote between this \\" and this"'"'"';export var2='"'"'"123"'"'"';echo helloo)'"""  # noqa: E501
    ]

    # Much easier to debug this loop than the function call.
    for x, y in zip(process_runner.calls[0], expected):
        assert x == y
    process_runner.assert_has_call("1.2.3.4", exact=expected)


def test_kubernetes_command_runner():
    process_runner = MockProcessRunner()
    provider = MockProvider()
    provider.create_node({}, {}, 1)
    args = {
        "log_prefix": "prefix",
        "namespace": "namespace",
        "node_id": 0,
        "auth_config": auth_config,
        "process_runner": process_runner,
    }
    cmd_runner = KubernetesCommandRunner(**args)

    env_vars = {"var1": "quote between this \" and this", "var2": "123"}
    cmd_runner.run("echo helloo", environment_variables=env_vars)

    expected = [
        "kubectl",
        "-n",
        "namespace",
        "exec",
        "-it",
        "0",
        "--",
        "bash",
        "--login",
        "-c",
        "-i",
        """\'true && source ~/.bashrc && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && (export var1=\'"\'"\'"quote between this \\" and this"\'"\'"\';export var2=\'"\'"\'"123"\'"\'"\';echo helloo)\'"""  # noqa: E501
    ]

    assert process_runner.calls[0] == " ".join(expected)


def test_docker_command_runner():
    process_runner = MockProcessRunner()
    provider = MockProvider()
    provider.create_node({}, {}, 1)
    cluster_name = "cluster"
    ssh_control_hash = hashlib.md5(cluster_name.encode()).hexdigest()
    ssh_user_hash = hashlib.md5(getuser().encode()).hexdigest()
    ssh_control_path = "/tmp/ray_ssh_{}/{}".format(ssh_user_hash[:10],
                                                   ssh_control_hash[:10])
    docker_config = {"container_name": "container"}
    args = {
        "log_prefix": "prefix",
        "node_id": 0,
        "provider": provider,
        "auth_config": auth_config,
        "cluster_name": cluster_name,
        "process_runner": process_runner,
        "use_internal_ip": False,
        "docker_config": docker_config,
    }
    cmd_runner = DockerCommandRunner(**args)
    assert len(process_runner.calls) == 0, "No calls should be made in ctor"

    env_vars = {"var1": "quote between this \" and this", "var2": "123"}
    cmd_runner.run("echo hello", environment_variables=env_vars)

    # This string is insane because there are an absurd number of embedded
    # quotes. While this is a ridiculous string, the escape behavior is
    # important and somewhat difficult to get right for environment variables.
    cmd = """'true && source ~/.bashrc && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && (docker exec -it  container /bin/bash -c '"'"'bash --login -c -i '"'"'"'"'"'"'"'"'true && source ~/.bashrc && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && (export var1='"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"quote between this \\" and this"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"';export var2='"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"123"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"';echo hello)'"'"'"'"'"'"'"'"''"'"' )'"""  # noqa: E501

    expected = [
        "ssh", "-tt", "-i", "8265.pem", "-o", "StrictHostKeyChecking=no", "-o",
        "UserKnownHostsFile=/dev/null", "-o", "IdentitiesOnly=yes", "-o",
        "ExitOnForwardFailure=yes", "-o", "ServerAliveInterval=5", "-o",
        "ServerAliveCountMax=3", "-o", "ControlMaster=auto", "-o",
        "ControlPath={}/%C".format(ssh_control_path), "-o",
        "ControlPersist=10s", "-o", "ConnectTimeout=120s", "ray@1.2.3.4",
        "bash", "--login", "-c", "-i", cmd
    ]
    # Much easier to debug this loop than the function call.
    for x, y in zip(process_runner.calls[0], expected):
        print(f"expeted:\t{y}")
        print(f"actual: \t{x}")
        assert x == y
    process_runner.assert_has_call("1.2.3.4", exact=expected)


def test_docker_rsync():
    process_runner = MockProcessRunner()
    provider = MockProvider()
    provider.create_node({}, {}, 1)
    cluster_name = "cluster"
    docker_config = {"container_name": "container"}
    args = {
        "log_prefix": "prefix",
        "node_id": 0,
        "provider": provider,
        "auth_config": auth_config,
        "cluster_name": cluster_name,
        "process_runner": process_runner,
        "use_internal_ip": False,
        "docker_config": docker_config,
    }
    cmd_runner = DockerCommandRunner(**args)

    local_mount = "/home/ubuntu/base/mount/"
    remote_mount = "/root/protected_mount/"
    remote_host_mount = f"{DOCKER_MOUNT_PREFIX}{remote_mount}"

    local_file = "/home/ubuntu/base-file"
    remote_file = "/root/protected-file"
    remote_host_file = f"{DOCKER_MOUNT_PREFIX}{remote_file}"

    process_runner.respond_to_call("docker inspect -f", ["true"])
    cmd_runner.run_rsync_up(
        local_mount, remote_mount, options={"file_mount": True})

    # Make sure we do not copy directly to raw destination
    process_runner.assert_not_has_call(
        "1.2.3.4", pattern=f"-avz {local_mount} ray@1.2.3.4:{remote_mount}")
    process_runner.assert_not_has_call(
        "1.2.3.4", pattern=f"mkdir -p {remote_mount}")
    # No docker cp for file_mounts
    process_runner.assert_not_has_call("1.2.3.4", pattern=f"docker cp")
    process_runner.assert_has_call(
        "1.2.3.4",
        pattern=f"-avz {local_mount} ray@1.2.3.4:{remote_host_mount}")
    process_runner.clear_history()
    ##############################

    process_runner.respond_to_call("docker inspect -f", ["true"])
    cmd_runner.run_rsync_up(
        local_file, remote_file, options={"file_mount": False})

    # Make sure we do not copy directly to raw destination
    process_runner.assert_not_has_call(
        "1.2.3.4", pattern=f"-avz {local_file} ray@1.2.3.4:{remote_file}")
    process_runner.assert_not_has_call(
        "1.2.3.4", pattern=f"mkdir -p {remote_file}")

    process_runner.assert_has_call("1.2.3.4", pattern=f"docker cp")
    process_runner.assert_has_call(
        "1.2.3.4", pattern=f"-avz {local_file} ray@1.2.3.4:{remote_host_file}")
    process_runner.clear_history()
    ##############################

    cmd_runner.run_rsync_down(
        remote_mount, local_mount, options={"file_mount": True})

    process_runner.assert_not_has_call("1.2.3.4", pattern=f"docker cp")
    process_runner.assert_not_has_call(
        "1.2.3.4", pattern=f"-avz ray@1.2.3.4:{remote_mount} {local_mount}")
    process_runner.assert_has_call(
        "1.2.3.4",
        pattern=f"-avz ray@1.2.3.4:{remote_host_mount} {local_mount}")

    process_runner.clear_history()
    ##############################

    cmd_runner.run_rsync_down(
        remote_file, local_file, options={"file_mount": False})

    process_runner.assert_has_call("1.2.3.4", pattern=f"docker cp")
    process_runner.assert_not_has_call(
        "1.2.3.4", pattern=f"-avz ray@1.2.3.4:{remote_file} {local_file}")
    process_runner.assert_has_call(
        "1.2.3.4", pattern=f"-avz ray@1.2.3.4:{remote_host_file} {local_file}")


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
