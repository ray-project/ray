import pytest
from ray.tests.test_autoscaler import MockProvider, MockProcessRunner
from ray.autoscaler.command_runner import SSHCommandRunner, \
    _with_environment_variables, DockerCommandRunner
from getpass import getuser
import hashlib

auth_config = {
    "ssh_user": "ray",
    "ssh_private_key": "8265.pem",
}


def test_environment_variable_encoder_strings():
    env_vars = {"var1": "quote between this \" and this", "var2": "123"}
    res = _with_environment_variables("echo hello", env_vars)
    expected = """export var1='quote between this " and this';export var2=123;echo hello"""  # noqa: E501
    assert res == expected


def test_environment_variable_encoder_dict():
    env_vars = {"value1": "string1", "value2": {"a": "b", "c": 2}}
    res = _with_environment_variables("echo hello", env_vars)

    expected = """export value1=string1;export value2='{a: b,c: 2}';echo hello"""  # noqa: E501
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
        """'true && source ~/.bashrc && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && export var1='"'"'quote between this " and this'"'"';export var2=123;echo helloo'"""  # noqa: E501
    ]

    # Much easier to debug this loop than the function call.
    for x, y in zip(process_runner.calls[0], expected):
        assert x == y
    process_runner.assert_has_call("1.2.3.4", exact=expected)


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
    process_runner.assert_has_call("1.2.3.4", "command -v docker")
    process_runner.clear_history()

    env_vars = {"var1": "quote between this \" and this", "var2": "123"}
    cmd_runner.run("echo hello", environment_variables=env_vars)

    # This string is insane because there are an absurd number of embedded
    # quotes. While this is a ridiculous string, the escape behavior is
    # important and somewhat difficult to get right for environment variables.
    cmd = """'true && source ~/.bashrc && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && docker exec -it  container /bin/bash -c '"'"'bash --login -c -i '"'"'"'"'"'"'"'"'true && source ~/.bashrc && export OMP_NUM_THREADS=1 PYTHONWARNINGS=ignore && export var1='"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'quote between this " and this'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"'"';export var2=123;echo hello'"'"'"'"'"'"'"'"''"'"' '"""  # noqa: E501

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
        assert x == y
    process_runner.assert_has_call("1.2.3.4", exact=expected)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
