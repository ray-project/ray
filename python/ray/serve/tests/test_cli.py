import json
import os
from pathlib import Path
import subprocess
import sys
import signal
import pytest
import requests

import ray
from ray import serve
from ray.tests.conftest import tmp_working_dir  # noqa: F401, E501
from ray._private.test_utils import wait_for_condition
from ray.dashboard.optional_utils import RAY_INTERNAL_DASHBOARD_NAMESPACE
from ray.serve.scripts import process_args_and_kwargs


def ping_endpoint(endpoint: str, params: str = ""):
    try:
        return requests.get(f"http://localhost:8000/{endpoint}{params}").text
    except requests.exceptions.ConnectionError:
        return "connection error"


@pytest.fixture
def ray_start_stop():
    subprocess.check_output(["ray", "start", "--head"])
    yield
    subprocess.check_output(["ray", "stop", "--force"])


class TestProcessArgsAndKwargs:
    def test_valid_args_and_kwargs(self):
        args_and_kwargs = (
            "argval1",
            "argval2",
            "--kwarg1",
            "kwval1",
            "--kwarg2",
            "kwval2",
        )
        args, kwargs = process_args_and_kwargs(args_and_kwargs)
        assert args == ["argval1", "argval2"]
        assert kwargs == {"kwarg1": "kwval1", "kwarg2": "kwval2"}

    def test_mixed_args_and_kwargs(self):
        args_and_kwargs = (
            "argval1",
            "--kwarg1",
            "kwval1",
            "argval2",
            "--kwarg2",
            "kwval2",
        )
        with pytest.raises(ValueError):
            process_args_and_kwargs(args_and_kwargs)

    def test_mixed_kwargs(self):
        args_and_kwargs = (
            "argval1",
            "argval2",
            "--kwarg1==kw==val1",
            "--kwarg2",
            "kwval2",
            "--kwarg3",
            "=kwval=3",
            "--kwarg4=",
            "--kwarg5",
            "kwval5",
        )
        args, kwargs = process_args_and_kwargs(args_and_kwargs)
        assert args == ["argval1", "argval2"]
        assert kwargs == {
            "kwarg1": "=kw==val1",
            "kwarg2": "kwval2",
            "kwarg3": "=kwval=3",
            "kwarg4": "",
            "kwarg5": "kwval5",
        }

    def test_empty_kwarg(self):
        args_and_kwargs = (
            "argval1",
            "--kwarg1",
            "--kwarg2",
            "kwval2",
        )
        with pytest.raises(ValueError):
            process_args_and_kwargs(args_and_kwargs)

        args_and_kwargs = ("--empty_kwarg_only",)
        with pytest.raises(ValueError):
            process_args_and_kwargs(args_and_kwargs)

    def test_empty_equals_kwarg(self):
        args_and_kwargs = (
            "argval1",
            "--kwarg1=--hello",
            "--kwarg2=",
        )
        args, kwargs = process_args_and_kwargs(args_and_kwargs)
        assert args == ["argval1"]
        assert kwargs == {
            "kwarg1": "--hello",
            "kwarg2": "",
        }

        args_and_kwargs = ("--empty_kwarg_only=",)
        args, kwargs = process_args_and_kwargs(args_and_kwargs)
        assert args == []
        assert kwargs == {"empty_kwarg_only": ""}

    def test_only_args(self):
        args_and_kwargs = ("argval1", "argval2", "argval3")
        args, kwargs = process_args_and_kwargs(args_and_kwargs)
        assert args == ["argval1", "argval2", "argval3"]
        assert kwargs == {}

        args_and_kwargs = ("single_arg",)
        args, kwargs = process_args_and_kwargs(args_and_kwargs)
        assert args == ["single_arg"]
        assert kwargs == {}

    def test_only_kwargs(self):
        args_and_kwargs = (
            "--kwarg1",
            "kwval1",
            "--kwarg2",
            "kwval2",
            "--kwarg3",
            "kwval3",
        )
        args, kwargs = process_args_and_kwargs(args_and_kwargs)
        assert args == []
        assert kwargs == {"kwarg1": "kwval1", "kwarg2": "kwval2", "kwarg3": "kwval3"}

        args_and_kwargs = (
            "--single_kwarg",
            "single_kwval",
        )
        args, kwargs = process_args_and_kwargs(args_and_kwargs)
        assert args == []
        assert kwargs == {"single_kwarg": "single_kwval"}

    def test_empty_args_and_kwargs(self):
        for empty_val in [None, ()]:
            args, kwargs = process_args_and_kwargs(empty_val)
            assert args == []
            assert kwargs == {}


def test_start_shutdown(ray_start_stop):
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_output(["serve", "shutdown"])

    subprocess.check_output(["serve", "start"])
    subprocess.check_output(["serve", "shutdown"])


def test_start_shutdown_in_namespace(ray_start_stop):
    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_output(["serve", "-n", "test", "shutdown"])

    subprocess.check_output(["serve", "-n", "test", "start"])
    subprocess.check_output(["serve", "-n", "test", "shutdown"])


class A:
    def __init__(self, value, increment=1):
        self.value = value
        self.increment = increment
        self.decrement = 0
        self.multiplier = int(os.environ["SERVE_TEST_MULTIPLIER"])

        p = Path("hello")
        assert p.exists()
        with open(p) as f:
            assert f.read() == "world"

    def reconfigure(self, config):
        self.decrement = config["decrement"]

    def __call__(self, inp):
        return (self.value + self.increment - self.decrement) * self.multiplier


@serve.deployment
class DecoratedA(A):
    pass


@pytest.mark.parametrize("class_name", ["A", "DecoratedA"])
def test_create_deployment(ray_start_stop, tmp_working_dir, class_name):  # noqa: F811
    subprocess.check_output(["serve", "start"])
    subprocess.check_output(
        [
            "serve",
            "--runtime-env-json",
            json.dumps(
                {
                    "working_dir": tmp_working_dir,
                }
            ),
            "create-deployment",
            f"ray.serve.tests.test_cli.{class_name}",
            "--options-json",
            json.dumps(
                {
                    "name": "B",
                    "init_args": [42],
                    "init_kwargs": {"increment": 10},
                    "num_replicas": 2,
                    "user_config": {"decrement": 5},
                    "ray_actor_options": {
                        "runtime_env": {
                            "env_vars": {
                                "SERVE_TEST_MULTIPLIER": "2",
                            },
                        }
                    },
                }
            ),
        ]
    )
    resp = requests.get("http://127.0.0.1:8000/B")
    resp.raise_for_status()
    assert resp.text == "94", resp.text


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_deploy(ray_start_stop):
    # Deploys some valid config files and checks that the deployments work

    # Initialize serve in test to enable calling serve.list_deployments()
    ray.init(address="auto", namespace=RAY_INTERNAL_DASHBOARD_NAMESPACE)
    serve.start(detached=True)

    # Create absolute file names to YAML config files
    three_deployments = os.path.join(
        os.path.dirname(__file__), "test_config_files", "three_deployments.yaml"
    )
    two_deployments = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_deployments.yaml"
    )
    deny_deployment = os.path.join(
        os.path.dirname(__file__), "test_config_files", "deny_access.yaml"
    )

    # Dictionary mapping test config file names to expected deployment names
    # and configurations. These should match the values specified in the YAML
    # files.
    configs = {
        three_deployments: {
            "shallow": {
                "num_replicas": 1,
                "response": "Hello shallow world!",
            },
            "deep": {
                "num_replicas": 1,
                "response": "Hello deep world!",
            },
            "one": {
                "num_replicas": 2,
                "response": "2",
            },
        },
        two_deployments: {
            "shallow": {
                "num_replicas": 3,
                "response": "Hello shallow world!",
            },
            "one": {
                "num_replicas": 2,
                "response": "2",
            },
        },
    }

    request_url = "http://localhost:8000/"
    success_message_fragment = b"Sent deploy request successfully!"

    # Check idempotence:
    for _ in range(2):
        for config_file_name, expected_deployments in configs.items():
            deploy_response = subprocess.check_output(
                ["serve", "deploy", config_file_name]
            )
            assert success_message_fragment in deploy_response

            for name, deployment_config in expected_deployments.items():
                wait_for_condition(
                    lambda: (
                        requests.get(f"{request_url}{name}").text
                        == deployment_config["response"]
                    ),
                    timeout=15,
                )

            running_deployments = serve.list_deployments()

            # Check that running deployment names match expected deployment names
            assert set(running_deployments.keys()) == expected_deployments.keys()

            for name, deployment in running_deployments.items():
                assert (
                    deployment.num_replicas
                    == expected_deployments[name]["num_replicas"]
                )

        # Deploy a deployment without HTTP access
        deploy_response = subprocess.check_output(["serve", "deploy", deny_deployment])
        assert success_message_fragment in deploy_response

        wait_for_condition(
            lambda: requests.get(f"{request_url}shallow").status_code == 404, timeout=15
        )
        assert (
            ray.get(serve.get_deployment("shallow").get_handle().remote())
            == "Hello shallow world!"
        )

    ray.shutdown()


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_info(ray_start_stop):
    # Deploys valid config file and checks that serve info returns correct
    # response

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_deployments.yaml"
    )
    success_message_fragment = b"Sent deploy request successfully!"
    deploy_response = subprocess.check_output(["serve", "deploy", config_file_name])
    assert success_message_fragment in deploy_response

    info_response = subprocess.check_output(["serve", "info"]).decode("utf-8")
    info = json.loads(info_response)

    assert "deployments" in info
    assert len(info["deployments"]) == 2

    # Validate non-default information about shallow deployment
    shallow_info = None
    for deployment_info in info["deployments"]:
        if deployment_info["name"] == "shallow":
            shallow_info = deployment_info

    assert shallow_info is not None
    assert shallow_info["import_path"] == "test_env.shallow_import.ShallowClass"
    assert shallow_info["num_replicas"] == 3
    assert shallow_info["route_prefix"] == "/shallow"
    assert (
        "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
        in shallow_info["ray_actor_options"]["runtime_env"]["py_modules"]
    )
    assert (
        "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
        in shallow_info["ray_actor_options"]["runtime_env"]["py_modules"]
    )

    # Validate non-default information about one deployment
    one_info = None
    for deployment_info in info["deployments"]:
        if deployment_info["name"] == "one":
            one_info = deployment_info

    assert one_info is not None
    assert one_info["import_path"] == "test_module.test.one"
    assert one_info["num_replicas"] == 2
    assert one_info["route_prefix"] == "/one"
    assert (
        "https://github.com/shrekris-anyscale/test_deploy_group/archive/HEAD.zip"
        in one_info["ray_actor_options"]["runtime_env"]["py_modules"]
    )
    assert (
        "https://github.com/shrekris-anyscale/test_module/archive/HEAD.zip"
        in one_info["ray_actor_options"]["runtime_env"]["py_modules"]
    )


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_status(ray_start_stop):
    # Deploys a config file and checks its status

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "three_deployments.yaml"
    )

    subprocess.check_output(["serve", "deploy", config_file_name])
    status_response = subprocess.check_output(["serve", "status"])
    statuses = json.loads(status_response)["statuses"]

    expected_deployments = {"shallow", "deep", "one"}
    for status in statuses:
        expected_deployments.remove(status["name"])
        assert status["status"] in {"HEALTHY", "UPDATING"}
        assert "message" in status
    assert len(expected_deployments) == 0


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_delete(ray_start_stop):
    # Deploys a config file and deletes it

    def get_num_deployments():
        info_response = subprocess.check_output(["serve", "info"])
        info = json.loads(info_response)
        return len(info["deployments"])

    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_deployments.yaml"
    )

    # Check idempotence
    for _ in range(2):
        subprocess.check_output(["serve", "deploy", config_file_name])
        wait_for_condition(lambda: get_num_deployments() == 2, timeout=35)

        subprocess.check_output(["serve", "delete", "-y"])
        wait_for_condition(lambda: get_num_deployments() == 0, timeout=35)


def parrot(request):
    return request.query_params["sound"]


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_basic(ray_start_stop):
    # Deploys valid config file and import path via serve run

    # Deploy via config file
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "two_deployments.yaml"
    )

    p = subprocess.Popen(["serve", "run", config_file_name])
    wait_for_condition(lambda: ping_endpoint("one") == "2", timeout=10)
    wait_for_condition(
        lambda: ping_endpoint("shallow") == "Hello shallow world!", timeout=10
    )

    p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
    p.wait()
    assert ping_endpoint("one") == "connection error"
    assert ping_endpoint("shallow") == "connection error"

    # Deploy via import path
    p = subprocess.Popen(["serve", "run", "ray.serve.tests.test_cli.parrot"])
    wait_for_condition(
        lambda: ping_endpoint("parrot", params="?sound=squawk") == "squawk", timeout=10
    )

    p.send_signal(signal.SIGINT)  # Equivalent to ctrl-C
    p.wait()
    assert ping_endpoint("parrot", params="?sound=squawk") == "connection error"


class Macaw:
    def __init__(self, color, name="Mulligan", surname=None):
        self.color = color
        self.name = name
        self.surname = surname

    def __call__(self):
        if self.surname is not None:
            return f"{self.name} {self.surname} is {self.color}!"
        else:
            return f"{self.name} is {self.color}!"


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_init_args_kwargs(ray_start_stop):
    # Tests serve run with specified args and kwargs

    # Deploy via import path
    p = subprocess.Popen(
        [
            "serve",
            "run",
            "ray.serve.tests.test_cli.Macaw",
            "--",
            "green",
            "--name",
            "Molly",
        ]
    )
    wait_for_condition(lambda: ping_endpoint("Macaw") == "Molly is green!", timeout=10)
    p.send_signal(signal.SIGINT)
    p.wait()
    assert ping_endpoint("Macaw") == "connection error"

    # Mix and match keyword notation
    p = subprocess.Popen(
        [
            "serve",
            "run",
            "ray.serve.tests.test_cli.Macaw",
            "--",
            "green",
            "--name",
            "Molly",
            "--surname==./u=6y",
        ]
    )
    wait_for_condition(
        lambda: ping_endpoint("Macaw") == "Molly =./u=6y is green!", timeout=10
    )
    p.send_signal(signal.SIGINT)
    p.wait()
    assert ping_endpoint("Macaw") == "connection error"

    # Args/kwargs with config file
    config_file_name = os.path.join(
        os.path.dirname(__file__), "test_config_files", "macaw.yaml"
    )

    with pytest.raises(subprocess.CalledProcessError):
        subprocess.check_output(
            ["serve", "run", config_file_name, "--", "green", "--name", "Molly"]
        )


@pytest.mark.skipif(sys.platform == "win32", reason="File path incorrect on Windows.")
def test_run_simultaneous(ray_start_stop):
    # Test that two serve run processes can run simultaneously

    p1 = subprocess.Popen(["serve", "run", "ray.serve.tests.test_cli.parrot"])
    wait_for_condition(
        lambda: ping_endpoint("parrot", params="?sound=squawk") == "squawk", timeout=10
    )

    p2 = subprocess.Popen(
        [
            "serve",
            "run",
            "ray.serve.tests.test_cli.Macaw",
            "--",
            "green",
            "--name=Molly",
            "--surname=Malarkey",
        ]
    )
    wait_for_condition(
        lambda: ping_endpoint("parrot", params="?sound=squawk") == "squawk", timeout=10
    )
    wait_for_condition(
        lambda: ping_endpoint("Macaw") == "Molly Malarkey is green!", timeout=10
    )

    # Macaw should still be available after parrot is torn down
    p1.send_signal(signal.SIGINT)
    p1.wait()
    assert "Path '/parrot' not found" in ping_endpoint("parrot")
    assert ping_endpoint("Macaw") == "Molly Malarkey is green!"

    # Serve should shut down after all deployments are torn down
    p2.send_signal(signal.SIGINT)
    p2.wait()
    assert ping_endpoint("parrot") == "connection error"
    assert ping_endpoint("Macaw") == "connection error"


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
