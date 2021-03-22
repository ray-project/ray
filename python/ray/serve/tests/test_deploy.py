import os

import pytest
import requests

import ray


@pytest.mark.parametrize("use_handle", [True, False])
def test_deploy(serve_instance, use_handle):
    client = serve_instance

    name = "test"

    def call():
        if use_handle:
            ret = ray.get(client.get_handle(name).remote())
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    def v1(*args):
        return f"1|{os.getpid()}"

    def v2(*args):
        return f"2|{os.getpid()}"

    client.deploy(name, v1, version="1")
    val1, pid1 = call()
    assert val1 == "1"

    # Redeploying with the same version and code should do nothing.
    client.deploy(name, v1, version="1")
    val2, pid2 = call()
    assert val2 == "1"
    assert pid2 == pid1

    # Redeploying with a new version should start a new actor.
    client.deploy(name, v1, version="2")
    val3, pid3 = call()
    assert val3 == "1"
    assert pid3 != pid2

    # Redeploying with the same version and new code should do nothing.
    client.deploy(name, v2, version="2")
    val4, pid4 = call()
    assert val4 == "1"
    assert pid4 == pid3

    # Redeploying with new code and a new version should start a new actor
    # running the new code.
    client.deploy(name, v2, version="3")
    val5, pid5 = call()
    assert val5 == "2"
    assert pid5 != pid4


@pytest.mark.parametrize("use_handle", [True, False])
def test_config_change(serve_instance, use_handle):
    client = serve_instance

    name = "test"

    def call():
        if use_handle:
            ret = ray.get(client.get_handle(name).remote())
        else:
            ret = requests.get(f"http://localhost:8000/{name}").text

        return ret.split("|")[0], ret.split("|")[1]

    class Backend:
        def __init__(self):
            self.ret = "1"

        def reconfigure(self, d):
            self.ret = d["ret"]

        def __call__(self, *args):
            return f"{self.ret}|{os.getpid()}"

    # First deploy with no user config set.
    client.deploy(name, Backend, version="1")
    val1, pid1 = call()
    assert val1 == "1"

    # Now update the user config without changing versions. Actor should stay
    # alive but return value should change.
    client.deploy(
        name, Backend, version="1", config={"user_config": {
            "ret": "2"
        }})
    val2, pid2 = call()
    assert pid2 == pid1
    assert val2 == "2"

    # Update the user config without changing the version again.
    client.deploy(
        name, Backend, version="1", config={"user_config": {
            "ret": "3"
        }})
    val3, pid3 = call()
    assert pid3 == pid2
    assert val3 == "3"

    # Update the version without changing the user config.
    client.deploy(
        name, Backend, version="2", config={"user_config": {
            "ret": "3"
        }})
    val4, pid4 = call()
    assert pid4 != pid3
    assert val4 == "3"

    # Update the version and the user config.
    client.deploy(
        name, Backend, version="3", config={"user_config": {
            "ret": "4"
        }})
    val5, pid5 = call()
    assert pid5 != pid4
    assert val5 == "4"


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
