import os
import random
import sys
import socket

import pytest

import ray

# LocalHost
host = "127.0.0.1"


# Function to check if the port is occupied.
def detect_listened_port(port: int):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(1)
        s.connect((host, port))
        return True
    except Exception:
        return False
    finally:
        s.close()


# If RAY_GCS_SERVER_PORT isn't set, gcs_server_port will be picked by
# random.randint(port, 65535) when ray.init() is called.
def test_as_gcs_server_port_is_not_set():
    try:
        ray.init()
    except Exception:
        assert False
    finally:
        ray.shutdown()


# If RAY_GCS_SERVER_PORT is set, gcs_server_port will be specified.
# Connection can be established.
def test_as_gcs_server_port_is_set():
    port_int = random.randint(6000, 10000)
    port_str = str(port_int)
    os.environ["RAY_GCS_SERVER_PORT"] = port_str
    ray.init()
    assert detect_listened_port(port_int)
    ray.shutdown()


# If RAY_GCS_SERVER_PORT is set as string, it will be ignored so that
# ray.init() will be executed normally.
def test_as_gcs_server_port_is_set_incorrectly():
    os.environ["RAY_GCS_SERVER_PORT"] = "non-numeric"
    try:
        ray.init()
    except Exception:
        assert False
    finally:
        ray.shutdown()


if __name__ == "__main__":

    # Make subprocess happy in bazel.
    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
