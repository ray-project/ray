import os
import pytest
import ray


def test_conn_cluster_with_tempdir():
    try:
        ray.init(
            redis_address="127.0.0.1:6379", temp_dir="/tmp/this_should_fail")
    except Exception:
        pass
    else:
        pytest.fail("This test should raise an exception.")


def test_conn_cluster_with_plasma_store_socket():
    try:
        ray.init(
            redis_address="127.0.0.1:6379", temp_dir="/tmp/this_should_fail")
    except Exception:
        pass
    else:
        pytest.fail("This test should raise an exception.")


def test_tempdir():
    ray.init(temp_dir="/tmp/i_am_a_temp_dir")
    assert os.path.exists(
        "/tmp/i_am_a_temp_dir"), "Specified temp dir not found."
    ray.shutdown()
    try:
        os.rmdir("/tmp/i_am_a_temp_dir")
    except Exception:
        pass


def test_temp_plasma_store_socket():
    ray.init(plasma_store_socket_name="/tmp/i_am_a_temp_socket")
    assert os.path.exists(
        "/tmp/i_am_a_temp_socket"), "Specified temp dir not found."
    ray.shutdown()
    try:
        os.remove("/tmp/i_am_a_temp_socket")
    except Exception:
        pass
