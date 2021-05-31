import os
import sys

import pytest
import redis

import ray
import ray._private.services
from ray.cluster_utils import Cluster


@pytest.fixture
def password():
    random_bytes = os.urandom(128)
    if hasattr(random_bytes, "hex"):
        return random_bytes.hex()  # Python 3
    return random_bytes.encode("hex")  # Python 2


class TestRedisPassword:
    def test_redis_password(self, password, shutdown_only):
        @ray.remote
        def f():
            return 1

        info = ray.init(_redis_password=password)
        address = info["redis_address"]
        redis_ip, redis_port = address.split(":")

        # Check that we can run a task
        object_ref = f.remote()
        ray.get(object_ref)

        # Check that Redis connections require a password
        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=None)
        with pytest.raises(redis.exceptions.AuthenticationError):
            redis_client.ping()
        # We want to simulate how this is called by ray.scripts.start().
        try:
            ray._private.services.wait_for_redis_to_start(
                redis_ip, redis_port, password="wrong password")
        # We catch a generic Exception here in case someone later changes the
        # type of the exception.
        except Exception as ex:
            if not (isinstance(ex.__cause__, redis.AuthenticationError)
                    and "invalid password" in str(ex.__cause__)) and not (
                        isinstance(ex, redis.ResponseError) and
                        "WRONGPASS invalid username-password pair" in str(ex)):
                raise
            # By contrast, we may be fairly confident the exact string
            # 'invalid password' won't go away, because redis-py simply wraps
            # the exact error from the Redis library.
            # https://github.com/andymccurdy/redis-py/blob/master/
            # redis/connection.py#L132
            # Except, apparently sometimes redis-py raises a completely
            # different *type* of error for a bad password,
            # redis.ResponseError, which is not even derived from
            # redis.ConnectionError as redis.AuthenticationError is.

        # Check that we can connect to Redis using the provided password
        redis_client = redis.StrictRedis(
            host=redis_ip, port=redis_port, password=password)
        assert redis_client.ping()

    def test_redis_password_cluster(self, password, shutdown_only):
        @ray.remote
        def f():
            return 1

        node_args = {"redis_password": password}
        cluster = Cluster(
            initialize_head=True, connect=True, head_node_args=node_args)
        cluster.add_node(**node_args)

        object_ref = f.remote()
        ray.get(object_ref)


def test_shutdown_and_reset_global_worker(shutdown_only):
    ray.init(job_config=ray.job_config.JobConfig(code_search_path=["a"]))
    ray.shutdown()
    ray.init()

    @ray.remote
    class A:
        def f(self):
            return 100

    a = A.remote()
    ray.get(a.f.remote())


def test_ports_assignment(ray_start_cluster):
    # Make sure value error is raised when there are the same ports.

    cluster = ray_start_cluster
    with pytest.raises(ValueError):
        cluster.add_node(dashboard_port=30000, metrics_export_port=30000)

    pre_selected_ports = {
        "redis_port": 30000,
        "object_manager_port": 30001,
        "node_manager_port": 30002,
        "gcs_server_port": 30003,
        "ray_client_server_port": 30004,
        "dashboard_port": 30005,
        "metrics_agent_port": 30006,
        "metrics_export_port": 30007,
    }

    # Make sure we can start a node properly.
    head_node = cluster.add_node(**pre_selected_ports)
    cluster.wait_for_nodes()
    cluster.remove_node(head_node)

    # Make sure the wrong worker list will raise an exception.
    with pytest.raises(ValueError):
        head_node = cluster.add_node(
            **pre_selected_ports, worker_port_list="30000,30001,30002,30003")

    # Make sure the wrong min & max worker will raise an exception
    with pytest.raises(ValueError):
        head_node = cluster.add_node(
            **pre_selected_ports, min_worker_port=25000, max_worker_port=35000)


@pytest.mark.skipif(sys.platform != "linux", reason="skip except linux")
def test_ray_init_from_workers(ray_start_cluster):
    cluster = ray_start_cluster
    # add first node
    node1 = cluster.add_node(node_ip_address="127.0.0.2")
    # add second node
    node2 = cluster.add_node(node_ip_address="127.0.0.3")
    address = cluster.address
    password = cluster.redis_password
    assert address.split(":")[0] == "127.0.0.2"
    assert node1.node_manager_port != node2.node_manager_port
    info = ray.init(
        address, _redis_password=password, _node_ip_address="127.0.0.3")
    assert info["node_ip_address"] == "127.0.0.3"

    address_info = ray._private.services.get_address_info_from_redis(
        address, "127.0.0.3", redis_password=password, log_warning=False)
    assert address_info["node_manager_port"] == node2.node_manager_port


if __name__ == "__main__":
    import pytest
    import sys
    sys.exit(pytest.main(["-v", __file__]))
