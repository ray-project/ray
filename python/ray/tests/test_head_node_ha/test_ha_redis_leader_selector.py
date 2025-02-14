import time
import os
import ray._private.parameter as parameter
from ray.cluster_utils import Cluster
from ray.ha import RedisBasedLeaderSelector
from ray.ha.redis_leader_selector import get_cluster_head_num
from ray._private.test_utils import wait_for_condition
from unittest.mock import patch
import requests

become_active_wait_time = 65


def test_leader_selector_start(ray_start_cluster_head_with_external_redis):
    cluster: Cluster = ray_start_cluster_head_with_external_redis
    ray_params = parameter.RayParams()
    ray_params.redis_password = cluster.redis_password
    leader_selector = RedisBasedLeaderSelector(
        ray_params, cluster.redis_address, "0.0.0.0"
    )
    leader_selector.start()
    time.sleep(1)
    leader_selector_2 = RedisBasedLeaderSelector(
        ray_params, cluster.redis_address, "1.1.1.1"
    )
    leader_selector_2.start()
    wait_for_condition(lambda: leader_selector.is_leader(), 5)
    wait_for_condition(lambda: not leader_selector_2.is_leader(), 5)
    leader_selector.stop()
    assert not leader_selector.is_leader()

    # test active/standby switch
    wait_for_condition(lambda: leader_selector_2.is_leader(), become_active_wait_time)
    leader_selector.start()
    wait_for_condition(lambda: not leader_selector.is_leader(), 5)
    leader_selector_2.stop()
    wait_for_condition(lambda: leader_selector.is_leader(), become_active_wait_time)
    leader_selector.stop()
    # test stop repeat
    leader_selector.stop()


def test_leader_selector_keep_leader(ray_start_cluster_head_with_external_redis):
    cluster: Cluster = ray_start_cluster_head_with_external_redis
    ray_params = parameter.RayParams()
    ray_params.redis_password = cluster.redis_password
    leader_selector = RedisBasedLeaderSelector(
        ray_params, cluster.redis_address, "0.0.0.0"
    )
    leader_selector.start()
    time.sleep(1)
    leader_selector_2 = RedisBasedLeaderSelector(
        ray_params, cluster.redis_address, "1.1.1.1"
    )
    leader_selector_2.start()
    leader_selector_3 = RedisBasedLeaderSelector(
        ray_params, cluster.redis_address, "2.2.2.2"
    )
    leader_selector_3.start()
    leader_selector_4 = RedisBasedLeaderSelector(
        ray_params, cluster.redis_address, "2.2.2.2"
    )
    leader_selector_4.start()
    wait_for_condition(lambda: leader_selector.is_leader(), 5)
    wait_for_condition(lambda: not leader_selector_2.is_leader(), 5)
    wait_for_condition(lambda: not leader_selector_3.is_leader(), 5)
    wait_for_condition(lambda: not leader_selector_4.is_leader(), 5)
    time.sleep(10)
    assert leader_selector.is_leader()
    assert not leader_selector_2.is_leader()
    assert not leader_selector_3.is_leader()
    assert not leader_selector_4.is_leader()
    leader_selector.stop()
    leader_selector_2.stop()
    leader_selector_3.stop()
    leader_selector_4.stop()


def test_selector_while_disconnect_redis(external_redis):
    redis_cluster = external_redis
    ray_params = parameter.RayParams()
    ray_params.redis_password = redis_cluster.redis_password
    leader_selector = RedisBasedLeaderSelector(
        ray_params, redis_cluster.redis_address, "0.0.0.0"
    )
    leader_selector.start()
    leader_selector_2 = RedisBasedLeaderSelector(
        ray_params, redis_cluster.redis_address, "1.1.1.1"
    )
    leader_selector_2.start()
    wait_for_condition(lambda: leader_selector.is_leader(), 5)
    wait_for_condition(lambda: not leader_selector_2.is_leader(), 5)
    redis_cluster.shutdown()
    wait_for_condition(lambda: not leader_selector.is_leader(), become_active_wait_time)
    wait_for_condition(
        lambda: not leader_selector_2.is_leader(), become_active_wait_time
    )


def test_special_action(ray_start_cluster_head_with_external_redis):
    os.environ["RAY_HA_WAIT_PRE_GCS_STOP_MAX_TIME_MS"] = "10000"
    cluster: Cluster = ray_start_cluster_head_with_external_redis
    ray_params = parameter.RayParams()
    ray_params.redis_password = cluster.redis_password
    leader_selector = RedisBasedLeaderSelector(
        ray_params, cluster.redis_address, "0.0.0.0"
    )
    leader_selector.start()
    start_time = time.time()
    assert not leader_selector.do_action_after_be_active()
    assert (time.time() - start_time) > 9
    os.environ.pop("RAY_HA_WAIT_PRE_GCS_STOP_MAX_TIME_MS")


def test_get_cluster_head_num():
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response._content = b'{"success":true,"message":"try to get cluster pod information: ray-test-zhiyu in namespace: arconkube","data":{"head":[{"default":{"replicasUpdated":1,"replicasTotal":1}}]}}\n'  # noqa: E501

    with patch("requests.post", return_value=mock_response):
        assert get_cluster_head_num(5) == 1

    mock_response.status_code = 200
    mock_response._content = b'{"success":true,"message":"try to get cluster pod information: ray-test-zhiyu in namespace: arconkube","data":{"head":[{"default":{"replicasUpdated":2,"replicasTotal":2}}]}}\n'  # noqa: E501
    with patch("requests.post", return_value=mock_response):
        assert get_cluster_head_num(5) == 2

    mock_response.status_code = 200
    mock_response._content = (
        b'{"success":false,"message":"invalid json :","data":null}\n'
    )
    with patch("requests.post", return_value=mock_response):
        assert get_cluster_head_num(5) == -1

    mock_response.status_code = 404
    mock_response._content = b"OK"
    with patch("requests.post", return_value=mock_response):
        assert get_cluster_head_num(5) == -1


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
