import os
import pytest
import ray._private.parameter as parameter
from ray.ha.redis_leader_selector import RedisBasedLeaderSelector


def test_leader_selector_init():
    ray_params = parameter.RayParams()
    leader_selector = RedisBasedLeaderSelector(ray_params, "0.0.0.0:0", "0.0.0.0")
    assert not ray_params.enable_head_ha
    assert str(leader_selector.name, "utf-8").startswith("0.0.0.0:0")
    assert leader_selector._config.check_interval_s == 3
    assert leader_selector._config.key_expire_time_ms == 60000
    assert leader_selector._config.max_failure_count == 5
    assert leader_selector._config.wait_pre_gcs_stop_max_time_s == 3600 * 24 * 3
    assert leader_selector._config.wait_time_after_be_active_s == 2


data_dict = [
    {
        "is_enable": ["true", True],
        "max_failure": ["4", 4],
        "check_inverval": ["600", 0.6],
        "expire_time": ["5000", 5000],
        "wait_time": ["3000", 3.0],
        "wait_gcs_max_time": ["20000", 20],
    },
    {
        "is_enable": ["false", False],
        "max_failure": ["6", 6],
        "check_inverval": ["550", 0.55],
        "expire_time": ["4111", 4111],
        "wait_time": ["3111", 3.111],
        "wait_gcs_max_time": ["22222", 22.222],
    },
    {
        "is_enable": ["True", True],
        "max_failure": ["10", 10],
        "check_inverval": ["10000", 10.0],
        "expire_time": ["30000", 30000],
        "wait_time": ["10000", 10.0],
        "wait_gcs_max_time": ["100000", 100.0],
    },
]


@pytest.fixture(params=data_dict)
def check_ha_env_config(request):
    os.environ["RAY_ENABLE_HEAD_HA"] = request.param["is_enable"][0]
    os.environ["RAY_HA_CHECK_MAX_FAILURE_COUNT"] = request.param["max_failure"][0]
    os.environ["RAY_HA_CHECK_INTERVAL_MS"] = request.param["check_inverval"][0]
    os.environ["RAY_HA_KEY_EXPIRE_TIME_MS"] = request.param["expire_time"][0]
    os.environ["RAY_HA_WAIT_TIME_AFTER_BE_ACTIVE_MS"] = request.param["wait_time"][0]
    os.environ["RAY_HA_WAIT_PRE_GCS_STOP_MAX_TIME_MS"] = request.param[
        "wait_gcs_max_time"
    ][0]
    yield request.param
    os.environ.pop("RAY_ENABLE_HEAD_HA")
    os.environ.pop("RAY_HA_CHECK_MAX_FAILURE_COUNT")
    os.environ.pop("RAY_HA_CHECK_INTERVAL_MS")
    os.environ.pop("RAY_HA_KEY_EXPIRE_TIME_MS")
    os.environ.pop("RAY_HA_WAIT_TIME_AFTER_BE_ACTIVE_MS")
    os.environ.pop("RAY_HA_WAIT_PRE_GCS_STOP_MAX_TIME_MS")


def test_leader_selector_init_param(check_ha_env_config):
    result = check_ha_env_config
    ray_params = parameter.RayParams()
    assert ray_params.enable_head_ha == result["is_enable"][1]

    leader_selector = RedisBasedLeaderSelector(ray_params, "0.0.0.0:0", "0.0.0.0")
    assert str(leader_selector.name, "utf-8").startswith("0.0.0.0:0")
    assert leader_selector._config.check_interval_s == result["check_inverval"][1]
    assert leader_selector._config.key_expire_time_ms == result["expire_time"][1]
    assert leader_selector._config.max_failure_count == result["max_failure"][1]
    assert (
        leader_selector._config.wait_pre_gcs_stop_max_time_s
        == result["wait_gcs_max_time"][1]
    )
    assert leader_selector._config.wait_time_after_be_active_s == result["wait_time"][1]


if __name__ == "__main__":
    import sys
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
