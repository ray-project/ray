import sys
from typing import Dict, Set

import pytest

import ray
import ray._private.usage.usage_lib as ray_usage_lib
from ray import tune


@pytest.fixture
def reset_usage_lib():
    yield
    ray.shutdown()
    ray_usage_lib.reset_global_state()


def _get_library_usages() -> Set[str]:
    return set(
        ray_usage_lib.get_library_usages_to_report(
            ray.experimental.internal_kv.internal_kv_get_gcs_client()
        )
    )


def _get_extra_usage_tags() -> Dict[str, str]:
    return ray_usage_lib.get_extra_usage_tags_to_report(
        ray.experimental.internal_kv.internal_kv_get_gcs_client()
    )


def test_not_used_on_import(reset_usage_lib):
    assert len(_get_library_usages()) == 0
    from ray import tune  # noqa: F401

    assert len(_get_library_usages()) == 0


@pytest.mark.parametrize("callsite", ["driver", "actor", "task"])
def test_tuner_fit(reset_usage_lib, callsite: str):
    assert len(_get_library_usages()) == 0

    def _use_tune():
        def objective(*args):
            pass

        tuner = tune.Tuner(objective)
        tuner.fit()

    if callsite == "driver":
        _use_tune()
    elif callsite == "actor":

        @ray.remote
        class A:
            def __init__(self):
                _use_tune()

        a = A.remote()
        ray.get(a.__ray_ready__.remote())

    elif callsite == "task":

        @ray.remote
        def f():
            _use_tune()

        ray.get(f.remote())

    assert _get_library_usages() in [{"train", "tune"}, {"core", "train", "tune"}]
    extra_usage_tags = _get_extra_usage_tags()
    assert all(
        [
            extra_usage_tags["tune_scheduler"] == "FIFOScheduler",
            extra_usage_tags["tune_searcher"] == "BasicVariantGenerator",
            extra_usage_tags["air_entrypoint"] == "Tuner.fit",
            extra_usage_tags["air_storage_configuration"] == "local",
        ]
    ), extra_usage_tags


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
