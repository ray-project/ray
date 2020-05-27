import pytest

from ray.autoscaler.resource_demand_scheduler import _utilization_score, \
    get_bin_pack_residual, get_instances_for

TYPES_A = {
    "m4.large": {
        "resources": {
            "CPU": 2
        },
        "max_workers": 10,
    },
    "m4.4xlarge": {
        "resources": {
            "CPU": 16
        },
        "max_workers": 8,
    },
    "m4.16xlarge": {
        "resources": {
            "CPU": 64
        },
        "max_workers": 4,
    },
    "p2.xlarge": {
        "resources": {
            "CPU": 16,
            "GPU": 1
        },
        "max_workers": 10,
    },
    "p2.8xlarge": {
        "resources": {
            "CPU": 32,
            "GPU": 8
        },
        "max_workers": 4,
    },
}


def test_util_score():
    assert _utilization_score({"CPU": 64}, [{"TPU": 16}]) is None
    assert _utilization_score({"GPU": 4}, [{"GPU": 2}]) == (0.5, 0.5)
    assert _utilization_score({"GPU": 4}, [{"GPU": 1}, {"GPU": 1}]) == \
        (0.5, 0.5)
    assert _utilization_score({"GPU": 2}, [{"GPU": 2}]) == (2, 2)
    assert _utilization_score({"GPU": 2}, [{"GPU": 1}, {"GPU": 1}]) == (2, 2)
    assert _utilization_score({"GPU": 2, "TPU": 1}, [{"GPU": 2}]) == (0, 1)
    assert _utilization_score({"CPU": 64}, [{"CPU": 64}]) == (64, 64)
    assert _utilization_score({"CPU": 64}, [{"CPU": 32}]) == (8, 8)
    assert _utilization_score({"CPU": 64}, [{"CPU": 16}, {"CPU": 16}]) == \
        (8, 8)


def test_bin_pack():
    assert get_bin_pack_residual([], [{"GPU": 2}, {"GPU": 2}]) == \
        [{"GPU": 2}, {"GPU": 2}]
    assert get_bin_pack_residual([{"GPU": 2}], [{"GPU": 2}, {"GPU": 2}]) == \
        [{"GPU": 2}]
    assert get_bin_pack_residual([{"GPU": 4}], [{"GPU": 2}, {"GPU": 2}]) == []
    arg = [{"GPU": 2}, {"GPU": 2, "CPU": 2}]
    assert get_bin_pack_residual(arg, [{"GPU": 2}, {"GPU": 2}]) == []
    arg = [{"CPU": 2}, {"GPU": 2}]
    assert get_bin_pack_residual(arg, [{"GPU": 2}, {"GPU": 2}]) == [{"GPU": 2}]


def test_get_instances_packing_heuristic():
    assert get_instances_for(TYPES_A, {}, 9999, [{"GPU": 8}]) == \
        [("p2.8xlarge", 1)]
    assert get_instances_for(TYPES_A, {}, 9999, [{"GPU": 1}] * 6) == \
        [("p2.8xlarge", 1)]
    assert get_instances_for(TYPES_A, {}, 9999, [{"GPU": 1}] * 4) == \
        [("p2.xlarge", 4)]
    assert get_instances_for(TYPES_A, {}, 9999, [{"CPU": 32, "GPU": 1}] * 3) \
        == [("p2.8xlarge", 3)]
    assert get_instances_for(TYPES_A, {}, 9999, [{"CPU": 64, "GPU": 1}] * 3) \
        == []
    assert get_instances_for(TYPES_A, {}, 9999, [{"CPU": 64}] * 3) == \
        [("m4.16xlarge", 3)]
    assert get_instances_for(TYPES_A, {}, 9999, [{"CPU": 64}, {"CPU": 1}]) \
        == [("m4.16xlarge", 1), ("m4.large", 1)]
    assert get_instances_for(
        TYPES_A, {}, 9999, [{"CPU": 64}, {"CPU": 9}, {"CPU": 9}]) == \
        [("m4.16xlarge", 1), ("m4.4xlarge", 2)]
    assert get_instances_for(TYPES_A, {}, 9999, [{"CPU": 16}] * 5) == \
        [("m4.16xlarge", 1), ("m4.4xlarge", 1)]
    assert get_instances_for(TYPES_A, {}, 9999, [{"CPU": 8}] * 10) == \
        [("m4.16xlarge", 1), ("m4.4xlarge", 1)]
    assert get_instances_for(TYPES_A, {}, 9999, [{"CPU": 1}] * 100) == \
        [("m4.16xlarge", 1), ("m4.4xlarge", 2), ("m4.large", 2)]
    assert get_instances_for(
        TYPES_A, {}, 9999, [{"GPU": 1}] + ([{"CPU": 1}] * 64)) == \
        [("m4.16xlarge", 1), ("p2.xlarge", 1)]
    assert get_instances_for(
        TYPES_A, {}, 9999, ([{"GPU": 1}] * 8) + ([{"CPU": 1}] * 64)) == \
        [("m4.16xlarge", 1), ("p2.8xlarge", 1)]


def test_get_instances_respects_max_limit():
    types = {
        "m4.large": {
            "resources": {
                "CPU": 2
            },
            "max_workers": 10,
        },
        "gpu": {
            "resources": {
                "GPU": 1
            },
            "max_workers": 99999,
        },
    }
    assert get_instances_for(types, {}, 2, [{"CPU": 1}] * 10) == \
        [("m4.large", 2)]
    assert get_instances_for(types, {"m4.large": 9999}, 9999, [{
        "CPU": 1
    }] * 10) == []
    assert get_instances_for(types, {"m4.large": 0}, 9999, [{
        "CPU": 1
    }] * 10) == [("m4.large", 5)]
    assert get_instances_for(types, {"m4.large": 7}, 4, [{
        "CPU": 1
    }] * 10) == [("m4.large", 3)]
    assert get_instances_for(types, {"m4.large": 7}, 2, [{
        "CPU": 1
    }] * 10) == [("m4.large", 2)]


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", __file__]))
