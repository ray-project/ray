import json

import numpy as np
import pytest

import ray
from ray.serve.utils import ServeEncoder
from ray._private.utils import import_attr


def test_bytes_encoder():
    data_before = {"inp": {"nest": b"bytes"}}
    data_after = {"inp": {"nest": "bytes"}}
    assert json.loads(json.dumps(data_before, cls=ServeEncoder)) == data_after


def test_numpy_encoding():
    data = [1, 2]
    floats = np.array(data).astype(np.float32)
    ints = floats.astype(np.int32)
    uints = floats.astype(np.uint32)

    assert json.loads(json.dumps(floats, cls=ServeEncoder)) == data
    assert json.loads(json.dumps(ints, cls=ServeEncoder)) == data
    assert json.loads(json.dumps(uints, cls=ServeEncoder)) == data


def test_import_attr():
    assert (import_attr("ray.serve.BackendConfig") ==
            ray.serve.config.BackendConfig)
    assert (import_attr("ray.serve.config.BackendConfig") ==
            ray.serve.config.BackendConfig)

    policy_cls = import_attr("ray.serve.controller.TrafficPolicy")
    assert policy_cls == ray.serve.controller.TrafficPolicy

    policy = policy_cls({"endpoint1": 0.5, "endpoint2": 0.5})
    with pytest.raises(ValueError):
        policy.set_traffic_dict({"endpoint1": 0.5, "endpoint2": 0.6})
    policy.set_traffic_dict({"endpoint1": 0.4, "endpoint2": 0.6})

    print(repr(policy))

    # Very meta...
    import_attr_2 = import_attr("ray._private.utils.import_attr")
    assert import_attr_2 == import_attr


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-s", __file__]))
