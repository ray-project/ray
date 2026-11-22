from contextlib import nullcontext
from functools import partial

import numpy as np
import pytest

from ray.train._internal.utils import (
    construct_train_func,
    count_required_parameters,
)
from ray.train.utils import _in_ray_train_worker
from ray.train.v2._internal.util import (
    construct_train_func as construct_train_func_v2,
)
from ray.train.v2.api.data_parallel_trainer import DataParallelTrainer


_DEFAULT_WEIGHTS = np.ones(3)


def _train_loop_with_data(config, X, y):
    return config, X, y


def _train_loop_with_array_default(config, weights=_DEFAULT_WEIGHTS):
    return config, weights


@pytest.mark.parametrize(
    "fn",
    [
        partial(_train_loop_with_data, X=np.ones((3, 2)), y=np.zeros(3)),
        _train_loop_with_array_default,
    ],
)
def test_count_required_parameters_array_like_defaults(fn):
    assert count_required_parameters(fn) == 1
    wrapped_v1 = construct_train_func(fn, {"lr": 0.01}, train_func_context=nullcontext)
    wrapped_v2 = construct_train_func_v2(
        fn, {"lr": 0.01}, train_func_context=nullcontext
    )
    assert callable(wrapped_v1)
    assert callable(wrapped_v2)


def test_count_required_parameters_pandas_defaults():
    pd = pytest.importorskip("pandas")
    fn = partial(_train_loop_with_data, X=pd.DataFrame({"a": [1, 2]}), y=1)
    assert count_required_parameters(fn) == 1
    wrapped_v1 = construct_train_func(fn, {"lr": 0.01}, train_func_context=nullcontext)
    wrapped_v2 = construct_train_func_v2(
        fn, {"lr": 0.01}, train_func_context=nullcontext
    )
    assert callable(wrapped_v1)
    assert callable(wrapped_v2)


def test_in_ray_train_worker(ray_start_4_cpus):
    assert not _in_ray_train_worker()

    def train_fn():
        assert _in_ray_train_worker()

    trainer = DataParallelTrainer(train_fn)
    trainer.fit()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
