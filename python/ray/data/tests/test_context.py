import pytest

import ray
from ray.util.annotations import RayDeprecationWarning


def test_write_file_retry_on_errors_emits_deprecation_warning(caplog):
    ctx = ray.data.DataContext.get_current()
    with pytest.warns(DeprecationWarning):
        ctx.write_file_retry_on_errors = []


@pytest.mark.parametrize(
    ("attr", "value"),
    [
        ("scheduling_strategy", "DEFAULT"),
        ("scheduling_strategy_large_args", "SPREAD"),
        ("large_args_threshold", 1),
    ],
)
def test_scheduling_config_emits_deprecation_warning(attr, value):
    ctx = ray.data.DataContext()
    with pytest.warns(RayDeprecationWarning, match=rf"DataContext\.{attr}"):
        setattr(ctx, attr, value)


def test_data_context_current_context_manager():
    import copy

    from ray.data.context import DataContext

    original = DataContext.get_current()
    ctx1 = copy.deepcopy(original)
    ctx1.set_config("level", "1")

    ctx2 = copy.deepcopy(original)
    ctx2.set_config("level", "2")

    with pytest.raises(ValueError):
        with DataContext.current(ctx1):
            assert DataContext.get_current() is ctx1
            # Nested context manager
            with DataContext.current(ctx2):
                assert DataContext.get_current().get_config("level") == "2"

            assert DataContext.get_current().get_config("level") == "1"

            # Test that raising will reset context too
            raise ValueError("boom")

    assert DataContext.get_current() is original


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
