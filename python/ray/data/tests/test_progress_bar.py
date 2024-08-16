import functools

import pytest
from pytest import fixture

import ray
from ray.data._internal.progress_bar import ProgressBar


@fixture(params=[True, False])
def enable_tqdm_ray(request):
    context = ray.data.DataContext.get_current()
    original_use_ray_tqdm = context.use_ray_tqdm
    context.use_ray_tqdm = request.param
    yield request.param
    context.use_ray_tqdm = original_use_ray_tqdm


def test_set_progress_bars_is_deprecated():
    with pytest.raises(DeprecationWarning):
        ray.data.set_progress_bars(True)


def test_progress_bar(enable_tqdm_ray):
    total = 10
    # Used to record the total value of the bar at close
    total_at_close = 0

    def patch_close(bar):
        nonlocal total_at_close
        total_at_close = 0
        original_close = bar.close

        @functools.wraps(original_close)
        def wrapped_close():
            nonlocal total_at_close
            total_at_close = bar.total

        bar.close = wrapped_close

    # Test basic usage
    pb = ProgressBar("", total, "", enabled=True)
    assert pb._bar is not None
    patch_close(pb._bar)
    for _ in range(total):
        pb.update(1)
    pb.close()

    assert pb._progress == total
    assert total_at_close == total

    # Test if update() exceeds the original total, the total will be updated.
    pb = ProgressBar("", total, "", enabled=True)
    assert pb._bar is not None
    patch_close(pb._bar)
    new_total = total * 2
    for _ in range(new_total):
        pb.update(1)
    pb.close()

    assert pb._progress == new_total
    assert total_at_close == new_total

    # Test that if the bar is not complete at close(), the total will be updated.
    pb = ProgressBar("", total, "")
    assert pb._bar is not None
    patch_close(pb._bar)
    new_total = total // 2
    for _ in range(new_total):
        pb.update(1)
    pb.close()

    assert pb._progress == new_total
    assert total_at_close == new_total

    # Test updating the total
    pb = ProgressBar("", total, "", enabled=True)
    assert pb._bar is not None
    patch_close(pb._bar)
    new_total = total * 2
    pb.update(0, new_total)

    assert pb._bar.total == new_total
    pb.update(total + 1, total)
    assert pb._bar.total == total + 1
    pb.close()
