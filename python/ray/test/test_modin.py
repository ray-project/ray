from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pytest

import ray  # noqa F401


def test_modin_import_with_ray_init():
    ray.init()
    import modin.pandas as pd
    frame_data = [1, 2, 3, 4, 5, 6, 7, 8]
    frame = pd.DataFrame(frame_data)
    assert frame.sum().squeeze() == sum(frame_data)


# The following can be activated once we have a modin release that
# includes https://github.com/modin-project/modin/pull/472.
@pytest.mark.skip(reason="needs #472 in modin")
def test_modin_import():
    import modin.pandas as pd
    frame_data = [1, 2, 3, 4, 5, 6, 7, 8]
    frame = pd.DataFrame(frame_data)
    assert frame.sum().squeeze() == sum(frame_data)
