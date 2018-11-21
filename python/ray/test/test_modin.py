from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import ray  # noqa F401


def test_modin_import():
    import modin.pandas as pd
    frame_data = [1, 2, 3, 4, 5, 6, 7, 8]
    frame = pd.DataFrame(frame_data)
    assert frame.sum().squeeze() == sum(frame_data)
