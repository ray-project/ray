from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import pytest
import ray


@pytest.mark.skipif(
    sys.version_info < (3, 5, 3), reason="requires python3.5.3 or higher")
def test_get_webui():
    addresses = ray.init(include_webui=True)
    webui_url = addresses["webui_url"]
    assert ray.worker.get_webui_url() == webui_url

    ray.shutdown()
