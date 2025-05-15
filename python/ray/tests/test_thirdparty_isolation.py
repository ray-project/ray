import os
import sys

import pytest

import ray


def test_sys_path_isolation():
    path = os.path.join(
        os.path.dirname(os.path.dirname(ray.__file__)), "thirdparty_files"
    )
    for import_path in sys.path:
        if import_path.endswith(path):
            raise ValueError(f"Found thirdparty_files in sys.path: {import_path}")


def test_thirdparty_isolation():
    try:
        import setprotitle as system_setproctitle
        import colorama as system_colorama
    except ImportError:
        system_setproctitle = None
        system_colorama = None

    from ray._private import setproctitle as ray_setproctitle
    from ray._private import colorama as ray_colorama

    assert system_setproctitle != ray_setproctitle
    assert system_colorama != ray_colorama


if __name__ == "__main__":
    import pytest

    sys.exit(pytest.main(["-v", __file__]))
