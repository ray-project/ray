import os
import pytest


@pytest.mark.linux
def test_environ_file_on_linux():
    def get_env(pid):
        f = open('/proc/%d/environ' % pid, 'r')
        return f.readlines()

    pid = os.getpid()
    orig_env = get_env(pid)

    import ray

    new_env = get_env(pid)
    assert new_env == orig_env


if __name__ == "__main__":
    import pytest
    import sys

    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
