import os
import pytest
import ray

@pytest.mark.skipif("sys.platform != 'linux'")
def test_environ_file_on_linux(ray_start_10_cpus):

    @ray.remote
    class Actor1:
        def __init__(self):
            pass

        def get_env_from_proc(self):
            pid = os.getpid()
            f = open('/proc/%d/environ' % pid, 'r')
            return f.readlines()

        def get_os_environ(self):
            return os.environ


    a = Actor1.remote()
    actor_proc_environ = ray.get(a.get_env_from_proc.remote())
    actor_os_environ = ray.get(a.get_os_environ.remote())
    assert len(actor_proc_environ) > 0 
    assert len(actor_os_environ) > 0


if __name__ == "__main__":
    import pytest
    import sys

    os.environ["LC_ALL"] = "en_US.UTF-8"
    os.environ["LANG"] = "en_US.UTF-8"
    sys.exit(pytest.main(["-v", __file__]))
