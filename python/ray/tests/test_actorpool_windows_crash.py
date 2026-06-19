import sys
import subprocess
import pytest

@pytest.mark.skipif(
    sys.platform != "win32",
    reason="This test is specifically for a Windows access violation bug",
)
def test_actorpool_windows_teardown_crash():
    """
    Tests that initializing an ActorPool with a runtime_env on Windows
    does not cause a fatal access violation during Ray shutdown teardown.
    See: https://github.com/ray-project/ray/issues/62442
    """
    script = """
import ray
from ray.util import ActorPool

# Initialize ray with a dummy runtime_env that triggers the setup hook
ray.init(runtime_env={"worker_process_setup_hook": lambda: None})

@ray.remote
class DummyActor:
    def do_work(self):
        return "success"

# Create an ActorPool
actors = [DummyActor.remote() for _ in range(2)]
pool = ActorPool(actors)

# Submit dummy work
results = list(pool.map(lambda a, v: a.do_work.remote(), [1, 2]))
assert results == ["success", "success"]

# Crucially, we do NOT call ray.shutdown() here.
# We let the script exit naturally so that Python's atexit and C++ destructors
# run during sys.is_finalizing(), triggering the bug if unpatched.
"""

    # Run the script in a subprocess
    process = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True
    )
    
    # A segfault/access violation will result in a non-zero exit code 
    # (usually a large negative number or a Windows exception code like 0xC0000005)
    assert process.returncode == 0, f"Subprocess crashed!\nSTDOUT: {process.stdout}\nSTDERR: {process.stderr}"

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
