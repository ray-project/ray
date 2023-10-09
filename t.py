import ray
import ray._private.node
import ray._private.ray_constants as ray_constants
import ray._private.utils
import ray.actor
from ray._private.async_compat import try_install_uvloop
from ray._private.parameter import RayParams
from ray._private.ray_logging import configure_log_file, get_worker_log_file_name
from ray._private.runtime_env.setup_hook import load_and_execute_setup_hook

ray._private.worker.global_worker

ray.init(num_cpus=1)

from ray.util.state import list_workers, list_tasks
@ray.remote
def f():
    print("new task")
    print(hex(id(ray._private.worker.global_worker)))
    import time
    time.sleep(30)

# ray.get(f.remote())
re = f.remote()
import time
time.sleep(1)

t = list_tasks()[0]
# print(t)
idle_pid = t.worker_pid
node_id = t.node_id
worker_id = t.worker_id
import signal
import os

os.kill(idle_pid, signal.SIGTERM)

# for _ in range(100):
#     ray.get(f.remote())
print(f"send signal to {idle_pid}")
time.sleep(5)

# ray.get(f.remote())
# worker_f = f"worker-{worker_id}-ffffffff-{idle_pid}.out"
# f = f"python-core-worker-{worker_id}_{idle_pid}.log"
# from ray.util.state import get_log
# # for l in get_log(filename=worker_f, node_id=node_id, follow=True):
#     # print(l)
# print(f"ray logs {worker_f}")
# import psutil
time.sleep(300)

