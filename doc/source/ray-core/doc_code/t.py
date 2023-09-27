import ray
import logging

# driver.py
def logging_setup_func():
    logging.basicConfig(level=logging.INFO)

ray.init(runtime_env={"worker_process_setup_hook": logging_setup_func})
logging_setup_func()

@ray.remote
class Actor:
    def f(self):
        logging.info("abc")

a = Actor.remote()
ray.get(a.__ray_ready__.remote())
ray.get(a.f.remote())
# ray.get(g.remote())

import time
time.sleep(30)
