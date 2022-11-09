from queue import Empty
import time
from ray.util.multiprocessing import Pool, JoinableQueue

num_workers = 2

def worker_fn(queue):
    while True:
        try:
            item = queue.get(block=False)
        except Empty:
            time.sleep(0.0001)
            continue

        if item is None:
            break
        # Do some stuff
        
with Pool(processes=num_workers) as pool:
         for i in range(num_workers):
                queue = Queue()
                pool.apply_async(worker_fn,
                                args=(queue),
                                error_callback=None)