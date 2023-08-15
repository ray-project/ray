#!/usr/bin/env python3

import os
import ray
from multiprocessing import Process

def fn():
    import time
    import setproctitle
    setproctitle.setproctitle('cadecadecadecade')
    time.sleep(1000)

@ray.remote
class MyActor:

    def run(self):
       p = Process(target=fn, daemon=True)
       p.start()
       print(f'Joining sleep process pid={os.getpid()}')
       p.join()

actor = MyActor.remote()
ray.get(actor.run.remote())
