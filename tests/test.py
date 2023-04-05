#!/usr/bin/env python3

import time
from multiprocessing import Process

import ray

import subprocess
subprocess.run("ps aux | grep ray:: | grep -v grep | awk '{print $2}' | xargs kill -9", shell=True)

# ray.init(num_cpus=1)
@ray.remote
class MyActor:
    def __init__(self):
        self.p = None

    def run(self):
        p = Process(target=time.sleep, args=(1000,), daemon=True)
        p.start()
        self.p = p
        #p.join()

    def __del__(self):
        print('destructor')
        print(f'del self {self}')
        #del self.p


actor = MyActor.remote()
actor.run.remote()

#del actor
print('sleeping')
time.sleep(300)
# ray.get(actor.run.remote())


# import ray
# import time
# from multiprocessing import Process

# if __name__ == '__main__':
#     p = Process(target=time.sleep, args=(1000,), daemon=True)
#     print(p)
#     p.start()
#     p.join()
