import os
import sys
import ray

from multiprocessing import Process
v = 10
@ray.remote
def g():
  return "AB"

def info(title, c):
    print(title)
    print('module name:', __name__)
    print('parent process:', os.getppid())
    print('process id:', os.getpid())
    print("GETTING")
    print('c: ', ray.get(c))
    from time import sleep
    sleep(1)
    print("Info>>>", v)
    raise Exception("ABC")
    print("END")

if __name__ == '__main__':
    ray.init()
    p = g.remote()
    from time import sleep
    sleep(1)
    p = Process(target=info, args=('bob', p))
    p.start()
    v = 20
    print(v)
    p.join()
    print("AAA")
