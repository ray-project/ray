import ray
import time
from multiprocessing import Process
# ray.init(num_cpus=1)
@ray.remote
class MyActor:

    def run(self):
       p = Process(target=time.sleep, args=(1000,), daemon=True)
       p.start()
       p.join()

actor = MyActor.remote()
actor.run.remote()
del actor
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
