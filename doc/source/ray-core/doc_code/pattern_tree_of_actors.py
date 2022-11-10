import ray

@ray.remote(num_cpus=1)
class Worker:
    def work(self):
        return "done"

@ray.remote(num_cpus=1)
class Supervisor:
    def __init__(self):
        self.workers = [Worker.remote() for _ in range(3)]

    def work(self):
        return ray.get([w.work.remote() for w in self.workers])

sup = Supervisor.remote()
print(ray.get(sup.work.remote()))  # outputs ['done', 'done', 'done']
