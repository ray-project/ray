import ray

# Initiate a driver.
ray.init()


@ray.remote
def task():
    print("task")


ray.get(task.remote())


@ray.remote
class Actor:
    def ready(self):
        print("actor")


actor = Actor.remote()
ray.get(actor.ready.remote())
