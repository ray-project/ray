import ray

@ray.remote
class NamedActor:
    def __init__(self):
        self.id = ''

    def set_id(self, id):
        self.id = id
        return self.id

    def get_id(self):
        return self.id


@ray.remote
class Worker:
    def __init__(self, rank):
        self.rank = rank

    def do_work(self):
        if self.rank == 0:
            id = '123'
            ac = NamedActor.options(name='uniquename', lifetime="detached").remote()
            ray.wait([ac.set_id.remote('123')])
        else:
            ac = ray.get_actor('uniquename')
            id = ray.get(ac.get_id.remote())
            print(id)
        return id


ray.init()

workers = [Worker.remote(rank) for rank in range(4)]
ret = ray.get(workers[0].do_work.remote())
m = ray.get([workers[i].do_work.remote() for i in range(1, 4)])
