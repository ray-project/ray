import ray
ray.init()
@ray.remote
class Actor:
    def __init__(self):
        self.data = 1

    def get_data(self):
        return self.data

a = Actor.options(name="GM").remote()
b = Actor.options(name="GM").remote()
print(ray.get(b.get_data.remote()))
