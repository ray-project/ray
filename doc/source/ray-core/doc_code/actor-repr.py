import ray


@ray.remote
class MyActor:
    def __init__(self, index):
        self.index = index

    def foo(self):
        print("hello there")

    def __repr__(self):
        return f"MyActor(index={self.index})"


a = MyActor.remote(1)
b = MyActor.remote(2)
ray.get(a.foo.remote())
ray.get(b.foo.remote())
