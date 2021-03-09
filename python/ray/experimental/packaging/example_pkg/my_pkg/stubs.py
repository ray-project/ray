import ray


@ray.remote
class MyActor:
    def __init__(self):
        from my_pkg import impl
        self.impl = impl

    def f(self):
        return self.impl.hello()


@ray.remote
def my_func():
    from my_pkg import impl
    return impl.hello()
