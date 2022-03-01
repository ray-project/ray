import ray


@ray.remote
def py_return_input(v):
    return v


@ray.remote
def py_return_val():
    return 42


@ray.remote
class Counter(object):
    def __init__(self, value):
        self.value = int(value)

    def increase(self, delta):
        self.value += int(delta)
        return str(self.value)
