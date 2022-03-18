import ray

@ray.remote
class Counter:
    def __init__(self, value):
        self.value = value

    def incr(self):
        self.value += 1
        return self.value


# Actor doesn't yet exist, so it is created with the given args.
a = Counter.options(name="foo").get_or_create(10)
assert ray.get(a.incr.remote()) == 11

# Actor already exists, so the given args (100,) are ignored.
b = Counter.options(name="foo").get_or_create(100)
assert ray.get(b.incr.remote()) == 12
