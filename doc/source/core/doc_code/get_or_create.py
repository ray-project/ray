import ray


@ray.remote
class Greeter:
    def __init__(self, value):
        self.value = value

    def say_hello(self):
        return self.value


# Actor `g1` doesn't yet exist, so it is created with the given args.
a = Greeter.options(name="g1", get_if_exists=True).remote("Old Greeting")
assert ray.get(a.say_hello.remote()) == "Old Greeting"

# Actor `g1` already exists, so it is returned (new args are ignored).
b = Greeter.options(name="g1", get_if_exists=True).remote("New Greeting")
assert ray.get(b.say_hello.remote()) == "Old Greeting"
