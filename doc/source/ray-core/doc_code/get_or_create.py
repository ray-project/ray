import ray


@ray.remote
class Greeter:
    def __init__(self, value):
        self.value = value

    def say_hello(self):
        return self.value


# Actor `g1` doesn't yet exist, so it is created with the given args.
a = Greeter.options(name="g1").get_or_create("Hi Alice")
assert ray.get(a.say_hello.remote()) == "Hi Alice"

# Actor `g1` already exists, so the given args ("Hi Alice",) are ignored.
b = Greeter.options(name="g1").get_or_create("Hi Dave")
assert ray.get(b.say_hello.remote()) == "Hi Alice"
