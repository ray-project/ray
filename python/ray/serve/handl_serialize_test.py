import ray
from ray.serve.utils import msgpack_serialize

ray.init()


@ray.remote
class Counter(object):
    def __init__(self):
        self.n = 0

    def increment(self):
        self.n += 1

    def read(self):
        return self.n


counter = Counter.remote()
print(type(counter))


serialized_handle = msgpack_serialize(counter)
print(type(serialized_handle))
print(serialized_handle)

# if __name__ == '__main__':
#     print(f"Started{' detached ' if detached else ' '}Serve instance in "
#           f"namespace '{controller_namespace}'.")
#
#     Counter.deploy()