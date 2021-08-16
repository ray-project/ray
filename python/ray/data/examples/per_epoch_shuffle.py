import ray
import numpy as np

ray.init(num_cpus=8, object_store_memory=4e9)


@ray.remote
def consume(name, split):
    i = 0
    for row in split.iter_rows():
        i += 1


def gen(i):
    return {
        "a": 4.0,
        "b": "hello" * 1000,
        "c": np.zeros(25000),
    }


ds = ray.data.range(10000).map(gen)
print(ds, ds.size_bytes())
pipeline = ds.repeat(100).random_shuffle()
print(pipeline)
a, b, c, d, e = pipeline.split(5, equal=True)
x1 = consume.remote("consumer A", a)
x2 = consume.remote("consumer B", b)
x3 = consume.remote("consumer C", c)
x4 = consume.remote("consumer D", d)
x5 = consume.remote("consumer E", e)
ray.get([x1, x2, x3, x4, x5])
