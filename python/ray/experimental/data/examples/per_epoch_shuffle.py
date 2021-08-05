import ray

ray.init()


@ray.remote
def consume(name, split):
    i = 0
    for row in split.iter_rows():
        i += 1


pipeline = ray.data.range(100000).repeat(100).random_shuffle()
a, b = pipeline.split(2)
x1 = consume.remote("consumer A", a)
x2 = consume.remote("consumer B", b)
ray.get(x1)
ray.get(x2)
