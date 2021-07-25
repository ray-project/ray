import ray

ray.init(num_cpus=2)


def compute(i):
    return i * 2


pipeline = ray.data.range(10).repeat(100)
pipeline = pipeline.map(compute).map(compute).map(compute).map(compute)
pipeline.repartition(1).write_json("/tmp/output")
