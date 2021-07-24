import ray

ray.init(num_cpus=2)


def compute(i):
    return i * 2


print("start")
pipeline = ray.data.range(10).repeat()
print("start1")
pipeline = pipeline.map(compute).map(compute).map(compute).map(
    compute).repartition(1)
print("start2")
for row in pipeline.iter_rows():
    print("OUTPUT", row)
