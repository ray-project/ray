import ray

ray.init(num_cpus=2)


def compute(i):
    import time
    time.sleep(1)
    return {"value": i**2}


print("start")
pipeline = ray.data.range(1).repeat()
print("start1")
pipeline = pipeline.map(compute).repartition(10)
print("start2")
for row in pipeline.iter_rows():
    print(row)
