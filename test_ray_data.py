import ray

ds = ray.data.range(1000)
ds = ds.map(lambda x: {"id": x["id"], "squared": x["id"]*x["id"]})
ds = ds.filter(lambda x: x["id"] % 3 == 0)
ds = ds.random_shuffle(seed=42)
ds = ds.map(lambda x: {**x, "cubed": x["id"]**3})


for _ in ds.iter_batches():
    pass
