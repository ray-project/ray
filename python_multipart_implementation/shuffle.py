import ray
import time


n = 500
ray.init()




start = time.time()

@ray.remote
def shuffle_map(n):
    return [b"a" for _ in range(n)]

@ray.remote
def shuffle_reduce(refs):
    for r in refs:
        ray.get(r)

map_out = [
    shuffle_map.options(num_returns=n).remote(n)
    for _ in range(n)
]
reduce_out = [
    shuffle_reduce.remote([m[i] for m in map_out])
    for i in range(n)
]
ray.get(reduce_out)
print("Normal shuffle", n, "partitions", time.time() - start)



start = time.time()

@ray.remote
def shuffle_map(n):
    return ray.put([b"a" for _ in range(n)], multipart=True)

@ray.remote
def shuffle_reduce(refs, i):
    for r in refs:
        ray.get(r, index=i)

map_out = [
    shuffle_map.remote(n) for _ in range(n)
]
map_out = ray.get(map_out)
reduce_out = [
    shuffle_reduce.remote(map_out, i)
    for i in range(n)
]
ray.get(reduce_out)
print("Range read shuffle", n, "partitions", time.time() - start)
