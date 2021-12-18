import ray
import time

N = 10000

start = time.time()
x = []
for _ in range(N):
    x.append(ray.put(b"a"))
print("Time creating", N, "objects", time.time() - start)
start = time.time()
for ref in x:
    ray.get(ref)
print("Time fetching", N, "objects", time.time() - start)

start = time.time()
x = [b"a"] * N
ref = ray.put(x, multipart=True)
print("Time creating multipart object", time.time() - start)
start = time.time()
for i in range(N):
    ray.get(ref, index=i)
print("Time fetching", N, "ranges", time.time() - start)
