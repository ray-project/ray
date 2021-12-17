import ray

ref = ray.put(["hello", "hi", "goodbye"], multipart=True)

print(ray.get(ref, index=0))
print(ray.get(ref, index=1))
print(ray.get(ref, index=2))
