import ray

ref = ray.put([b"hello", b"goodbye", b"hi"], multipart=True)

print(ray.get(ref, index=0))
print(ray.get(ref, index=1))
print(ray.get(ref, index=2))
