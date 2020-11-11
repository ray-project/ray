import ray.experimental.client as ray

ray.connect("localhost:50051")

objectref = ray.put("hello world")
print(objectref)

print(ray.get(objectref))
