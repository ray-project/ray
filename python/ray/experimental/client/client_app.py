import ray.experimental.client as ray

ray.connect("localhost:50051")

@ray.remote
def plus2(x):
    return x + 2

objectref = ray.put("hello world")
print(objectref)

print(ray.get(objectref))

# ref2 = plus2.remote(234)
# print(ref2)

# print(ray.get(ref2))
