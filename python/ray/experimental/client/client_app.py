import ray.experimental.client as ray

ray.connect("localhost:50051")

@ray.remote
def plus2(x):
    return x + 2

@ray.remote
def fact(x):
    print(x, type(fact))
    #return fact.remote(x - 1) * x

objectref = ray.put("hello world")
print(objectref)

print(ray.get(objectref))

print(fact, type(fact))


ref2 = plus2.remote(234)
print(ref2)

print(ray.get(ref2))

ref3 = fact.remote(5)
print(ref3)
print(ray.get(ref3))
