import ray.experimental.client as ray

ray.connect("localhost:50051")


@ray.remote
def plus2(x):
    return x + 2


@ray.remote
def fact(x):
    print(x, type(fact))
    if x <= 0:
        return 1
    # This hits the "nested tasks" issue
    # https://github.com/ray-project/ray/issues/3644
    # So we're on the right track!
    return ray.get(fact.remote(x - 1)) * x


objectref = ray.put("hello world")

# `ClientObjectRef(...)`
print(objectref)

# `hello world`
print(ray.get(objectref))

ref2 = plus2.remote(234)
# `ClientObjectRef(...)`
print(ref2)
# `236`
print(ray.get(ref2))

ref3 = fact.remote(20)
# `ClientObjectRef(...)`
print(ref3)
# `2432902008176640000`
print(ray.get(ref3))

# Reuse the cached ClientRemoteFunc object
ref4 = fact.remote(5)
# `120`
print(ray.get(ref4))

ref5 = fact.remote(10)

print([ref2, ref3, ref4, ref5])
# should return ref2, ref3, ref4
res = ray.wait([ref5, ref2, ref3, ref4], num_returns=3)
print(res)
assert [ref2, ref3, ref4] == res[0]
assert [ref5] == res[1]

# should return ref2, ref3, ref4, ref5
res = ray.wait([ref2, ref3, ref4, ref5], num_returns=4)
print(res)
assert [ref2, ref3, ref4, ref5] == res[0]
assert [] == res[1]
