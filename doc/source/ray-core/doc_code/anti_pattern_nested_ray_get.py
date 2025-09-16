# __anti_pattern_start__
import ray
import time


@ray.remote
def f():
    return 1


@ray.remote
def pass_via_nested_ref(refs):
    print(sum(ray.get(refs)))


@ray.remote
def pass_via_direct_arg(*args):
    print(sum(args))


# Anti-pattern: Passing nested refs requires `ray.get` in a nested task.
ray.get(pass_via_nested_ref.remote([f.remote() for _ in range(3)]))

# Better approach: Pass refs as direct arguments. Use *args syntax to unpack
# multiple arguments.
ray.get(pass_via_direct_arg.remote(*[f.remote() for _ in range(3)]))
# __anti_pattern_end__
