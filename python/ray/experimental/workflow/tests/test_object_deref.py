import ray
from ray.experimental import workflow


@ray.remote
def nested_ref():
    return ray.put(42)


@workflow.step
def nested_workflow(n):
    if n <= 0:
        return "nested"
    else:
        return nested_workflow.step(n - 1)


@workflow.step
def deref_check(u, v, w, x, y, z):
    try:
        return (u == 42 and ray.get(v) == 42 and ray.get(ray.get(w[0])) == 42
                and x == "nested" and y[0] == "nested"
                and z[0]["output"] == "nested")
    except Exception:
        return False


@workflow.step
def deref_shared(x, y):
    # x and y should share the same variable.
    x.append(2)
    return y == [1, 2]


@workflow.step
def empty_list():
    return [1]


def test_object_deref():
    ray.init()

    output = workflow.run(
        deref_check.step(
            ray.put(42), nested_ref.remote(), [nested_ref.remote()],
            nested_workflow.step(10), [nested_workflow.step(9)], [{
                "output": nested_workflow.step(7)
            }]), )
    assert ray.get(output)

    x = empty_list.step()
    output = workflow.run(deref_shared.step(x, x))
    assert ray.get(output)

    ray.shutdown()
