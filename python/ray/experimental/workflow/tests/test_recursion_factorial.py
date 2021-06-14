from ray.experimental import workflow


@workflow.step
def mul(a, b):
    return a * b


@workflow.step
def factorial(n):
    if n == 1:
        return 1
    else:
        return mul.step(n, factorial.step(n - 1))


@workflow.step
def recursion_factorial(n):
    return factorial.step(n)


def test_recursion_factorial():
    import ray
    ray.init()

    outputs = workflow.run(recursion_factorial.step(10))
    assert ray.get(outputs) == 3628800
    ray.shutdown()
