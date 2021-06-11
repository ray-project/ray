from ray.experimental import workflow


@workflow.step
def identity(x):
    return x


@workflow.step
def projection(x, _):
    return x


@workflow.step
def variable_mutable():
    x = []
    a = identity.step(x)
    x.append(1)
    b = identity.step(x)
    return projection.step(a, b)


def test_variable_mutable():
    import ray
    ray.init()

    outputs = workflow.run(variable_mutable.step())
    assert ray.get(outputs) == []
    ray.shutdown()
