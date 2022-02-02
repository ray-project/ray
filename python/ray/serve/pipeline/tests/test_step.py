import pytest

from ray.serve import pipeline
from ray.serve.pipeline import ExecutionMode
from ray.serve.pipeline.step import PipelineStep


def test_decorator_no_args():
    @pipeline.step
    def f():
        pass

    assert isinstance(f, PipelineStep)
    assert f.num_replicas == 1

    @pipeline.step
    class A:
        pass

    assert isinstance(A, PipelineStep)
    assert A.num_replicas == 1


def test_decorator_with_arg():
    @pipeline.step(num_replicas=2)
    def f():
        pass

    assert isinstance(f, PipelineStep)
    assert f.num_replicas == 2

    @pipeline.step(num_replicas=5)
    class A:
        pass

    assert isinstance(A, PipelineStep)
    assert A.num_replicas == 5


def test_pass_step_without_calling():
    @pipeline.step
    def step1():
        pass

    @pipeline.step
    def step2():
        pass

    step2(step1(pipeline.INPUT))
    with pytest.raises(TypeError):
        step2(step1)


def test_input_step_multiple_args_rejected():
    @pipeline.step
    def step1():
        pass

    @pipeline.step
    def step2():
        pass

    step1(pipeline.INPUT)
    with pytest.raises(ValueError):
        step1(pipeline.INPUT, step2(pipeline.INPUT))


@pytest.mark.parametrize(
    "execution_mode",
    [
        (ExecutionMode.LOCAL, "LOCAL"),
        (ExecutionMode.TASKS, "TASKS"),
        (ExecutionMode.ACTORS, "ACTORS"),
    ],
)
def test_execution_mode_validation(execution_mode):
    mode_enum, mode_str = execution_mode

    @pipeline.step(execution_mode=mode_enum)
    def f1():
        pass

    @pipeline.step(execution_mode=mode_str)
    def f2():
        pass

    @pipeline.step(execution_mode=mode_str.lower())
    def f3():
        pass

    with pytest.raises(TypeError):

        @pipeline.step(execution_mode=123)
        def f4():
            pass


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-s", __file__]))
