import sys
import tempfile
import os
import psutil

import pytest

from ray.serve import pipeline
from ray.serve.pipeline.test_utils import enable_local_execution_mode_only
from ray._private.test_utils import wait_for_condition

ALL_EXECUTION_MODES = list(pipeline.ExecutionMode)


@pytest.mark.parametrize("execution_mode", ALL_EXECUTION_MODES)
@enable_local_execution_mode_only
def test_basic_sequential(execution_mode, shared_ray_instance):
    @pipeline.step(execution_mode=execution_mode)
    def step1(input_arg: str):
        assert isinstance(input_arg, str)
        return input_arg + "|step1"

    @pipeline.step(execution_mode=execution_mode)
    def step2(input_arg: str):
        assert isinstance(input_arg, str)
        return input_arg + "|step2"

    sequential = step2(step1(pipeline.INPUT)).deploy()
    assert sequential.call("HELLO") == "HELLO|step1|step2"


@pytest.mark.parametrize("execution_mode", ALL_EXECUTION_MODES)
@enable_local_execution_mode_only
def test_basic_parallel(execution_mode, shared_ray_instance):
    @pipeline.step(execution_mode=execution_mode)
    def step1(input_arg: str):
        return input_arg

    @pipeline.step(execution_mode=execution_mode)
    def step2_1(input_arg: str):
        return f"step2_1_{input_arg}"

    @pipeline.step(execution_mode=execution_mode)
    def step2_2(input_arg: str):
        return f"step2_2_{input_arg}"

    @pipeline.step(execution_mode=execution_mode)
    def step3(step2_1_output: str, step2_2_output: str):
        return f"{step2_1_output}|{step2_2_output}"

    step1_output = step1(pipeline.INPUT)
    parallel = step3(step2_1(step1_output), step2_2(step1_output)).deploy()
    assert parallel.call("HELLO") == "step2_1_HELLO|step2_2_HELLO"


@pytest.mark.parametrize("execution_mode", ALL_EXECUTION_MODES)
@enable_local_execution_mode_only
def test_multiple_inputs(execution_mode, shared_ray_instance):
    @pipeline.step(execution_mode=execution_mode)
    def step1(input_arg: str):
        return f"step1_{input_arg}"

    @pipeline.step(execution_mode=execution_mode)
    def step2(input_arg: str):
        return f"step2_{input_arg}"

    @pipeline.step(execution_mode=execution_mode)
    def step3(step1_output: str, step2_output: str):
        return f"{step1_output}|{step2_output}"

    multiple_inputs = step3(step1(pipeline.INPUT), step2(pipeline.INPUT)).deploy()
    assert multiple_inputs.call("HELLO") == "step1_HELLO|step2_HELLO"


@pytest.mark.parametrize("execution_mode", ALL_EXECUTION_MODES)
@enable_local_execution_mode_only
def test_basic_class(execution_mode, shared_ray_instance):
    @pipeline.step(execution_mode=execution_mode)
    class GreeterStep:
        def __init__(self, greeting: str):
            self._greeting = greeting

        def __call__(self, name: str):
            return f"{self._greeting} {name}!"

    greeter = GreeterStep("Top of the morning")(pipeline.INPUT).deploy()
    assert greeter.call("Theodore") == "Top of the morning Theodore!"


@pytest.mark.parametrize("execution_mode", ALL_EXECUTION_MODES)
@enable_local_execution_mode_only
def test_class_constructor_not_called_until_deployed(
    execution_mode, shared_ray_instance
):
    """Constructor should only be called after .deploy()."""

    tmp = tempfile.NamedTemporaryFile("w+", suffix=".tmp", delete=False)
    tmp.close()

    @pipeline.step(execution_mode=execution_mode)
    class FileWriter:
        def __init__(self, tmpfile_name, msg: str):
            with open(tmpfile_name, "w") as tmpfile:
                tmpfile.write(msg)
                tmpfile.flush()

        def __call__(self, arg: str):
            return arg

    msg = "hello"

    def constructor_called():
        with open(tmp.name, "r") as f:
            ret = f.readline() == msg
        return ret

    file_writer = FileWriter(tmp.name, msg)
    assert not constructor_called()

    writer_pipeline = file_writer(pipeline.INPUT)
    assert not constructor_called()

    assert writer_pipeline.deploy().call("hi") == "hi"
    assert constructor_called()

    os.unlink(tmp.name)


@pytest.mark.parametrize("execution_mode", ALL_EXECUTION_MODES)
@enable_local_execution_mode_only
def test_mix_classes_and_functions(execution_mode, shared_ray_instance):
    @pipeline.step(execution_mode=execution_mode)
    class GreeterStep1:
        def __init__(self, greeting: str):
            self._greeting = greeting

        def __call__(self, name: str):
            return f"{self._greeting} {name}!"

    @pipeline.step(execution_mode=execution_mode)
    def greeter_step_2(name: str):
        return f"How's it hanging, {name}?"

    @pipeline.step(execution_mode=execution_mode)
    def combiner(greeting1: str, greeting2: str):
        return f"{greeting1}|{greeting2}"

    greeter = combiner(
        GreeterStep1("Howdy")(pipeline.INPUT), greeter_step_2(pipeline.INPUT)
    ).deploy()
    assert greeter.call("Teddy") == "Howdy Teddy!|How's it hanging, Teddy?"


def test_cleanup_and_destory(shared_ray_instance):
    @pipeline.step(execution_mode="actors")
    class A:
        def __call__(self, _input):
            return os.getpid()

    p = A()(pipeline.INPUT).deploy()
    actor_pid = p.call(None)
    assert psutil.pid_exists(actor_pid)
    del p
    wait_for_condition(lambda: not psutil.pid_exists(actor_pid))


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
