import tempfile

import pytest

from ray.serve import pipeline

def test_basic_sequential():
    @pipeline.step
    def step1(input_arg: str):
        assert isinstance(input_arg, str)
        return input_arg + "|step1"

    @pipeline.step
    def step2(input_arg: str):
        assert isinstance(input_arg, str)
        return input_arg + "|step2"

    sequential = step2(step1(pipeline.INPUT)).deploy()
    assert sequential.call("HELLO") == "HELLO|step1|step2"

def test_basic_parallel():
    @pipeline.step
    def step1(input_arg: str):
        return input_arg

    @pipeline.step
    def step2_1(input_arg: str):
        return f"step2_1_{input_arg}"

    @pipeline.step
    def step2_2(input_arg: str):
        return f"step2_2_{input_arg}"

    @pipeline.step
    def step3(step2_1_output: str, step2_2_output: str):
        return f"{step2_1_output}|{step2_2_output}"

    step1_output = step1(pipeline.INPUT)
    parallel = step3(step2_1(step1_output), step2_2(step1_output)).deploy()
    assert parallel.call("HELLO") == "step2_1_HELLO|step2_2_HELLO"

def test_multiple_inputs():
    @pipeline.step
    def step1(input_arg: str):
        return f"step1_{input_arg}"

    @pipeline.step
    def step2(input_arg: str):
        return f"step2_{input_arg}"

    @pipeline.step
    def step3(step1_output: str, step2_output: str):
        return f"{step1_output}|{step2_output}"

    multiple_inputs = step3(step1(pipeline.INPUT), step2(pipeline.INPUT)).deploy()
    assert multiple_inputs.call("HELLO") == "step1_HELLO|step2_HELLO"

def test_basic_class():
    @pipeline.step
    class GreeterStep:
        def __init__(self, greeting: str):
            self._greeting = greeting

        def __call__(self, name: str):
            return f"{self._greeting} {name}!"

    greeter = GreeterStep("Top of the morning")(pipeline.INPUT).deploy()
    assert greeter.call("Theodore") == "Top of the morning Theodore!"

def test_class_constructor_not_called_until_deployed():
    """Constructor should only be called once, on .deploy()."""

    with tempfile.NamedTemporaryFile("w") as tmp:
        @pipeline.step
        class FileWriter:
            def __init__(self, msg: str):
                tmp.write(msg)
                tmp.flush()

            def __call__(self, arg: str):
                return arg

        msg = "hello"
        def constructor_called_once():
            with open(tmp.name, "r") as f:
                return f.read() == msg

        file_writer = FileWriter(msg)
        assert not constructor_called_once()

        not_deployed = file_writer(pipeline.INPUT)
        assert not constructor_called_once()

        deployed = not_deployed.deploy()
        assert constructor_called_once()

        [deployed.call("hello") for _ in range(100)]
        assert constructor_called_once()

def test_mix_classes_and_functions():
    @pipeline.step
    class GreeterStep1:
        def __init__(self, greeting: str):
            self._greeting = greeting

        def __call__(self, name: str):
            return f"{self._greeting} {name}!"

    @pipeline.step
    def greeter_step_2(name: str):
        return f"How's it hanging, {name}?"

    @pipeline.step
    def combiner(greeting1: str, greeting2: str):
        return f"{greeting1}|{greeting2}"

    greeter = combiner(GreeterStep1("Howdy")(pipeline.INPUT), greeter_step_2(pipeline.INPUT)).deploy()
    assert greeter.call("Teddy") == "Howdy Teddy!|How's it hanging, Teddy?"
