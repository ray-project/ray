import unittest

from ray.rllib.models.base_model import (
    UnrollOutputType,
    Model,
    RecurrentModel,
    ForwardOutputType,
)
import numpy as np
from ray.rllib.models.temp_spec_classes import TensorDict, SpecDict


class NpRecurrentModelImpl(RecurrentModel):
    input_spec = SpecDict({"in": "h"}, h=3)
    output_spec = SpecDict({"out": "o"}, o=2)
    next_state_spec = SpecDict({"out": "i"}, i=4)
    prev_state_spec = SpecDict({"in": "o"}, o=1)

    def __init__(self, input_check=None, output_check=None):
        self.input_check = input_check
        self.output_check = output_check

    def _check_inputs_and_prev_state(self, inputs, states):
        if self.input_check:
            self.input_check(inputs, states)
        return inputs, states

    def _check_outputs_and_next_state(self, outputs, states):
        if self.output_check:
            self.output_check(outputs, states)
        return outputs, states

    def _initial_state(self):
        return TensorDict({"in": np.arange(1)})

    def _unroll(self, inputs: TensorDict, prev_state: TensorDict) -> UnrollOutputType:
        assert np.all(inputs["in"] == np.arange(3))
        assert np.all(prev_state["in"] == np.arange(1))
        return TensorDict({"out": np.arange(2)}), TensorDict({"out": np.arange(4)})


class NpModelImpl(Model):
    input_spec = SpecDict({"in": "h"}, h=3)
    output_spec = SpecDict({"out": "o"}, o=2)

    def __init__(self, input_check=None, output_check=None):
        self.input_check = input_check
        self.output_check = output_check

    def _check_inputs(self, inputs):
        if self.input_check:
            return self.input_check(inputs)
        return inputs

    def _check_outputs(self, outputs):
        if self.output_check:
            self.output_check(outputs)
        return outputs

    def _forward(self, inputs: TensorDict) -> ForwardOutputType:
        return TensorDict({"out": np.arange(2)})


class TestRecurrentModel(unittest.TestCase):
    def test_initial_state(self):
        """Check that the _initial state is corrected called by initial_state
        and outputs correct values."""
        output = NpRecurrentModelImpl().initial_state()
        desired = TensorDict({"in": np.arange(1)})
        for k in output.flatten().keys() | desired.flatten().keys():
            self.assertTrue(np.all(output[k] == desired[k]))

    def test_unroll(self):
        """Test that _unroll is correctly called by unroll and outputs are the
        correct values"""
        out, out_state = NpRecurrentModelImpl().unroll(
            inputs=TensorDict({"in": np.arange(3)}),
            prev_state=TensorDict({"in": np.arange(1)}),
        )
        desired, desired_state = (
            TensorDict({"out": np.arange(2)}),
            TensorDict({"out": np.arange(4)}),
        )

        for k in out.flatten().keys() | desired.flatten().keys():
            self.assertTrue(np.all(out[k] == desired[k]))
        for k in out_state.flatten().keys() | desired_state.flatten().keys():
            self.assertTrue(np.all(out_state[k] == desired_state[k]))

    def test_unroll_filter(self):
        """Test that unroll correctly filters unused data"""

        def in_check(inputs, states):
            assert "bork" not in inputs.keys() and "borkbork" not in states.keys()
            return inputs, states

        out, state = NpRecurrentModelImpl(input_check=in_check).unroll(
            inputs=TensorDict({"in": np.arange(3), "bork": np.zeros(1)}),
            prev_state=TensorDict({"in": np.arange(1), "borkbork": np.zeros(1)}),
        )

    def test_hooks(self):
        """Test that _check_inputs_and_prev_state and _check_outputs_and_prev_state
        are called during unroll"""

        class MyException(Exception):
            pass

        def exc(a, b):
            raise MyException()

        with self.assertRaises(MyException):
            NpRecurrentModelImpl(input_check=exc).unroll(
                inputs=TensorDict({"in": np.arange(3)}),
                prev_state=TensorDict({"in": np.arange(1)}),
            )

        with self.assertRaises(MyException):
            NpRecurrentModelImpl(output_check=exc).unroll(
                inputs=TensorDict({"in": np.arange(3)}),
                prev_state=TensorDict({"in": np.arange(1)}),
            )


class TestModel(unittest.TestCase):
    def test_unroll(self):
        """Test that unroll correctly calls _forward"""
        output, nullstate = NpModelImpl().unroll(
            inputs=TensorDict(), prev_state=TensorDict()
        )

        self.assertEqual(
            nullstate,
            TensorDict(),
        )
        self.assertTrue(np.all(output == np.arange(2)))

    def test_hooks(self):
        """Test that unroll correctly calls the filter functions
        before _forward"""

        class MyException(Exception):
            pass

        def exc(a):
            raise MyException()

        with self.assertRaises(MyException):
            NpModelImpl(input_check=exc).unroll(
                inputs=TensorDict({"in": np.arange(3)}),
                prev_state=TensorDict(),
            )

        with self.assertRaises(MyException):
            NpModelImpl(output_check=exc).unroll(
                inputs=TensorDict({"in": np.arange(3)}),
                prev_state=TensorDict(),
            )
