import unittest

from ray.rllib.models.base_model import (
    UnrollOutputType,
    Model,
    RecurrentModel,
    ForwardOutputType,
)
import numpy as np
from ray.rllib.models.temp_spec_classes import TensorDict, SpecDict
from ray.rllib.utils.annotations import override
from ray.rllib.utils.test_utils import check


class NpRecurrentModelImpl(RecurrentModel):
    """A numpy recurrent model for checking:
    (1) initial states
    (2) that model in/out is as expected
    (3) unroll logic
    (4) spec checking"""

    def __init__(self, input_check=None, output_check=None):
        super().__init__()
        self.input_check = input_check
        self.output_check = output_check

    @property
    @override(RecurrentModel)
    def input_spec(self):
        return SpecDict({"in": "h"}, h=3)

    @property
    @override(RecurrentModel)
    def output_spec(self):
        return SpecDict({"out": "o"}, o=2)

    @property
    @override(RecurrentModel)
    def next_state_spec(self):
        return SpecDict({"out": "i"}, i=4)

    @property
    @override(RecurrentModel)
    def prev_state_spec(self):
        return SpecDict({"in": "o"}, o=1)

    @override(RecurrentModel)
    def _update_inputs_and_prev_state(self, inputs, states):
        if self.input_check:
            self.input_check(inputs, states)
        return inputs, states

    @override(RecurrentModel)
    def _update_outputs_and_next_state(self, outputs, states):
        if self.output_check:
            self.output_check(outputs, states)
        return outputs, states

    @override(RecurrentModel)
    def _initial_state(self):
        return TensorDict({"in": np.arange(1)})

    @override(RecurrentModel)
    def _unroll(self, inputs: TensorDict, prev_state: TensorDict) -> UnrollOutputType:
        # Ensure unroll is passed the input/state as expected
        # and does not mutate/permute it in any way
        check(inputs["in"], np.arange(3))
        check(prev_state["in"], np.arange(1))
        return TensorDict({"out": np.arange(2)}), TensorDict({"out": np.arange(4)})


class NpModelImpl(Model):
    """Non-recurrent extension of NPRecurrentModelImpl

    For testing:
    (1) rollout and forward_ logic
    (2) spec checking
    """

    def __init__(self, input_check=None, output_check=None):
        super().__init__()
        self.input_check = input_check
        self.output_check = output_check

    @property
    @override(Model)
    def input_spec(self):
        return SpecDict({"in": "h"}, h=3)

    @property
    @override(Model)
    def output_spec(self):
        return SpecDict({"out": "o"}, o=2)

    @override(Model)
    def _update_inputs(self, inputs):
        if self.input_check:
            return self.input_check(inputs)
        return inputs

    @override(Model)
    def _update_outputs(self, outputs):
        if self.output_check:
            self.output_check(outputs)
        return outputs

    @override(Model)
    def _forward(self, inputs: TensorDict) -> ForwardOutputType:
        # Ensure _forward is passed the input from unroll as expected
        # and does not mutate/permute it in any way
        check(inputs["in"], np.arange(3))
        return TensorDict({"out": np.arange(2)})


class TestRecurrentModel(unittest.TestCase):
    def test_initial_state(self):
        """Check that the _initial state is corrected called by initial_state
        and outputs correct values."""
        output = NpRecurrentModelImpl().initial_state()
        desired = TensorDict({"in": np.arange(1)})
        for k in output.flatten().keys() | desired.flatten().keys():
            check(output[k], desired[k])

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
            check(out[k], desired[k])
        for k in out_state.flatten().keys() | desired_state.flatten().keys():
            check(out_state[k], desired_state[k])

    def test_unroll_filter(self):
        """Test that unroll correctly filters unused data"""

        def in_check(inputs, states):
            assert "bork" not in inputs.keys() and "borkbork" not in states.keys()
            return inputs, states

        m = NpRecurrentModelImpl(input_check=in_check)
        out, state = m.unroll(
            inputs=TensorDict({"in": np.arange(3), "bork": np.zeros(1)}),
            prev_state=TensorDict({"in": np.arange(1), "borkbork": np.zeros(1)}),
        )

    def test_hooks(self):
        """Test that _update_inputs_and_prev_state and _update_outputs_and_prev_state
        are called during unroll"""

        class MyException(Exception):
            pass

        def exc(a, b):
            raise MyException()

        with self.assertRaises(MyException):
            m = NpRecurrentModelImpl(input_check=exc)
            m.unroll(
                inputs=TensorDict({"in": np.arange(3)}),
                prev_state=TensorDict({"in": np.arange(1)}),
            )

        with self.assertRaises(MyException):
            m = NpRecurrentModelImpl(output_check=exc)
            m.unroll(
                inputs=TensorDict({"in": np.arange(3)}),
                prev_state=TensorDict({"in": np.arange(1)}),
            )


class TestModel(unittest.TestCase):
    def test_unroll(self):
        """Test that unroll correctly calls _forward. The outputs
        should be as expected."""
        m = NpModelImpl()
        output, nullstate = m.unroll(
            inputs=TensorDict({"in": np.arange(3)}), prev_state=TensorDict()
        )

        self.assertEqual(
            nullstate,
            TensorDict(),
        )
        check(output["out"], np.arange(2))

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


if __name__ == "__main__":
    import pytest
    import sys

    sys.exit(pytest.main(["-v", __file__]))
