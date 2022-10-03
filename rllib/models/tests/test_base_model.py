import unittest

from ray.rllib.models.base_model import (
    SpecDict,
    UnrollOutputType,
    TensorDict,
    Model,
    RecurrentModel,
    ForwardOutputType,
)


class NoopRecurrentModelImpl(RecurrentModel):
    input_spec = output_spec = next_state_spec = prev_state_spec = SpecDict()

    def _initial_state(self):
        return TensorDict()

    def _unroll(self, inputs: TensorDict, prev_state: TensorDict) -> UnrollOutputType:
        return (TensorDict(), TensorDict())


class NoopModelImpl(Model):
    input_spec = output_spec = SpecDict()

    def _forward(self, inputs: TensorDict) -> ForwardOutputType:
        return TensorDict()


class TestNoopRecurrentModel(unittest.TestCase):
    def test_initial_state(self):
        self.assertEqual(NoopRecurrentModelImpl().initial_state(), TensorDict())

    def test_unroll(self):
        self.assertEqual(
            NoopRecurrentModelImpl().unroll(
                inputs=TensorDict(), prev_state=TensorDict()
            ),
            (TensorDict(), TensorDict()),
        )

    def test_hooks(self):
        class MyException(Exception):
            pass

        def exc(a, b):
            raise MyException()

        m = NoopRecurrentModelImpl()
        m._check_inputs_and_prev_state = exc

        with self.assertRaises(MyException):
            m.unroll(TensorDict(), TensorDict())

        m = NoopRecurrentModelImpl()
        m._check_outputs_and_next_state = exc

        with self.assertRaises(MyException):
            m.unroll(TensorDict(), TensorDict())


class TestNoopModel(unittest.TestCase):
    def test_unroll(self):
        self.assertEqual(
            NoopModelImpl().unroll(inputs=TensorDict(), prev_state=TensorDict()),
            TensorDict(),
        )
