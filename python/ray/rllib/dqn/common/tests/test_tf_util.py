# tests for tf_util
import tensorflow as tf
from ray.rllib.dqn.common.tf_util import (
    function,
    initialize,
    set_value,
    single_threaded_session
)


def test_set_value():
    a = tf.Variable(42.)
    with single_threaded_session():
        set_value(a, 5)
        assert a.eval() == 5
        g = tf.get_default_graph()
        g.finalize()
        set_value(a, 6)
        assert a.eval() == 6

        # test the test
        try:
            assert a.eval() == 7
        except AssertionError:
            pass
        else:
            assert False, "assertion should have failed"


def test_function():
    tf.reset_default_graph()
    x = tf.placeholder(tf.int32, (), name="x")
    y = tf.placeholder(tf.int32, (), name="y")
    z = 3 * x + 2 * y
    lin = function([x, y], z, givens={y: 0})

    with single_threaded_session():
        initialize()

        assert lin(2) == 6
        assert lin(x=3) == 9
        assert lin(2, 2) == 10
        assert lin(x=2, y=3) == 12


def test_multikwargs():
    tf.reset_default_graph()
    x = tf.placeholder(tf.int32, (), name="x")
    with tf.variable_scope("other"):
        x2 = tf.placeholder(tf.int32, (), name="x")
    z = 3 * x + 2 * x2

    lin = function([x, x2], z, givens={x2: 0})
    with single_threaded_session():
        initialize()
        assert lin(2) == 6
        assert lin(2, 2) == 10
        expt_caught = False
        try:
            lin(x=2)
        except AssertionError:
            expt_caught = True
        assert expt_caught


if __name__ == '__main__':
    test_set_value()
    test_function()
    test_multikwargs()
