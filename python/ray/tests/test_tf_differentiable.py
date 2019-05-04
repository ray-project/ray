from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from functools import reduce
import operator

import numpy as np
import pytest
import tensorflow as tf

import ray
from ray.experimental.tf_utils import tf_differentiable


@pytest.fixture
def actors():
    """Builds actors for testing."""

    @ray.remote
    class RayActor(object):
        """Ray Actor with TF differentiable functions."""

        def __init__(self):
            # Enable eager execution.
            tf.enable_eager_execution()

        # Returns nested TF value (unsupported for now).
        @tf_differentiable
        def nested_tf_return(self):
            return {"a": [tf.constant(1.0)]}

        # "Identity" functions.
        @tf_differentiable
        def identity(self, x):
            return x

        @ray.method(num_return_vals=8)
        @tf_differentiable
        def identity_mixed(self, i_1, i_2, i_3, i_4, i_5):
            return i_4, i_4, i_5, i_1, i_1, i_3, i_4, i_2

        @ray.method(num_return_vals=8)
        @tf_differentiable
        def identity_mixed_kw(self, i_1, i_2, i_3=5, i_4=2, i_5=3):
            return i_4, i_4, i_5, i_1, i_1, i_3, i_4, i_2

        # No inputs.
        @ray.method(num_return_vals=2)
        @tf_differentiable
        def return_5(self):
            return 5.0, tf.constant(5.0)

        # Single input, single output.
        @tf_differentiable
        def square(self, x):
            return x**2

        @tf_differentiable
        def cube(self, x):
            return x**3

        @tf_differentiable
        def double(self, x):
            return 2 * x

        # Multiple inputs, single output.
        @tf_differentiable
        def sum(self, *inputs):
            return sum(inputs)

        @tf_differentiable
        def prod(self, *inputs):
            return reduce(operator.mul, inputs)

        @tf_differentiable
        def sum_square_cube(self, x, y):
            return x**2 + y**3

        # Single input, multiple outputs.
        @ray.method(num_return_vals=2)
        @tf_differentiable
        def single_in_square_cube(self, x):
            return x**2, x**3

        # Multiple inputs, multiple outputs.
        @ray.method(num_return_vals=2)
        @tf_differentiable
        def two_in_square_cube_v1(self, x, y):
            return x**2, y**3

        @ray.method(num_return_vals=2)
        @tf_differentiable
        def two_in_square_cube_v2(self, x, y):
            return x**2, x**3

        @ray.method(num_return_vals=5)
        @tf_differentiable
        def f(self, x, y, z):
            return x**2, y**3, x**4, 2 * z, x * y

        @ray.method(num_return_vals=2)
        @tf_differentiable
        def g(self, x, y, z):
            return x + y + z, x * y * z

        # Kwargs.
        @tf_differentiable
        def kw_power(self, x, n=2):
            return x**n

        @ray.method(num_return_vals=2)
        @tf_differentiable
        def kw_power_mul(self, x, y, x_pow=2, y_mul=2):
            return x**x_pow, y * y_mul

    class DummyActor(object):
        """Regular Python version of above Ray actor."""

        def __init__(self):
            # Enable eager execution.
            tf.enable_eager_execution()

        # "Identity" functions.
        def identity(self, x):
            return x

        def identity_mixed(self, i_1, i_2, i_3, i_4, i_5):
            return i_4, i_4, i_5, i_1, i_1, i_3, i_4, i_2

        def identity_mixed_kw(self, i_1, i_2, i_3=5, i_4=2, i_5=3):
            return i_4, i_4, i_5, i_1, i_1, i_3, i_4, i_2

        # Single input, single output.
        def square(self, x):
            return x**2

        def cube(self, x):
            return x**3

        def double(self, x):
            return 2 * x

        # No inputs.
        def return_5(self):
            return 5.0, tf.constant(5.0)

        # Multiple inputs, single output.
        def sum(self, *inputs):
            return sum(inputs)

        def prod(self, *inputs):
            return reduce(operator.mul, inputs)

        def sum_square_cube(self, x, y):
            return x**2 + y**3

        # Single input, multiple outputs.
        def single_in_square_cube(self, x):
            return x**2, x**3

        # Multiple inputs, multiple outputs.
        def two_in_square_cube_v1(self, x, y):
            return x**2, y**3

        def two_in_square_cube_v2(self, x, y):
            return x**2, x**3

        def f(self, x, y, z):
            return x**2, y**3, x**4, 2 * z, x * y

        def g(self, x, y, z):
            return x + y + z, x * y * z

        # Kwargs.
        def kw_power(self, x, n=2):
            return x**n

        def kw_power_mul(self, x, y, x_pow=2, y_mul=2):
            return x**x_pow, y * y_mul

    return RayActor.remote(), DummyActor()


def check_outputs(out_1, out_2):
    """Verifies that two outputs (that may contain TF eager
       tensors) are equivalent.

    Args:
        out_1 (obj): The first output to compare.
        out_2 (obj): The second output to compare.
    """

    # This is needed because `ray.get` returns a list.
    if isinstance(out_1, list) and isinstance(out_2, tuple):
        out_2 = list(out_2)
    elif isinstance(out_1, tuple) and isinstance(out_2, list):
        out_1 = list(out_1)
    else:
        assert type(out_1) == type(out_2)

    if isinstance(out_1, (tf.Tensor, tf.Variable)):
        # `tf.Tensor` or `tf.Variable`.
        assert out_1.dtype == out_2.dtype
        assert np.all(out_1.numpy() == out_2.numpy())
    elif isinstance(out_1, (list, tuple, set)):
        # Recursively compare the elements.
        for elem_1, elem_2 in zip(out_1, out_2):
            check_outputs(elem_1, elem_2)
    elif isinstance(out_1, dict):
        # Recursively compare the elements.
        for key, value in out_1.items():
            assert key in out_2.keys()
            check_outputs(value, out_2[key])
    else:
        # Everything else.
        assert out_1 == out_2


# Test input validation.
# Test nested TF arg.
def test_nested_tf_arg(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(4.0)
    nested = [0.0, {"a": [-7, (x, )]}]

    with pytest.raises(
            ValueError,
            match="Cannot have nested TF values in arguments or return values."
    ):
        ray.get(ray_actor.identity.remote(nested))


# Test nested TF return value.
def test_nested_tf_return(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    # Because this error happens in the remote task, it comes wrapped in
    # `ray.exceptions.RayTaskError`.
    with pytest.raises(
            ray.exceptions.RayTaskError,
            match=("ValueError: Cannot have nested TF "
                   "values in arguments or return values.")):
        ray.get(ray_actor.nested_tf_return.remote())


# Test that decorated function behaves like a normal Python function.


# Test behavior with no args.
def test_func_behavior_no_args(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    out_hat = ray.get(ray_actor.return_5.remote())
    out = dummy_actor.return_5()

    check_outputs(out_hat, out)


# Test behavior with native Python args.
def test_func_behavior_simple(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = [1, 2, 3]

    out_hat = ray.get(ray_actor.identity.remote(x))
    out = dummy_actor.identity(x)

    check_outputs(out_hat, out)


# Test behavior with heterogenous TF + native Python args.
def test_func_behavior_complex(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    i_1 = tf.Variable(2.0, dtype=tf.float64)
    i_2 = [{"e": [-1, 2, 3]}, {"a": 2}, (1, )]
    i_3 = tf.constant([5.0, 6.0, -7.0], dtype=tf.float32)
    i_4 = [1, 2, i_2]
    i_5 = {"a": 5, "b": [6, 7, 8]}

    out_hat = ray.get(ray_actor.identity_mixed.remote(i_1, i_2, i_3, i_4, i_5))
    out = dummy_actor.identity_mixed(i_1, i_2, i_3, i_4, i_5)

    check_outputs(out_hat, out)


# Test behavior with heterogenous TF + native Python args and kwargs.
def test_func_behavior_complex_kwargs(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    i_1 = tf.Variable(2.0, dtype=tf.float64)
    i_2 = [{"e": [-1, 2, 3]}, {"a": 2}, (1, )]
    i_3 = tf.constant([5.0, 6.0, -7.0], dtype=tf.float32)
    i_4 = {"a": 5, "b": [6, 7, 8]}

    out_hat = ray.get(
        ray_actor.identity_mixed_kw.remote(i_1, i_2, i_3=i_3, i_4=i_4))
    out = dummy_actor.identity_mixed_kw(i_1, i_2, i_3=i_3, i_4=i_4)

    check_outputs(out_hat, out)


# Gradient/output correctness tests.


# Single input/output w/ linear computation graph
# custom op w/ single input & single output.
def test_single_op_single_in_single_out(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    with tf.GradientTape() as t:
        out = x
        out = ray_actor.square.remote(out)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        out = x
        out = dummy_actor.square(out)
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ single input & single output both preceded and
# followed by TF ops.
def test_single_op_single_in_single_out_tf_sandwich(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    with tf.GradientTape() as t:
        out = x
        out = 2 * out + out**2
        out = ray_actor.square.remote(out)
        out = ray.get(out)
        out_hat = out**3
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        out = x
        out = 2 * out + out**2
        out = dummy_actor.square(out)
        out = out**3
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Multiple custom ops w/ single input & single output.
def test_multiple_ops_single_in_single_out(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    with tf.GradientTape() as t:
        out = x
        out = ray_actor.double.remote(out)
        out = ray_actor.square.remote(out)
        out = ray_actor.cube.remote(out)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        out = x
        out = dummy_actor.double(out)
        out = dummy_actor.square(out)
        out = dummy_actor.cube(out)
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Multiple custom ops w/ single input & single output
# w/ ray fetches in-between.
def test_multiple_ops_single_in_single_out_inter_fetches(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    with tf.GradientTape() as t:
        out = x
        out = ray_actor.double.remote(out)
        out = ray.get(out)
        out = ray_actor.square.remote(out)
        out = ray.get(out)
        out = ray_actor.cube.remote(out)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        out = x
        out = dummy_actor.double(out)
        out = dummy_actor.square(out)
        out = dummy_actor.cube(out)
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Single input/output w/ non-linear computation graph


# single custom op w/ single input & single output and
# resulting `ray.ObjectID` reused multiple times.
def test_single_op_single_in_single_out_reuse_result(ray_start_regular,
                                                     actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    with tf.GradientTape() as t:
        out = x
        out = ray_actor.square.remote(out)
        out_1 = ray.get(out)
        out_2 = ray.get(out)
        out_hat = out_1 + out_2
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        out = x
        out = dummy_actor.square(out)
        out = out + out
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Multiple custom ops w/ single input & single output with reused
# intermediate results.
def test_multiple_ops_single_in_single_out_reuse_inter_results(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    with tf.GradientTape() as t:
        out = x
        out = ray_actor.square.remote(out)
        stream_1 = ray_actor.cube.remote(out)
        stream_2 = ray_actor.double.remote(out)
        out_1 = ray.get(stream_1)
        out_2 = ray.get(stream_2)
        out_hat = out_1 + out_2
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        out = x
        out = dummy_actor.square(out)
        stream_1 = dummy_actor.cube(out)
        stream_2 = dummy_actor.double(out)
        out = stream_1 + stream_2
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


def test_multiple_ops_single_in_single_out_large_v1(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable([2.0, 1.0, -2.0, 4.0], dtype=tf.float64)
    y = tf.Variable(-3.0, dtype=tf.float64)
    z = tf.Variable(4.0, dtype=tf.float64)
    with tf.GradientTape() as t:
        i_1 = x**2 + 2 * y
        i_1 = ray_actor.cube.remote(i_1)
        i_2 = ray_actor.double.remote(x)
        i_3 = ray.get(ray_actor.double.remote(i_2)) + ray.get(i_1)
        i_4 = ray.get(i_1) + ray.get(i_2) + i_3
        i_5 = ray_actor.square.remote(i_4)
        out_hat = ray.get(i_5)**2 + ray.get(i_1)
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1 = x**2 + 2 * y
        i_1 = dummy_actor.cube(i_1)
        i_2 = dummy_actor.double(x)
        i_3 = dummy_actor.double(i_2) + i_1
        i_4 = i_1 + i_2 + i_3
        i_5 = dummy_actor.square(i_4)
        out = i_5**2 + i_1
    grad = t.gradient(out, [x, y, z])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


def test_multiple_ops_single_in_single_out_large_v2(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    w = tf.Variable(-2.0, dtype=tf.float64)
    x = tf.Variable(3.0, dtype=tf.float64)
    y = tf.Variable(4.0, dtype=tf.float64)
    z = tf.Variable(-5.0, dtype=tf.float64)
    with tf.GradientTape() as t:
        i_1 = ray_actor.double.remote(w * x)
        i_2 = ray_actor.cube.remote(i_1)
        i_3 = ray_actor.square.remote(i_1)
        i_4 = w * x * ray.get(i_2) * ray.get(i_3)
        i_5 = ray.get(ray_actor.double.remote(i_4)) + ray.get(
            ray_actor.square.remote(i_3))
        i_6 = ray_actor.double.remote(i_5 * z * y)
        out_hat = ray.get(i_6) + w * ray.get(i_6) + x * ray.get(i_3)
    grad_hat = t.gradient(out_hat, [w, x, y, z])

    with tf.GradientTape() as t:
        i_1 = dummy_actor.double(w * x)
        i_2 = dummy_actor.cube(i_1)
        i_3 = dummy_actor.square(i_1)
        i_4 = w * x * i_2 * i_3
        i_5 = dummy_actor.double(i_4) + dummy_actor.square(i_3)
        i_6 = dummy_actor.double(i_5 * z * y)
        out = i_6 + w * i_6 + x * i_3
    grad = t.gradient(out, [w, x, y, z])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Multiple inputs/outputs


# custom op w/ single input & multiple outputs.
def test_single_op_single_in_multiple_out(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    with tf.GradientTape() as t:
        out = x
        out = ray_actor.single_in_square_cube.remote(out)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        out = x
        out = dummy_actor.single_in_square_cube(out)
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ multiple inputs & single output.
def test_single_op_multiple_in_single_out(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    with tf.GradientTape() as t:
        out = ray_actor.sum_square_cube.remote(x, y)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, [x, y])

    with tf.GradientTape() as t:
        out = dummy_actor.sum_square_cube(x, y)
    grad = t.gradient(out, [x, y])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ multiple inputs & outputs.
def test_single_op_multiple_in_multiple_out(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    with tf.GradientTape() as t:
        out = ray_actor.two_in_square_cube_v1.remote(x, y)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, [x, y])

    with tf.GradientTape() as t:
        out = dummy_actor.two_in_square_cube_v1(x, y)
    grad = t.gradient(out, [x, y])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ multiple inputs & outputs and heterogenous
# input and output dtypes.
def test_single_op_multiple_in_multiple_out_hetero_dtype(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0, dtype=tf.float32)
    y = tf.Variable(3.0, dtype=tf.float64)
    with tf.GradientTape() as t:
        out = ray_actor.two_in_square_cube_v1.remote(x, y)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, [x, y])

    with tf.GradientTape() as t:
        out = dummy_actor.two_in_square_cube_v1(x, y)
    grad = t.gradient(out, [x, y])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ multiple inputs & outputs and unused source.
@pytest.mark.skip("Gradient w.r.t unused input is 0.0 instead of None.")
def test_single_op_multiple_in_multiple_out_unused_source(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    with tf.GradientTape() as t:
        i_1, i_2 = ray_actor.two_in_square_cube_v1.remote(x, y)
        out_hat = ray.get(i_1)
    grad_hat = t.gradient(out_hat, [x, y])

    with tf.GradientTape() as t:
        i_1, i_2 = dummy_actor.two_in_square_cube_v1(x, y)
        out = i_1
    grad = t.gradient(out, [x, y])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ multiple inputs & outputs where one input of op is never used.
def test_single_op_multiple_in_multiple_out_unused_input(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    with tf.GradientTape() as t:
        out = ray_actor.two_in_square_cube_v2.remote(x, y)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, [x, y])

    with tf.GradientTape() as t:
        out = dummy_actor.two_in_square_cube_v2(x, y)
    grad = t.gradient(out, [x, y])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ multiple inputs & outputs w/ only one `ray.ObjectID` fetched.
def test_single_op_multiple_in_multiple_out_single_fetch(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    z = tf.Variable(4.0)
    with tf.GradientTape() as t:
        out = ray_actor.f.remote(x, y, z)
        out_hat = ray.get(out[0])
    grad_hat = t.gradient(out_hat, [x])

    with tf.GradientTape() as t:
        out = dummy_actor.f(x, y, z)
        out = out[0]
    grad = t.gradient(out, [x])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ multiple inputs & outputs both preceded and
# followed by tf ops w/ two `ray.ObjectID`s fetched.
def test_single_op_multiple_in_multiple_out_tf_sandwich_double_fetch(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    z = tf.Variable(4.0)
    with tf.GradientTape() as t:
        i_1 = x * y
        i_2 = 2 * z
        i_3 = y**2
        i_4 = ray_actor.f.remote(i_1, i_2, i_3)
        out_1, out_2 = ray.get(i_4[:2])
        out_1 *= 2
        out_2 **= 2
        out_hat = [out_1, out_2]
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1 = x * y
        i_2 = 2 * z
        i_3 = y**2
        i_4 = dummy_actor.f(i_1, i_2, i_3)
        out_1, out_2 = i_4[:2]
        out_1 *= 2
        out_2 **= 2
        out = [out_1, out_2]
    grad = t.gradient(out, [x])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Multiple custom ops w/ multiple inputs and outputs.
def test_multiple_ops_multiple_in_multiple_out(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    z = tf.Variable(4.0)
    with tf.GradientTape() as t:
        i_1, i_2, i_3, i_4, i_5 = ray_actor.f.remote(x, y, z)
        i_6, i_7 = ray_actor.g.remote(i_1, i_3, i_5)
        i_8 = ray_actor.sum_square_cube.remote(i_6, z)
        out_hat = ray.get(i_8)
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1, i_2, i_3, i_4, i_5 = dummy_actor.f(x, y, z)
        i_6, i_7 = dummy_actor.g(i_1, i_3, i_5)
        i_8 = dummy_actor.sum_square_cube(i_6, z)
        out = i_8
    grad = t.gradient(out, [x, y, z])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Multiple custom ops w/ multiple inputs and outputs w/ ray fetches in-between.
def test_multiple_ops_multiple_in_multiple_out_inter_fetches(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    z = tf.Variable(4.0)
    with tf.GradientTape() as t:
        i_1, i_2, i_3, i_4, i_5 = ray_actor.f.remote(x, y, z)
        i_1, i_5 = ray.get(i_1), ray.get(i_5)
        i_6, i_7 = ray_actor.g.remote(i_1, i_3, i_5)
        i_6 = ray.get(i_6)
        i_8 = ray_actor.sum_square_cube.remote(i_6, z)
        out_hat = ray.get(i_8)
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1, i_2, i_3, i_4, i_5 = dummy_actor.f(x, y, z)
        i_6, i_7 = dummy_actor.g(i_1, i_3, i_5)
        i_8 = dummy_actor.sum_square_cube(i_6, z)
        out = i_8
    grad = t.gradient(out, [x, y, z])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Multiple custom ops w/ multiple inputs and outputs w/ intertwined TF
# ops and not all ray.ObjectIDs (intermediate and final) used.
def test_multiple_ops_multiple_in_multiple_out_tf_intertwined_partial_use(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.5, dtype=tf.float64)
    y = tf.Variable(3.0, dtype=tf.float64)
    z = tf.Variable(-4.0, dtype=tf.float64)
    with tf.GradientTape() as t:
        i_1 = x * y + z
        i_2 = z * 3
        i_3, i_4, i_5, i_6, i_7 = ray_actor.f.remote(x, i_1, i_2)
        i_8, i_9 = ray_actor.g.remote(i_3, i_5, i_7)
        i_8 = ray.get(i_8)**2
        i_10, i_11 = ray_actor.two_in_square_cube_v2.remote(i_8, i_8)
        out_hat = ray.get(i_10)**2
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1 = x * y + z
        i_2 = z * 3
        i_3, i_4, i_5, i_6, i_7 = dummy_actor.f(x, i_1, i_2)
        i_8, i_9 = dummy_actor.g(i_3, i_5, i_7)
        i_8 = i_8**2
        i_10, i_11 = dummy_actor.two_in_square_cube_v2(i_8, i_8)
        out = i_10**2
    grad = t.gradient(out, [x, y, z])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


def test_multiple_ops_multiple_in_multiple_out_large_v1(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0, dtype=tf.float64)
    y = tf.Variable(3.0, dtype=tf.float64)
    z = tf.Variable(4.0, dtype=tf.float64)

    with tf.GradientTape() as t:
        i_1 = x * y * z
        i_1 = i_1 / 10.0
        i_2 = (z / 1.0) * 3.0 * ray.get(ray_actor.prod.remote(x, y, y))
        i_2 = i_2 / 10.0
        i_3 = i_2 / 3.0
        i_4 = ray.get(
            ray_actor.prod.remote(*ray_actor.f.remote(i_1, i_2, i_3)))
        i_5 = i_4 / 15.0
        i_6 = ray_actor.sum.remote(i_4)
        i_7 = ray.get(ray_actor.prod.remote(i_3, i_3, i_4, i_5, i_6))
        i_7 = i_7 / 15.0
        i_8, _ = ray_actor.g.remote(i_3, i_5, i_7)
        i_9 = ray_actor.prod.remote(i_8)
        _, i_10 = ray_actor.two_in_square_cube_v2.remote(i_8, i_9)
        i_11 = ray_actor.prod.remote(i_10, x)
        out_hat = ray.get(i_11)
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1 = x * y * z
        i_1 = i_1 / 10.0
        i_2 = (z / 1.0) * 3.0 * dummy_actor.prod(x, y, y)
        i_2 = i_2 / 10.0
        i_3 = i_2 / 3.0
        i_4 = dummy_actor.prod(*dummy_actor.f(i_1, i_2, i_3))
        i_5 = i_4 / 15.0
        i_6 = dummy_actor.sum(i_4)
        i_7 = dummy_actor.prod(i_3, i_3, i_4, i_5, i_6)
        i_7 = i_7 / 15.0
        i_8, _ = dummy_actor.g(i_3, i_5, i_7)
        i_9 = dummy_actor.prod(i_8)
        _, i_10 = dummy_actor.two_in_square_cube_v2(i_8, i_9)
        i_11 = dummy_actor.prod(i_10, x)
        out = i_11
    grad = t.gradient(out, [x, y, z])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


def test_multiple_ops_multiple_in_multiple_out_large_v2(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable([2.0, 2.0, 2.0], dtype=tf.float64)
    y = tf.Variable(3.0, dtype=tf.float64)
    z = tf.Variable([4.0, 4.0, 4.0], dtype=tf.float64)

    with tf.GradientTape() as t:
        i_1 = x * y + z**2.0
        i_1 = i_1 / 10.0
        i_2 = (z / 1.0) * 3.0 * ray.get(ray_actor.prod.remote(x, y, y))
        i_2 = i_2 / 5.0
        i_3 = i_2 / 3.0
        i_4, _, i_5, _, i_6 = ray_actor.f.remote(i_1, i_2, i_3)
        i_7 = ray.get(ray_actor.prod.remote(i_1, i_3, i_4, i_5, i_6))
        i_7 = i_7 / 8.0
        i_8, _ = ray_actor.g.remote(i_3, i_5, i_7)
        i_8 = ray.get(i_8)**2.0 / 5.0
        _, i_9 = ray_actor.two_in_square_cube_v2.remote(i_8, i_4)
        i_9 = ray.get(i_9) / 5.0
        i_10 = ray_actor.prod.remote(i_9, x)
        out_hat = ray.get(i_10)
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1 = x * y + z**2.0
        i_1 = i_1 / 10.0
        i_2 = (z / 1.0) * 3.0 * dummy_actor.prod(x, y, y)
        i_2 = i_2 / 5.0
        i_3 = i_2 / 3.0
        i_4, _, i_5, _, i_6 = dummy_actor.f(i_1, i_2, i_3)
        i_7 = dummy_actor.prod(i_1, i_3, i_4, i_5, i_6)
        i_7 = i_7 / 8.0
        i_8, _ = dummy_actor.g(i_3, i_5, i_7)
        i_8 = i_8**2.0 / 5.0
        _, i_9 = dummy_actor.two_in_square_cube_v2(i_8, i_4)
        i_9 = i_9 / 5.0
        i_10 = dummy_actor.prod(i_9, x)
        out = i_10
    grad = t.gradient(out, [x, y, z])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Kwargs


# Custom op w/ single input & single output w/ kwarg.
def test_kwargs_single_in_single_out(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(5.0)

    with tf.GradientTape() as t:
        i_1 = ray_actor.kw_power.remote(x, n=5)
        out_hat = ray.get(i_1)
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        i_1 = dummy_actor.kw_power(x, n=5)
        out = i_1
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ single input & single output w/ kwarg & using default kwarg.
def test_kwargs_single_in_single_out_default(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(5.0, dtype=tf.float64)

    with tf.GradientTape() as t:
        i_1 = ray_actor.kw_power.remote(x)
        out_hat = ray.get(i_1)
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        i_1 = dummy_actor.kw_power(x)
        out = i_1
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ single input & single output w/ kwarg & using native
# Python type for arg & using TF type for kwarg.
def test_kwargs_single_in_single_out_tensor_kwarg(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    n = tf.Variable(5.0)

    with tf.GradientTape() as t:
        i_1 = ray_actor.kw_power.remote(2, n=n)
        out_hat = ray.get(i_1)
    grad_hat = t.gradient(out_hat, n)

    with tf.GradientTape() as t:
        i_1 = dummy_actor.kw_power(2, n=n)
        out = i_1
    grad = t.gradient(out, n)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ single input & single output w/ kwarg & using TF type
# for arg & using TF type for kwarg.
def test_kwargs_single_in_single_out_tensor_arg_tensor_kwarg(
        ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(5.0)
    n = tf.Variable(5.0)

    with tf.GradientTape() as t:
        i_1 = ray_actor.kw_power.remote(x, n=n)
        out_hat = ray.get(i_1)
    grad_hat = t.gradient(out_hat, [x, n])

    with tf.GradientTape() as t:
        i_1 = dummy_actor.kw_power(x, n=n)
        out = i_1
    grad = t.gradient(out, [x, n])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ single input & single output w/ kwarg & using
# output of custom op for kwarg.
def test_kwargs_single_in_single_out_tf_obj_id_kwarg(ray_start_regular,
                                                     actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(5.0)
    y = tf.Variable(2.0)

    with tf.GradientTape() as t:
        n = ray_actor.prod.remote(y, y)
        i_1 = ray_actor.kw_power.remote(x, n=n)
        out_hat = ray.get(i_1)
    grad_hat = t.gradient(out_hat, x, y)

    with tf.GradientTape() as t:
        n = dummy_actor.prod(y, y)
        i_1 = dummy_actor.kw_power(x, n=n)
        out = i_1
    grad = t.gradient(out, x, y)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ multiple inputs and multiple outputs w/ out-of-order kwargs.
def test_kwargs_multiple_in_multiple_out_out_of_order(ray_start_regular,
                                                      actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(4.0)
    y = tf.Variable(-3.0)

    with tf.GradientTape() as t:
        i_1, i_2 = ray_actor.kw_power_mul.remote(x, y, y_mul=x, x_pow=y)
        out_hat = ray.get(ray_actor.prod.remote(i_1, i_2))
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        i_1, i_2 = dummy_actor.kw_power_mul(x, y, y_mul=x, x_pow=y)
        out = dummy_actor.prod(i_1, i_2)
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Custom op w/ multiple inputs and multiple outputs w/ TF
# ops before and after.
def test_kwargs_multiple_in_multiple_out_tf_sandwich(ray_start_regular,
                                                     actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(4.0)
    y = tf.Variable(-3.0)

    with tf.GradientTape() as t:
        y_mul = x * y
        i_1, i_2 = ray_actor.kw_power_mul.remote(x, y, x_pow=3, y_mul=y_mul)
        i_3 = ray.get(ray_actor.prod.remote(i_1, i_2))
        i_3 *= 5.0
        out_hat = i_3
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        y_mul = x * y
        i_1, i_2 = dummy_actor.kw_power_mul(x, y, x_pow=3, y_mul=y_mul)
        i_3 = dummy_actor.prod(i_1, i_2)
        i_3 *= 5.0
        out = i_3
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


def test_kwargs_large(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable([2.0, 2.0, 2.0], dtype=tf.float64)
    y = tf.Variable(3.0, dtype=tf.float64)
    z = tf.Variable([4.0, 4.0, 4.0], dtype=tf.float64)

    with tf.GradientTape() as t:
        i_1 = (x * y + z**2.0) / 10.0
        i_2, i_3 = ray_actor.kw_power_mul.remote(y, i_1, x_pow=y, y_mul=y)
        i_4 = ray_actor.prod.remote(i_2, i_3)
        i_5 = ray_actor.kw_power.remote(y, n=i_4)
        i_6 = (z / 1.0) * 3.0 * ray.get(ray_actor.prod.remote(x, y, y))
        i_7 = ray.get(i_2) / 5.0
        i_8 = ray.get(i_2) / 3.0
        i_9, _, i_10, _, i_11 = ray_actor.f.remote(i_6, i_7, i_8)
        i_12, i_13 = ray_actor.kw_power_mul.remote(
            i_11, i_7, x_pow=1.0, y_mul=i_4)
        _, i_14 = ray_actor.two_in_square_cube_v2.remote(i_8, i_12)
        i_15 = ray.get(i_14) / 5.0
        i_16 = ray_actor.prod.remote(i_15, i_5)
        out_hat = ray.get(i_16)
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1 = (x * y + z**2.0) / 10.0
        i_2, i_3 = dummy_actor.kw_power_mul(y, i_1, x_pow=y, y_mul=y)
        i_4 = dummy_actor.prod(i_2, i_3)
        i_5 = dummy_actor.kw_power(y, n=i_4)
        i_6 = (z / 1.0) * 3.0 * dummy_actor.prod(x, y, y)
        i_7 = i_2 / 5.0
        i_8 = i_2 / 3.0
        i_9, _, i_10, _, i_11 = dummy_actor.f(i_6, i_7, i_8)
        i_12, i_13 = dummy_actor.kw_power_mul(i_11, i_7, x_pow=1.0, y_mul=i_4)
        _, i_14 = dummy_actor.two_in_square_cube_v2(i_8, i_12)
        i_15 = i_14 / 5.0
        i_16 = dummy_actor.prod(i_15, i_5)
        out = i_16
    grad = t.gradient(out, [x, y, z])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Test computation graph with native Python types mixed in.


def test_grad_mixed_args_v1(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    i_1 = tf.Variable(2.0)
    i_2 = [{"e": [-1, 2, 3]}, {"a": 2}, (1, )]
    i_3 = tf.constant([5.0, 6.0, -7.0])
    i_4 = [1, 2, i_2]
    i_5 = {"a": 5, "b": [6, 7, 8]}

    with tf.GradientTape() as t:
        in_1 = 2 * i_1 + i_3
        _, _, _, in_3, _, in_4, in_5, in_6 = ray.get(
            ray_actor.identity_mixed.remote(in_1, i_2, in_1, i_4, i_5))
        in_7 = 2 * ray.get(ray_actor.prod.remote(in_3, in_4, in_6[0]["e"][0]))
        out_hat = [in_7, in_5, in_6]
    grad_hat = t.gradient(out_hat[0], [i_1, i_3])

    with tf.GradientTape() as t:
        in_1 = 2 * i_1 + i_3
        _, _, _, in_3, _, in_4, in_5, in_6 = dummy_actor.identity_mixed(
            in_1, i_2, in_1, i_4, i_5)
        in_7 = 2 * dummy_actor.prod(in_3, in_4, in_6[0]["e"][0])
        out = [in_7, in_5, in_6]
    grad = t.gradient(out[0], [i_1, i_3])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


def test_grad_mixed_args_v2(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    i_1 = tf.Variable(2.0)
    i_2 = [{"e": [-1, 2, 3]}, {"a": 2}, (1, )]
    i_3 = tf.constant([5.0, 6.0, -7.0])
    i_4 = [1, 2, i_2]
    i_5 = {"a": 5, "b": [6, 7, 8]}

    with tf.GradientTape() as t:
        in_1 = 2 * i_1 + i_3
        _, _, _, in_3, _, in_4, in_5, in_6 = ray_actor.identity_mixed.remote(
            in_1, i_2, in_1, i_4, i_5)
        in_7 = ray.get(in_6)
        in_8 = ray_actor.prod.remote(in_3, in_4, in_7[0]["e"][0])
        in_9 = ray.get(ray_actor.double.remote(in_5))
        if in_9[1] > 1:
            in_10, _ = ray.get(
                ray_actor.two_in_square_cube_v2.remote(in_3, in_8))
        else:
            _, in_10 = ray.get(
                ray_actor.two_in_square_cube_v1.remote(in_3, in_8))
        in_11 = ray.get(in_6)
        out_hat = [in_10, in_9, in_11]
    grad_hat = t.gradient(out_hat[0], [i_1, i_3])

    with tf.GradientTape() as t:
        in_1 = 2 * i_1 + i_3
        _, _, _, in_3, _, in_4, in_5, in_6 = dummy_actor.identity_mixed(
            in_1, i_2, in_1, i_4, i_5)
        in_7 = in_6
        in_8 = dummy_actor.prod(in_3, in_4, in_7[0]["e"][0])
        in_9 = dummy_actor.double(in_5)
        if in_9[1] > 1:
            in_10, _ = dummy_actor.two_in_square_cube_v2(in_3, in_8)
        else:
            _, in_10 = dummy_actor.two_in_square_cube_v1(in_3, in_8)
        in_11 = in_6
        out = [in_10, in_9, in_11]
    grad = t.gradient(out[0], [i_1, i_3])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


# Test gradient through identity function.
# This is an interesting case to consider because our gradient targets
# in the backward pass can now be `tf.Variable`s.


@pytest.mark.skip("Not sure why backwards pass doesn't get called.")
def test_grad_identity_v1(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    x = tf.Variable(2.0)

    with tf.GradientTape() as t:
        out_hat = ray.get(ray_actor.identity.remote(x))
        out_hat = 1.0 * out_hat
    grad_hat = t.gradient(out_hat, x)

    with tf.GradientTape() as t:
        out = dummy_actor.identity(x)
        out = 1.0 * out
    grad = t.gradient(out, x)

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)


@pytest.mark.skip(
    "This will cause us to try to use a `tf.Variable` as a target, "
    "which is not supported. The reason this happens is because of how "
    "we take intermediate gradients for each function and not one end-to-end "
    "gradient (which is what the correct TF implementation does).")
def test_grad_identity_v2(ray_start_regular, actors):
    ray_actor, dummy_actor = actors

    i_1 = tf.Variable(2.0)
    i_2 = 1.0
    i_3 = tf.constant([5.0, 6.0, -7.0])
    i_4 = 1.0
    i_5 = 1.0

    with tf.GradientTape() as t:
        out_hat = ray.get(
            ray_actor.identity_mixed.remote(i_1, i_2, i_3, i_4, i_5))[5]
    grad_hat = t.gradient(out_hat, [i_1, i_3])

    with tf.GradientTape() as t:
        out = dummy_actor.identity_mixed(i_1, i_2, i_3, i_4, i_5)[5]
    grad = t.gradient(out, [i_1, i_3])

    check_outputs(out_hat, out)
    check_outputs(grad_hat, grad)
