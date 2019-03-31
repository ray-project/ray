from functools import reduce

import pytest
import tensorflow as tf
import numpy as np

import ray
from ray.experimental.tf_utils import tf_differentiable

@pytest.fixture
def start_ray():
    ray.init()

    yield None

    # teardown
    ray.shutdown()

@pytest.fixture
def ray_actor():
    @ray.remote
    class Actor(object):
        """ Ray Actor with TF differentiable functions. """

        def __init__(self):
            # enable eager execution
            tf.enable_eager_execution()

        # single input, single output
        @tf_differentiable(num_return_vals=1)
        def square(self, x):
            return x**2

        @tf_differentiable(num_return_vals=1)
        def cube(self, x):
            return x**3

        @tf_differentiable(num_return_vals=1)
        def double(self, x):
            return 2 * x


        # multiple inputs, single output
        @tf_differentiable(num_return_vals=1)
        def sum(self, *inputs):
            return reduce(lambda x, y: x + y, inputs)

        @tf_differentiable(num_return_vals=1)
        def prod(self, *inputs):
            return reduce(lambda x, y: x * y, inputs)

        @tf_differentiable(num_return_vals=1)
        def sum_square_cube(self, x, y):
            return x**2 + y**3


        # single input, multiple outputs
        @tf_differentiable(num_return_vals=2)
        def single_in_square_cube(self, x):
            return x**2, x**3


        # multiple inputs, multiple outputs
        @tf_differentiable(num_return_vals=2)
        def two_in_square_cube_v1(self, x, y):
            return x**2, y**3

        @tf_differentiable(num_return_vals=2)
        def two_in_square_cube_v2(self, x, y):
            return x**2, x**3

        @tf_differentiable(num_return_vals=5)
        def f(self, x, y, z):
            return x**2, y**3, x**4, 2 * z, x * y

        @tf_differentiable(num_return_vals=2)
        def g(self, x, y, z):
            return x + y + z, x * y * z


        # incorrect number of return values
        @tf_differentiable(num_return_vals=2)
        def incorrect_num_return_vals(self, x, y, z):
            return x**2, y**3, x**4, 2 * z, x * y

    return Actor.remote()

@pytest.fixture
def dummy_actor():
    class Actor(object):
        """ Regular Python version of above Ray Actor. """

        def __init__(self):
            # enable eager execution
            tf.enable_eager_execution()

        # single input, single output
        def square(self, x):
            return x**2

        def cube(self, x):
            return x**3

        def double(self, x):
            return 2 * x


        # multiple inputs, single output
        def sum(self, *inputs):
            return reduce(lambda x, y: x + y, inputs)

        def prod(self, *inputs):
            return reduce(lambda x, y: x * y, inputs)

        def sum_square_cube(self, x, y):
            return x**2 + y**3


        # single input, multiple outputs
        def single_in_square_cube(self, x):
            return x**2, x**3


        # multiple inputs, multiple outputs
        def two_in_square_cube_v1(self, x, y):
            return x**2, y**3

        def two_in_square_cube_v2(self, x, y):
            return x**2, x**3

        def f(self, x, y, z):
            return x**2, y**3, x**4, 2 * z, x * y

        def g(self, x, y, z):
            return x + y + z, x * y * z


        # incorrect number of return values
        def incorrect_num_return_vals(self, x, y, z):
            return x**2, y**3, x**4, 2 * z, x * y

    return Actor()


def check_tensor_outputs(out_1, out_2):
    """ Verifies that two outputs consisting of TF eager tensors are equivalent. """

    #TODO(vsatish): Figure out why we sometimes return 0.0 instead of None.
    if out_1 is None and isinstance(out_2, tf.Tensor) and out_2.numpy() == 0.0:
        out_1 = tf.constant(0.0)
    elif out_2 is None and isinstance(out_1, tf.Tensor) and out_1.numpy() == 0.0:
        out_2 = tf.constant(0.0)

    if isinstance(out_1, list) and isinstance(out_2, tuple):
        out_2 = list(out_2)
    elif isinstance(out_1, tuple) and isinstance(out_2, list):
        out_1 = list(out_1)
    else:
        assert type(out_1) == type(out_2), "Outputs must be of the same type."    

    if isinstance(out_1, tf.Tensor):
        # vanilla tensors
        assert np.all(out_1.numpy() == out_2.numpy()), "OUT 1: {}, OUT 2:".format(out_1.numpy() - out_2.numpy())
    elif out_1 is None:
        # this can happen if we are taking the gradient w.r.t an unused source
        assert out_2 is None
    elif isinstance(out_1, (list, tuple)):
        # recursively compare the elements
        for elem_1, elem_2 in zip(out_1, out_2):
            check_tensor_outputs(elem_1, elem_2)
    else:
        raise ValueError("Unsupported comparison type '{}'".format(type(out_1)))

############################## SINGLE INPUT/OUTPUT ##############################
### LINEAR
# custom op w/ single input & single output
@pytest.mark.skip
def test_single_op_single_in_single_out(start_ray, ray_actor, dummy_actor):
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

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# custom op w/ single input & single output both preceded and followed by tf ops
@pytest.mark.skip
def test_single_op_single_in_single_out_tf_sandwich(start_ray, ray_actor, dummy_actor):
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

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# multiple custom ops w/ single input & single output
@pytest.mark.skip
def test_multiple_ops_single_in_single_out(start_ray, ray_actor, dummy_actor):
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

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# multiple custom ops w/ single input & single output w/ ray fetches in-between
@pytest.mark.skip
def test_multiple_ops_single_in_single_out_inter_fetches(start_ray, ray_actor, dummy_actor):
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

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

### NON-LINEAR
# single custom op w/ single input & single output and resulting ObjectID reused multiple times
@pytest.mark.skip
def test_single_op_single_in_single_out_reuse_result(start_ray, ray_actor, dummy_actor):
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

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# multiple custom ops w/ single input & single output with reused intermediate results
@pytest.mark.skip
def test_multiple_ops_single_in_single_out_reuse_inter_results(start_ray, ray_actor, dummy_actor):
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

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

## LARGE TESTS
def test_multiple_ops_single_in_single_out_large_v1(start_ray, ray_actor, dummy_actor):
    x = tf.Variable([1.0, 2.0, 3.0, 4.0], dtype=tf.float64)
    y = tf.Variable(3.0, dtype=tf.float64)
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

    check_tensor_outputs(out_hat, out)
    print("GRAD", grad)
    print("GRAD_HAT", grad_hat)
    check_tensor_outputs(grad_hat, grad)

@pytest.mark.skip
def test_multiple_ops_single_in_single_out_large_v2(start_ray, ray_actor, dummy_actor):
    w = tf.Variable(2.0, dtype=tf.float64)
    x = tf.Variable(3.0, dtype=tf.float64)
    y = tf.Variable(4.0, dtype=tf.float64)
    z = tf.Variable(5.0, dtype=tf.float64)
    with tf.GradientTape() as t:
        i_1 = ray_actor.double.remote(w * x)
        i_2 = ray_actor.cube.remote(i_1)
        i_3 = ray_actor.square.remote(i_1)
        i_4 = w * x * ray.get(i_2) * ray.get(i_3)
        i_5 = ray.get(ray_actor.double.remote(i_4)) + ray.get(ray_actor.square.remote(i_3))
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

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)


############################## MULTIPLE INPUTS/OUTPUTS ##############################
# custom op w/ single input & multiple outputs
@pytest.mark.skip
def test_single_op_single_in_multiple_out(start_ray, ray_actor, dummy_actor):
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

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# custom op w/ multiple inputs & single output
@pytest.mark.skip
def test_single_op_multiple_in_single_out(start_ray, ray_actor, dummy_actor):
    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    with tf.GradientTape() as t:
        out = ray_actor.sum_square_cube.remote(x, y)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, [x, y])

    with tf.GradientTape() as t:
        out = dummy_actor.sum_square_cube(x, y)
    grad = t.gradient(out, [x, y])

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# custom op w/ multiple inputs & outputs
@pytest.mark.skip
def test_single_op_multiple_in_multiple_out(start_ray, ray_actor, dummy_actor):
    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    with tf.GradientTape() as t:
        out = ray_actor.two_in_square_cube_v1.remote(x, y)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, [x, y])

    with tf.GradientTape() as t:
        out = dummy_actor.two_in_square_cube_v1(x, y)
    grad = t.gradient(out, [x, y])

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# custom op w/ multiple inputs & outputs where one input of op is never used
@pytest.mark.skip
def test_single_op_multiple_in_multiple_out_unused_input(start_ray, ray_actor, dummy_actor):
    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    with tf.GradientTape() as t:
        out = ray_actor.two_in_square_cube_v2.remote(x, y)
        out_hat = ray.get(out)
    grad_hat = t.gradient(out_hat, [x, y])

    with tf.GradientTape() as t:
        out = dummy_actor.two_in_square_cube_v2(x, y)
    grad = t.gradient(out, [x, y])

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# custom op w/ multiple inputs & outputs w/ only one ray.ObjectID fetched
@pytest.mark.skip
def test_single_op_multiple_in_multiple_out_single_fetch(start_ray, ray_actor, dummy_actor):
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

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# custom op w/ multiple inputs & outputs both preceded and followed by tf ops w/ two ray.ObjectIDs fetched
@pytest.mark.skip
def test_single_op_multiple_in_multiple_out_tf_sandwich_double_fetch(start_ray, ray_actor, dummy_actor):
    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    z = tf.Variable(4.0)
    with tf.GradientTape() as t:
        i_1 = x * y
        i_2 = 2 * z
        i_3 = y**2
        i_4 = ray_actor.f.remote(i_1, i_2, i_3)
        out_1, out_2 = ray.get(i_4[:2])
        out_1*=2
        out_2**=2
        out_hat = [out_1, out_2] 
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1 = x * y
        i_2 = 2 * z
        i_3 = y**2
        i_4 = dummy_actor.f(i_1, i_2, i_3)
        out_1, out_2 = i_4[:2]
        out_1*=2
        out_2**=2
        out = [out_1, out_2] 
    grad = t.gradient(out, [x])

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# multiple custom ops w/ multiple inputs and outputs
@pytest.mark.skip
def test_multiple_ops_multiple_in_multiple_out(start_ray, ray_actor, dummy_actor):
    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    z = tf.Variable(4.0)
    with tf.GradientTape() as t:
        i_1, i_2, i_3, i_4, i_5 = ray_actor.f.remote(x, y, z)
        i_6, i_7 = ray_actor.g.remote(i_1, i_3, i_5)
        i_8 = ray_actor.sum_square_cube.remote(i_6, i_7) 
        out_hat = ray.get(i_8)
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1, i_2, i_3, i_4, i_5 = dummy_actor.f(x, y, z)
        i_6, i_7 = dummy_actor.g(i_1, i_3, i_5)
        i_8 = dummy_actor.sum_square_cube(i_6, i_7) 
        out = i_8
    grad = t.gradient(out, [x, y, z])

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# multiple custom ops w/ multiple inputs and outputs w/ ray fetches in-between
@pytest.mark.skip
def test_multiple_ops_multiple_in_multiple_out_inter_fetches(start_ray, ray_actor, dummy_actor):
    x = tf.Variable(2.0)
    y = tf.Variable(3.0)
    z = tf.Variable(4.0)
    with tf.GradientTape() as t:
        i_1, i_2, i_3, i_4, i_5 = ray_actor.f.remote(x, y, z)
        i_1, i_5 = ray.get(i_1), ray.get(i_5)
        i_6, i_7 = ray_actor.g.remote(i_1, i_3, i_5)
        i_6 = ray.get(i_6)
        i_8 = ray_actor.sum_square_cube.remote(i_6, i_7) 
        out_hat = ray.get(i_8)
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1, i_2, i_3, i_4, i_5 = dummy_actor.f(x, y, z)
        i_6, i_7 = dummy_actor.g(i_1, i_3, i_5)
        i_8 = dummy_actor.sum_square_cube(i_6, i_7) 
        out = i_8
    grad = t.gradient(out, [x, y, z])

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

# multiple custom ops w/ multiple inputs and outputs w/ intertwined tf ops and not all ray.ObjectIDs (intermediate and final) used
@pytest.mark.skip
def test_multiple_ops_multiple_in_multiple_out_tf_intertwined_partial_use(start_ray, ray_actor, dummy_actor):
    x = tf.Variable(6.5, dtype=tf.float64)
    y = tf.Variable(7.0, dtype=tf.float64)
    z = tf.Variable(-8.0, dtype=tf.float64)
    with tf.GradientTape() as t:
        i_1 = x * y + z**2
        i_2 = z * 3
        i_3, i_4, i_5, i_6, i_7 = ray_actor.f.remote(x, i_1, i_2)
        i_8, i_9 = ray_actor.g.remote(i_3, i_5, i_7)
        i_8 = ray.get(i_8)**2
        i_10, i_11 = ray_actor.two_in_square_cube_v2.remote(i_8, i_8) 
        out_hat = ray.get(i_10)**2
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1 = x * y + z**2
        i_2 = z * 3
        i_3, i_4, i_5, i_6, i_7 = dummy_actor.f(x, i_1, i_2)
        i_8, i_9 = dummy_actor.g(i_3, i_5, i_7)
        i_8 = i_8**2
        i_10, i_11 = dummy_actor.two_in_square_cube_v2(i_8, i_8) 
        out = i_10**2
    grad = t.gradient(out, [x, y, z])

    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)

## LARGE TESTS
@pytest.mark.skip
def test_multiple_ops_multiple_in_multiple_out_large_v1(start_ray, ray_actor, dummy_actor):
    x = tf.Variable(2.0, dtype=tf.float64)
    y = tf.Variable(3.0, dtype=tf.float64)
    z = tf.Variable(4.0, dtype=tf.float64)
    
    with tf.GradientTape() as t:
        i_1 = x * y + z**2
        i_2 = z * 3 * ray.get(ray_actor.prod.remote(x, y, y))
        i_3 = ray_actor.sum.remote(x, y, z)
        i_4, _, i_5, _, i_6 = ray_actor.f.remote(i_1, i_2, i_3)
        i_7 = ray_actor.prod.remote(i_1, i_3, i_4, i_5, i_6)
        i_8, _ = ray_actor.g.remote(i_3, i_5, i_7)
        i_8 = ray.get(i_8)**2
        _, i_9 = ray_actor.two_in_square_cube_v2.remote(i_8, i_8) 
        i_10 = ray_actor.prod.remote(i_9, x)
        print(i_8)
        out_hat = i_8**2
    grad_hat = t.gradient(out_hat, [x, y, z])

    with tf.GradientTape() as t:
        i_1 = x * y + z**2
        i_2 = z * 3 * dummy_actor.prod(x, y, y)
        i_3 = dummy_actor.sum(x, y, z)
        i_4, _, i_5, _, i_6 = dummy_actor.f(i_1, i_2, i_3)
        i_7 = dummy_actor.prod(i_1, i_3, i_4, i_5, i_6)
        i_8, _ = dummy_actor.g(i_3, i_5, i_7)
        i_8 = i_8**2
        _, i_9 = dummy_actor.two_in_square_cube_v2(i_8, i_8) 
        i_10 = dummy_actor.prod(i_9, x)
        out = i_8**2
    grad = t.gradient(out, [x, y, z])

    print(out_hat, out)
    check_tensor_outputs(out_hat, out)
    check_tensor_outputs(grad_hat, grad)


############################## EXCEPTIONS ##############################
# not having eager execution enabled
@pytest.mark.skip(reason="Need a way to disable eager execution from prior tests.")
def test_eager_not_enabled(start_ray):
    @ray.remote
    class NonEagerActor(object):
        """ Ray Actor with TF differentiable functions without eager execution enabled. """
        
        @tf_differentiable(num_return_vals=1)
        def square(x):
            return x**2

    non_eager_actor = NonEagerActor.remote()
    
    x = tf.Variable(2.0)
    with tf.GradientTape() as t:
        with pytest.raises(RuntimeError):
            out = non_eager_actor.square.remote(x)

