Using Ray with GPUs
===================

GPUs are well-suited to parallel computations and can provide additional 
speedup to your machine learning algorithms. If there are GPUs available 
on your machine, Ray supports enabling GPU usage with minimal changes to 
syntax.

Starting Ray with GPUs
----------------------

In order to start Ray with GPUs, you need to first specify the number of 
available GPUs. You can pass this in when calling ``ray.init``.

.. code-block:: python

  ray.init(num_gpus=2)

Similarly, you can also specify the number of CPUs to use by passing it in.

.. code-block:: python

  ray.init(num_cpus=10, num_gpus=2)

Otherwise, if you don't pass in any arguments to ``ray.init``, Ray will 
detect the number of CPUs automatically with ``psutil.cpu_count()``. 
Ray then assumes that there are 0 GPUs and ignores any on the machine. 

Using Remote Functions with GPUs
--------------------------------

In order to use GPUs in a remote function, you only need
to indicate the number of GPUs needed to execute this 
function in the ``ray.remote`` decorator.

.. code-block:: python

  @ray.remote(num_gpus=1)
  def gpu_method():
    return "I am allowed to use 1 GPU."

The number of GPUs need only be passed when defining the
remote function on the driver.

A GPU-enabled remote function f can be called with ``f.remote``
as usual.

.. code-block:: python

  ray.get(gpu_method.remote())  # Returns "I am allowed to use 1 GPU."

Using Actors with GPUs
----------------------

When defining an actor that uses GPUs, you must indicate the  
number of GPUs an actor instance may use in the ``ray.remote`` 
decorator. 

.. code-block:: python

  @ray.remote(num_gpus=1)
  class GPUActor(object):
    def some_method(self):
      return "I am allowed to use 1 GPU."

Note that Ray must have been started with at least as many GPUs as
the number of GPUs you pass into the actor's ``ray.remote`` decorator. 
Otherwise, if you pass in a number greater than what was passed into 
``ray.init``, an exception will be thrown when instantiating the 
actor.

To instantiate and use a GPU-enabled actor, you can call 
``GPUActor.remote`` and ``some_method.remote`` as usual.

.. code-block:: python

  a1 = GPUActor.remote()
  ray.get(a1.some_method.remote())  # Returns "I am allowed to use 1 GPU."

You can easily create one actor for each GPU in Ray as follows:

.. code-block:: python

  ray.init(num_gpus=x)  # x is a placeholder here
  # [...]
  actors = [GPUActor.remote() for _ in range(x)]

Also, when an actor is instantiated, it can access the list of IDs for 
the GPUs that the actor has been allowed to use via ``ray.get_gpu_ids()``. 
This returns a list of integers, such as ``[]``, ``[1]``, or 
``[2, 5, 6]``. This list of integer ids is of the same length as 
the number of GPUs specified into the ``ray.remote`` decorator.

.. code-block:: python

  @ray.remote(num_gpus=1)
  class GPUActor(object):
    def __init__(self):
      gpu_ids = ray.get_gpu_ids()  # Of length 1

Accessing the list of GPU IDs via ``ray.get_gpu_ids()`` is thus 
useful for setting up GPU usage in CUDA applications such as 
TensorFlow. The CUDA API for supported GPUs allows you to select
the GPU devices for an application by integer ID. 

For example, you can tell TensorFlow which GPUs to use by setting 
the ``CUDA_VISIBLE_DEVICES`` environment variable as follows:

.. code-block:: python

  @ray.remote(num_gpus=1)
  class GPUActor(object):
    def __init__(self):
      os.environ['CUDA_VISIBLE_DEVICES'] = ','.join([str(i) for i in ray.get_gpu_ids()])

The above code only makes the actor's GPUs visible to TensorFlow, 
and reenumerates these GPUs' device IDs as 0, 1, 2, etc.

Note that for TensorFlow, ``CUDA_VISIBLE_DEVICES`` must be set 
prior to calling ``tf.Session``.
