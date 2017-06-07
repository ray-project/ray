Using Ray with GPUs
===================

GPUs are well-suited to parallel computations and can provide additional 
speedup to your machine learning algorithms. Ray supports enabling GPU 
usage on your machine with minimal changes to syntax.

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

TODO

Using Actors with GPUs
----------------------

When defining an actor that uses GPUs, you must indicate the  
number of GPUs the actor may use in the ``ray.remote`` decorator. 

.. code-block:: python

  @ray.remote(num_gpus=1)
  class GPUActor(object):
    def __init__(self):
      print("I am allowed to use 1 GPU.")

Note that Ray must have been started with at least as many GPUs as
the number of GPUs you pass in to the ``ray.remote`` decorator. 
Otherwise, if you pass in a number greater than what was passed into 
``ray.init``, an exception will be thrown when instantiating a 
``GPUActor``.

To instantiate a GPU-enabled actor, you can call ``GPUActor.remote``
as usual.

.. code-block:: python

  a1 = GPUActor.remote()

You can easily create one actor for each GPU in Ray as follows:

.. code-block:: python

  ray.init(num_gpus=x)  # x is a placeholder here
  actors = [GPUActor.remote() for _ in range(x)]

When an actor is created, it can access the list of IDs of the GPUs
that it is allowed to use via ``ray.get_gpu_ids()``. This returns
a list of integers, such as ``[]``, ``[1]``, or ``[2, 5, 6]``. This 
list of integer ids is of the same length as the number of GPUs 
specified into the ``ray.remote`` decorator.

.. code-block:: python

  @ray.remote(num_gpus=1)
  class GPUActor(object):
    def __init__(self):
      gpu_ids = ray.get_gpu_ids()  # Always of length 1

These integer IDs are the IDs automatically assigned to your 
machine's GPU devices by CUDA. CUDA is a direct GPU programming 
API created by Nvidia, and assigns integer IDs via arbitrary 
ennumeration. For example, 0 would represent the first GPU on 
your machine, 1 would represent the second GPU on your machine, 
etc. 

Accessing the list of GPU IDs via ``ray.get_gpu_ids()`` is thus 
useful for setting up GPU usage in CUDA applications such as 
TensorFlow. For example, you can set which GPU devices are visible 
by setting the ``CUDA_VISIBLE_DEVICES`` environment variable as 
follows:

.. code-block:: python

  @ray.remote(num_gpus=1)
  class GPUActor(object):
    def __init__(self):
      os.environ['CUDA_VISIBLE_DEVICES'] = ','.join([str(i) for i in ray.get_gpu_ids()])

Note that for TensorFlow, ``CUDA_VISIBLE_DEVICES`` must be set 
prior to calling ``tf.Session``.
