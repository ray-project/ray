GPU Support
===========

GPUs are critical for many machine learning applications. Ray enables remote
functions and actors to specify their GPU requirements in the ``ray.remote``
decorator.

Starting Ray with GPUs
----------------------

Ray will automatically detect the number of GPUs available on a machine.
If you need to, you can override this by specifying ``ray.init(num_gpus=N)`` or
``ray start --num-gpus=N``.

**Note:** There is nothing preventing you from passing in a larger value of
``num_gpus`` than the true number of GPUs on the machine. In this case, Ray will
act as if the machine has the number of GPUs you specified for the purposes of
scheduling tasks that require GPUs. Trouble will only occur if those tasks
attempt to actually use GPUs that don't exist.

Using Remote Functions with GPUs
--------------------------------

If a remote function requires GPUs, indicate the number of required GPUs in the
remote decorator.

.. code-block:: python

  import os

  @ray.remote(num_gpus=1)
  def use_gpu():
      print("ray.get_gpu_ids(): {}".format(ray.get_gpu_ids()))
      print("CUDA_VISIBLE_DEVICES: {}".format(os.environ["CUDA_VISIBLE_DEVICES"]))

Inside of the remote function, a call to ``ray.get_gpu_ids()`` will return a
list of strings indicating which GPUs the remote function is allowed to use.
Typically, it is not necessary to call ``ray.get_gpu_ids()`` because Ray will
automatically set the ``CUDA_VISIBLE_DEVICES`` environment variable.

**Note:** The function ``use_gpu`` defined above doesn't actually use any
GPUs. Ray will schedule it on a machine which has at least one GPU, and will
reserve one GPU for it while it is being executed, however it is up to the
function to actually make use of the GPU. This is typically done through an
external library like TensorFlow. Here is an example that actually uses GPUs.
Note that for this example to work, you will need to install the GPU version of
TensorFlow.

.. code-block:: python

  import tensorflow as tf

  @ray.remote(num_gpus=1)
  def use_gpu():
      # Create a TensorFlow session. TensorFlow will restrict itself to use the
      # GPUs specified by the CUDA_VISIBLE_DEVICES environment variable.
      tf.Session()

**Note:** It is certainly possible for the person implementing ``use_gpu`` to
ignore ``ray.get_gpu_ids`` and to use all of the GPUs on the machine. Ray does
not prevent this from happening, and this can lead to too many workers using the
same GPU at the same time. However, Ray does automatically set the
``CUDA_VISIBLE_DEVICES`` environment variable, which will restrict the GPUs used
by most deep learning frameworks.

Fractional GPUs
---------------

If you want two tasks to share the same GPU, then the tasks can each request
half (or some other fraction) of a GPU.

.. code-block:: python

  import ray
  import time

  ray.init(num_cpus=4, num_gpus=1)

  @ray.remote(num_gpus=0.25)
  def f():
      time.sleep(1)

  # The four tasks created here can execute concurrently.
  ray.get([f.remote() for _ in range(4)])

It is the developer's responsibility to make sure that the individual tasks
don't use more than their share of the GPU memory. TensorFlow can be configured
to limit its memory usage.

Workers not Releasing GPU Resources
-----------------------------------

**Note:** Currently, when a worker executes a task that uses a GPU (e.g.,
through TensorFlow), the task may allocate memory on the GPU and may not release
it when the task finishes executing. This can lead to problems the next time a
task tries to use the same GPU. You can address this by setting ``max_calls=1``
in the remote decorator so that the worker automatically exits after executing
the task (thereby releasing the GPU resources).

.. code-block:: python

  import tensorflow as tf

  @ray.remote(num_gpus=1, max_calls=1)
  def leak_gpus():
      # This task will allocate memory on the GPU and then never release it, so
      # we include the max_calls argument to kill the worker and release the
      # resources.
      sess = tf.Session()
