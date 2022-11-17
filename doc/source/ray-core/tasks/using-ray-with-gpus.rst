.. _gpu-support:

GPU Support
===========

GPUs are critical for many machine learning applications.
Ray natively supports GPU as a pre-defined :ref:`resource <core-resources>` type and allows tasks and actors to specify their GPU :ref:`resource requirements <resource-requirements>`.

Starting Ray Nodes with GPUs
----------------------------

By default, Ray will set the quantity of GPU resources of a node to the physical quantities of GPUs auto detected by Ray.
If you need to, you can :ref:`override <specify-node-resources>` this.

.. note::

  There is nothing preventing you from specifying a larger value of
  ``num_gpus`` than the true number of GPUs on the machine given Ray resources are :ref:`logical <logical-resources>`.
  In this case, Ray will act as if the machine has the number of GPUs you specified
  for the purposes of scheduling tasks and actors that require GPUs.
  Trouble will only occur if those tasks and actors
  attempt to actually use GPUs that don't exist.

.. tip::

  You can set ``CUDA_VISIBLE_DEVICES`` environment variable before starting a Ray node
  to limit the GPUs that are visible to Ray.
  For example, ``CUDA_VISIBLE_DEVICES=1,3 ray start --head --num-gpus=2``
  will let Ray only see devices 1 and 3.

Using GPUs in Tasks and Actors
------------------------------

If a task or actor requires GPUs, you can specify the corresponding :ref:`resource requirements <resource-requirements>` (e.g. ``@ray.remote(num_gpus=1)``).
Ray will then schedule the task or actor to a node that has enough free GPU resources
and assign GPUs to the task or actor by setting the ``CUDA_VISIBLE_DEVICES`` environment variable before running the task or actor code.

.. literalinclude:: ../doc_code/gpus.py
    :language: python
    :start-after: __get_gpu_ids_start__
    :end-before: __get_gpu_ids_end__

Inside a task or actor, :ref:`ray.get_gpu_ids() <ray-get_gpu_ids-ref>` will return a
list of GPU IDs that are available to the task or actor.
Typically, it is not necessary to call ``ray.get_gpu_ids()`` because Ray will
automatically set the ``CUDA_VISIBLE_DEVICES`` environment variable,
which most ML frameworks will respect for purposes of GPU assignment.

**Note:** The function ``use_gpu`` defined above doesn't actually use any
GPUs. Ray will schedule it on a node which has at least one GPU, and will
reserve one GPU for it while it is being executed, however it is up to the
function to actually make use of the GPU. This is typically done through an
external library like TensorFlow. Here is an example that actually uses GPUs.
In order for this example to work, you will need to install the GPU version of
TensorFlow.

.. literalinclude:: ../doc_code/gpus.py
    :language: python
    :start-after: __gpus_tf_start__
    :end-before: __gpus_tf_end__

**Note:** It is certainly possible for the person implementing ``use_gpu`` to
ignore ``ray.get_gpu_ids()`` and to use all of the GPUs on the machine. Ray does
not prevent this from happening, and this can lead to too many tasks or actors using the
same GPU at the same time. However, Ray does automatically set the
``CUDA_VISIBLE_DEVICES`` environment variable, which will restrict the GPUs used
by most deep learning frameworks assuming it's not overridden by the user.

Fractional GPUs
---------------

Ray supports :ref:`fractional resource requirements <fractional-resource-requirements>`
so multiple tasks and actors can share the same GPU.

.. literalinclude:: ../doc_code/gpus.py
    :language: python
    :start-after: __fractional_gpus_start__
    :end-before: __fractional_gpus_end__

**Note:** It is the user's responsibility to make sure that the individual tasks
don't use more than their share of the GPU memory.
TensorFlow can be configured to limit its memory usage.

When Ray assigns GPUs of a node to tasks or actors with fractional resource requirements,
it will pack one GPU before moving on to the next one to avoid fragmentation.

.. literalinclude:: ../doc_code/gpus.py
    :language: python
    :start-after: __fractional_gpus_packing_start__
    :end-before: __fractional_gpus_packing_end__

Workers not Releasing GPU Resources
-----------------------------------

Currently, when a worker executes a task that uses a GPU (e.g.,
through TensorFlow), the task may allocate memory on the GPU and may not release
it when the task finishes executing. This can lead to problems the next time a
task tries to use the same GPU. To address the problem, Ray disables the worker
process reuse between GPU tasks by default, where the GPU resources is released after
the task process exists. Since this adds overhead to GPU task scheduling,
you can re-enable worker reuse by setting ``max_calls=0``
in the :ref:`ray.remote <ray-remote-ref>` decorator.

.. literalinclude:: ../doc_code/gpus.py
    :language: python
    :start-after: __leak_gpus_start__
    :end-before: __leak_gpus_end__
