.. _gpu-support:
.. _accelerator-support:

Accelerator Support
===================

Accelerators (e.g. GPUs) are critical for many machine learning applications.
Ray Core natively supports many accelerators as pre-defined :ref:`resource <core-resources>` types and allows tasks and actors to specify their accelerator :ref:`resource requirements <resource-requirements>`.

The accelerators natively supported by Ray Core are:

.. list-table::
   :header-rows: 1

   * - Accelerator
     - Ray Resource Name
     - Support Level
   * - Nvidia GPU
     - GPU
     - Fully tested, supported by the Ray team
   * - Intel GPU
     - GPU
     - Experimental, supported by the community
   * - `AWS Neuron Core <https://awsdocs-neuron.readthedocs-hosted.com/en/latest/general/arch/model-architecture-fit.html>`_
     - neuron_cores
     - Experimental, supported by the community
   * - Google TPU
     - TPU
     - Experimental, supported by the community
   * - Intel Gaudi
     - HPU
     - Experimental, supported by the community
   * - Huawei Ascend
     - NPU
     - Experimental, supported by the community

Starting Ray Nodes with Accelerators
------------------------------------

By default, Ray will set the quantity of accelerator resources of a node to the physical quantities of accelerators auto detected by Ray.
If you need to, you can :ref:`override <specify-node-resources>` this.

.. tab-set::

    .. tab-item:: Nvidia GPU
        :sync: Nvidia GPU

        .. tip::

            You can set ``CUDA_VISIBLE_DEVICES`` environment variable before starting a Ray node
            to limit the Nvidia GPUs that are visible to Ray.
            For example, ``CUDA_VISIBLE_DEVICES=1,3 ray start --head --num-gpus=2``
            will let Ray only see devices 1 and 3.

    .. tab-item:: Intel GPU
        :sync: Intel GPU

        .. tip::

            You can set ``ONEAPI_DEVICE_SELECTOR`` environment variable before starting a Ray node
            to limit the Intel GPUs that are visible to Ray.
            For example, ``ONEAPI_DEVICE_SELECTOR=1,3 ray start --head --num-gpus=2``
            will let Ray only see devices 1 and 3.

    .. tab-item:: AWS Neuron Core
        :sync: AWS Neuron Core

        .. tip::

            You can set ``NEURON_RT_VISIBLE_CORES`` environment variable before starting a Ray node
            to limit the AWS Neuro Cores that are visible to Ray.
            For example, ``NEURON_RT_VISIBLE_CORES=1,3 ray start --head --resources='{"neuron_cores": 2}'``
            will let Ray only see devices 1 and 3.

    .. tab-item:: Google TPU
        :sync: Google TPU

        .. tip::

            You can set ``TPU_VISIBLE_CHIPS`` environment variable before starting a Ray node
            to limit the Google TPUs that are visible to Ray.
            For example, ``TPU_VISIBLE_CHIPS=1,3 ray start --head --resources='{"TPU": 2}'``
            will let Ray only see devices 1 and 3.

    .. tab-item:: Intel Gaudi
        :sync: Intel Gaudi

        .. tip::

            You can set ``HABANA_VISIBLE_MODULES`` environment variable before starting a Ray node
            to limit the Intel Gaudi HPUs that are visible to Ray.
            For example, ``HABANA_VISIBLE_MODULES=1,3 ray start --head --resources='{"HPU": 2}'``
            will let Ray only see devices 1 and 3.

    .. tab-item:: Huawei Ascend
        :sync: Huawei Ascend

        .. tip::

            You can set ``ASCEND_VISIBLE_DEVICES`` environment variable before starting a Ray node
            to limit the Huawei Ascend NPUs that are visible to Ray.
            For example, ``ASCEND_VISIBLE_DEVICES=1,3 ray start --head --resources='{"NPU": 2}'``
            will let Ray only see devices 1 and 3.

.. note::

  There is nothing preventing you from specifying a larger number of
  accelerator resources (e.g. ``num_gpus``) than the true number of accelerators on the machine given Ray resources are :ref:`logical <logical-resources>`.
  In this case, Ray will act as if the machine has the number of accelerators you specified
  for the purposes of scheduling tasks and actors that require accelerators.
  Trouble will only occur if those tasks and actors
  attempt to actually use accelerators that don't exist.

Using accelerators in Tasks and Actors
--------------------------------------

If a task or actor requires accelerators, you can specify the corresponding :ref:`resource requirements <resource-requirements>` (e.g. ``@ray.remote(num_gpus=1)``).
Ray will then schedule the task or actor to a node that has enough free accelerator resources
and assign accelerators to the task or actor by setting the corresponding environment variable (e.g. ``CUDA_VISIBLE_DEVICES``) before running the task or actor code.

.. tab-set::

    .. tab-item:: Nvidia GPU
        :sync: Nvidia GPU

        .. testcode::

            import os
            import ray

            ray.init(num_gpus=2)

            @ray.remote(num_gpus=1)
            class GPUActor:
                def ping(self):
                    print("GPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["GPU"]))
                    print("CUDA_VISIBLE_DEVICES: {}".format(os.environ["CUDA_VISIBLE_DEVICES"]))

            @ray.remote(num_gpus=1)
            def gpu_task():
                print("GPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["GPU"]))
                print("CUDA_VISIBLE_DEVICES: {}".format(os.environ["CUDA_VISIBLE_DEVICES"]))

            gpu_actor = GPUActor.remote()
            ray.get(gpu_actor.ping.remote())
            # The actor uses the first GPU so the task will use the second one.
            ray.get(gpu_task.remote())

        .. testoutput::
            :options: +MOCK

            (GPUActor pid=52420) GPU ids: [0]
            (GPUActor pid=52420) CUDA_VISIBLE_DEVICES: 0
            (gpu_task pid=51830) GPU ids: [1]
            (gpu_task pid=51830) CUDA_VISIBLE_DEVICES: 1

    .. tab-item:: Intel GPU
        :sync: Intel GPU

        .. testcode::
            :hide:

            ray.shutdown()

        .. testcode::
            :skipif: True

            import os
            import ray

            ray.init(num_gpus=2)

            @ray.remote(num_gpus=1)
            class GPUActor:
                def ping(self):
                    print("GPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["GPU"]))
                    print("ONEAPI_DEVICE_SELECTOR: {}".format(os.environ["ONEAPI_DEVICE_SELECTOR"]))

            @ray.remote(num_gpus=1)
            def gpu_task():
                print("GPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["GPU"]))
                print("ONEAPI_DEVICE_SELECTOR: {}".format(os.environ["ONEAPI_DEVICE_SELECTOR"]))

            gpu_actor = GPUActor.remote()
            ray.get(gpu_actor.ping.remote())
            # The actor uses the first GPU so the task will use the second one.
            ray.get(gpu_task.remote())

        .. testoutput::
            :options: +MOCK

            (GPUActor pid=52420) GPU ids: [0]
            (GPUActor pid=52420) ONEAPI_DEVICE_SELECTOR: 0
            (gpu_task pid=51830) GPU ids: [1]
            (gpu_task pid=51830) ONEAPI_DEVICE_SELECTOR: 1

    .. tab-item:: AWS Neuron Core
        :sync: AWS Neuron Core

        .. testcode::
            :hide:

            ray.shutdown()

        .. testcode::

            import os
            import ray

            ray.init(resources={"neuron_cores": 2})

            @ray.remote(resources={"neuron_cores": 1})
            class NeuronCoreActor:
                def ping(self):
                    print("Neuron Core ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["neuron_cores"]))
                    print("NEURON_RT_VISIBLE_CORES: {}".format(os.environ["NEURON_RT_VISIBLE_CORES"]))

            @ray.remote(resources={"neuron_cores": 1})
            def neuron_core_task():
                print("Neuron Core ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["neuron_cores"]))
                print("NEURON_RT_VISIBLE_CORES: {}".format(os.environ["NEURON_RT_VISIBLE_CORES"]))

            neuron_core_actor = NeuronCoreActor.remote()
            ray.get(neuron_core_actor.ping.remote())
            # The actor uses the first Neuron Core so the task will use the second one.
            ray.get(neuron_core_task.remote())

        .. testoutput::
            :options: +MOCK

            (NeuronCoreActor pid=52420) Neuron Core ids: [0]
            (NeuronCoreActor pid=52420) NEURON_RT_VISIBLE_CORES: 0
            (neuron_core_task pid=51830) Neuron Core ids: [1]
            (neuron_core_task pid=51830) NEURON_RT_VISIBLE_CORES: 1

    .. tab-item:: Google TPU
        :sync: Google TPU

        .. testcode::
            :hide:

            ray.shutdown()

        .. testcode::

            import os
            import ray

            ray.init(resources={"TPU": 2})

            @ray.remote(resources={"TPU": 1})
            class TPUActor:
                def ping(self):
                    print("TPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["TPU"]))
                    print("TPU_VISIBLE_CHIPS: {}".format(os.environ["TPU_VISIBLE_CHIPS"]))

            @ray.remote(resources={"TPU": 1})
            def tpu_task():
                print("TPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["TPU"]))
                print("TPU_VISIBLE_CHIPS: {}".format(os.environ["TPU_VISIBLE_CHIPS"]))

            tpu_actor = TPUActor.remote()
            ray.get(tpu_actor.ping.remote())
            # The actor uses the first TPU so the task will use the second one.
            ray.get(tpu_task.remote())

        .. testoutput::
            :options: +MOCK

            (TPUActor pid=52420) TPU ids: [0]
            (TPUActor pid=52420) TPU_VISIBLE_CHIPS: 0
            (tpu_task pid=51830) TPU ids: [1]
            (tpu_task pid=51830) TPU_VISIBLE_CHIPS: 1

    .. tab-item:: Intel Gaudi
        :sync: Intel Gaudi

        .. testcode::
            :hide:

            ray.shutdown()

        .. testcode::

            import os
            import ray

            ray.init(resources={"HPU": 2})

            @ray.remote(resources={"HPU": 1})
            class HPUActor:
                def ping(self):
                    print("HPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["HPU"]))
                    print("HABANA_VISIBLE_MODULES: {}".format(os.environ["HABANA_VISIBLE_MODULES"]))

            @ray.remote(resources={"HPU": 1})
            def hpu_task():
                print("HPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["HPU"]))
                print("HABANA_VISIBLE_MODULES: {}".format(os.environ["HABANA_VISIBLE_MODULES"]))

            hpu_actor = HPUActor.remote()
            ray.get(hpu_actor.ping.remote())
            # The actor uses the first HPU so the task will use the second one.
            ray.get(hpu_task.remote())

        .. testoutput::
            :options: +MOCK

            (HPUActor pid=52420) HPU ids: [0]
            (HPUActor pid=52420) HABANA_VISIBLE_MODULES: 0
            (hpu_task pid=51830) HPU ids: [1]
            (hpu_task pid=51830) HABANA_VISIBLE_MODULES: 1

    .. tab-item:: Huawei Ascend
        :sync: Huawei Ascend

        .. testcode::
            :hide:

            ray.shutdown()

        .. testcode::

            import os
            import ray

            ray.init(resources={"NPU": 2})

            @ray.remote(resources={"NPU": 1})
            class NPUActor:
                def ping(self):
                    print("NPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["NPU"]))
                    print("ASCEND_VISIBLE_DEVICES: {}".format(os.environ["ASCEND_VISIBLE_DEVICES"]))

            @ray.remote(resources={"NPU": 1})
            def npu_task():
                print("NPU ids: {}".format(ray.get_runtime_context().get_accelerator_ids()["NPU"]))
                print("ASCEND_VISIBLE_DEVICES: {}".format(os.environ["ASCEND_VISIBLE_DEVICES"]))

            npu_actor = NPUActor.remote()
            ray.get(npu_actor.ping.remote())
            # The actor uses the first NPU so the task will use the second one.
            ray.get(npu_task.remote())

        .. testoutput::
            :options: +MOCK

            (NPUActor pid=52420) NPU ids: [0]
            (NPUActor pid=52420) ASCEND_VISIBLE_DEVICES: 0
            (npu_task pid=51830) NPU ids: [1]
            (npu_task pid=51830) ASCEND_VISIBLE_DEVICES: 1


Inside a task or actor, :func:`ray.get_runtime_context().get_accelerator_ids() <ray.runtime_context.RuntimeContext.get_accelerator_ids>` will return a
list of accelerator IDs that are available to the task or actor.
Typically, it is not necessary to call ``get_accelerator_ids()`` because Ray will
automatically set the corresponding environment variable (e.g. ``CUDA_VISIBLE_DEVICES``),
which most ML frameworks will respect for purposes of accelerator assignment.

**Note:** The remote function or actor defined above doesn't actually use any
accelerators. Ray will schedule it on a node which has at least one accelerator, and will
reserve one accelerator for it while it is being executed, however it is up to the
function to actually make use of the accelerator. This is typically done through an
external library like TensorFlow. Here is an example that actually uses accelerators.
In order for this example to work, you will need to install the GPU version of
TensorFlow.

.. testcode::

    @ray.remote(num_gpus=1)
    def gpu_task():
        import tensorflow as tf

        # Create a TensorFlow session. TensorFlow will restrict itself to use the
        # GPUs specified by the CUDA_VISIBLE_DEVICES environment variable.
        tf.Session()


**Note:** It is certainly possible for the person to
ignore assigned accelerators and to use all of the accelerators on the machine. Ray does
not prevent this from happening, and this can lead to too many tasks or actors using the
same accelerator at the same time. However, Ray does automatically set the
environment variable (e.g. ``CUDA_VISIBLE_DEVICES``), which will restrict the accelerators used
by most deep learning frameworks assuming it's not overridden by the user.

Fractional Accelerators
-----------------------

Ray supports :ref:`fractional resource requirements <fractional-resource-requirements>`
so multiple tasks and actors can share the same accelerator.

.. tab-set::

    .. tab-item:: Nvidia GPU
        :sync: Nvidia GPU

        .. testcode::
            :hide:

            ray.shutdown()

        .. testcode::

            ray.init(num_cpus=4, num_gpus=1)

            @ray.remote(num_gpus=0.25)
            def f():
                import time

                time.sleep(1)

            # The four tasks created here can execute concurrently
            # and share the same GPU.
            ray.get([f.remote() for _ in range(4)])

    .. tab-item:: Intel GPU
        :sync: Intel GPU

        .. testcode::
            :hide:

            ray.shutdown()

        .. testcode::

            ray.init(num_cpus=4, num_gpus=1)

            @ray.remote(num_gpus=0.25)
            def f():
                import time

                time.sleep(1)

            # The four tasks created here can execute concurrently
            # and share the same GPU.
            ray.get([f.remote() for _ in range(4)])

    .. tab-item:: AWS Neuron Core
        :sync: AWS Neuron Core

        AWS Neuron Core doesn't support fractional resource.

    .. tab-item:: Google TPU
        :sync: Google TPU

        Google TPU doesn't support fractional resource.

    .. tab-item:: Intel Gaudi
        :sync: Intel Gaudi

        Intel Gaudi doesn't support fractional resource.

    .. tab-item:: Huawei Ascend
        :sync: Huawei Ascend

        .. testcode::
            :hide:

            ray.shutdown()

        .. testcode::

            ray.init(num_cpus=4, resources={"NPU": 1})

            @ray.remote(resources={"NPU": 0.25})
            def f():
                import time

                time.sleep(1)

            # The four tasks created here can execute concurrently
            # and share the same NPU.
            ray.get([f.remote() for _ in range(4)])


**Note:** It is the user's responsibility to make sure that the individual tasks
don't use more than their share of the accelerator memory.
Pytorch and TensorFlow can be configured to limit its memory usage.

When Ray assigns accelerators of a node to tasks or actors with fractional resource requirements,
it will pack one accelerator before moving on to the next one to avoid fragmentation.

.. testcode::
    :hide:

    ray.shutdown()

.. testcode::

    ray.init(num_gpus=3)

    @ray.remote(num_gpus=0.5)
    class FractionalGPUActor:
        def ping(self):
            print("GPU id: {}".format(ray.get_runtime_context().get_accelerator_ids()["GPU"]))

    fractional_gpu_actors = [FractionalGPUActor.remote() for _ in range(3)]
    # Ray will try to pack GPUs if possible.
    [ray.get(fractional_gpu_actors[i].ping.remote()) for i in range(3)]

.. testoutput::
    :options: +MOCK

    (FractionalGPUActor pid=57417) GPU id: [0]
    (FractionalGPUActor pid=57416) GPU id: [0]
    (FractionalGPUActor pid=57418) GPU id: [1]

.. _gpu-leak:

Workers not Releasing GPU Resources
-----------------------------------

Currently, when a worker executes a task that uses a GPU (e.g.,
through TensorFlow), the task may allocate memory on the GPU and may not release
it when the task finishes executing. This can lead to problems the next time a
task tries to use the same GPU. To address the problem, Ray disables the worker
process reuse between GPU tasks by default, where the GPU resources is released after
the task process exits. Since this adds overhead to GPU task scheduling,
you can re-enable worker reuse by setting ``max_calls=0``
in the :func:`ray.remote <ray.remote>` decorator.

.. testcode::

    # By default, ray will not reuse workers for GPU tasks to prevent
    # GPU resource leakage.
    @ray.remote(num_gpus=1)
    def leak_gpus():
        import tensorflow as tf

        # This task will allocate memory on the GPU and then never release it.
        tf.Session()

.. _accelerator-types:

Accelerator Types
-----------------

Ray supports resource specific accelerator types. The `accelerator_type` option can be used to force to a task or actor to run on a node with a specific type of accelerator.
Under the hood, the accelerator type option is implemented as a :ref:`custom resource requirement <custom-resources>` of ``"accelerator_type:<type>": 0.001``.
This forces the task or actor to be placed on a node with that particular accelerator type available.
This also lets the multi-node-type autoscaler know that there is demand for that type of resource, potentially triggering the launch of new nodes providing that accelerator.

.. testcode::
    :hide:

    ray.shutdown()
    import ray.util.accelerators
    import ray._private.ray_constants as ray_constants

    v100_resource_name = f"{ray_constants.RESOURCE_CONSTRAINT_PREFIX}{ray.util.accelerators.NVIDIA_TESLA_V100}"
    ray.init(num_gpus=4, resources={v100_resource_name: 1})

.. testcode::

    from ray.util.accelerators import NVIDIA_TESLA_V100

    @ray.remote(num_gpus=1, accelerator_type=NVIDIA_TESLA_V100)
    def train(data):
        return "This function was run on a node with a Tesla V100 GPU"

    ray.get(train.remote(1))

See ``ray.util.accelerators`` for available accelerator types.
