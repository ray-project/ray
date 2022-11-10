.. _core-resources:

Resources
=========

Ray allows you to seemlessly scale your applications from a laptop to a cluster without code change and Ray resources make it possible.
Ray resources abstract away physical machines and let you express your computation in terms of resources instead of machines,
while the system manages scheduling and autoscaling based on resource requests.

A resource in Ray is a key-value pair where the key denotes a resource name, and the value is a float quantity.
For convenience, Ray has native support for CPU, GPU, and memory resource types and they are called pre-defined resources.
Besides those, Ray also supports custom resources.

Logical Resources
-----------------

Ray resources are **logical** and don’t need to have 1-to-1 mapping with physical resources.
For example, you can start a Ray head node with 3 GPUs via ``ray start --head --num-gpus=3`` even if it physically has zero.
They are mainly used for admission control during scheduling.

The resources being logical has several implications:

- Resource requirements of tasks or actors does NOT impose any limits on the actual physical resource usage.
  For example, Ray doesn't prevent a ``num_cpus=1`` task from launching multiple threads and using multiple physical CPUs.
  It's your responsibility to make sure your tasks or actors use no more resources than specified via resource requirements.
- Ray doesn't provide CPU isolation for tasks or actors.
  For example, Ray won't reserve a physical CPU excludsively and pin a ``num_cpus=1`` task to it.
  Ray will let the OS scheduler schedule and run the task instead.
  If needed, you can use OS APIs like ``sched_setaffinity`` to pin a task to a physical CPU.
- Ray does provide :ref:`GPU <gpu-support>` isolation in forms of visible devices by automatically setting the ``CUDA_VISIBLE_DEVICES`` envrionment variable,
  but it is still up to the task or actor to actually honor it or override it.

Custom Resources
----------------

Besides pre-defined resources, you can also specify custom resources that a Ray node has and request them in your tasks or actors.
Several use cases of custom resources are:

- Your node has a special hardware and you can represent it as a custom resource.
  Then your tasks or actors can request the custom resource via ``@ray.remote(resources={"special_hardware": 1})``
  and Ray will schedule the tasks or actors to the node that has the custom resource.
- You can use custom resources as labels to tag nodes and you can achieve label based affinity scheduling.
  For example, you can do ``ray.remote(resources={"custom_label": 0.001})`` to schedule tasks or actors to nodes with ``custom_label`` custom resource.
  In this use case, the actual quantity doesn't matter that much and the convention is to specify a tiny number so that the label resource is
  not the limiting factor for parallelism.

Specifying Node Resources
-------------------------

By default, Ray nodes start with pre-defiend resources and the quantities of those resources on each node are set to the physical quantities auto detected by Ray.
For example, if you start a head node with ``ray start --head`` then the quantity of logical CPU resources will be equal to the number of physical CPUs on the machine.
However you can always override that by manually specifying the quantities of pre-defined resources and also add custom resources.
There are several ways to do that depending on how you start the Ray cluster:

.. tabbed:: ray.init()

    If you are using :ref:`ray.init() <ray-init-ref>` to start a single node Ray cluster, you can do the following to manually specify node resources:

    .. literalinclude:: ../doc_code/resources.py
        :language: python
        :start-after: __specifying_node_resources_start__
        :end-before: __specifying_node_resources_end__

.. tabbed:: ray start

    If you are using :ref:`ray start <ray-start-doc>` to start a Ray node, you can run:

    .. code-block:: shell

        ray start --head --num-cpus=3 --num-gpus=4 --resources='{"special_hardware": 1, "custom_label": 1}'

.. tabbed:: ray up

    If you are using :ref:`ray up <ray-up-doc>` to start a Ray cluster, you can set the :ref:`resources field <cluster-configuration-resources-type>` in the yaml file:

    .. code-block:: yaml

        available_node_types:
          head:
            ...
            resources:
              CPU: 3
              GPU: 4
              special_hardware: 1
              custom_label: 1

.. tabbed:: kuberay

    If you are using :ref:`kuberay <kuberay-index>` to start a Ray cluster, you can set the :ref:`rayStartParams field <rayStartParams>` in the yaml file:

    .. code-block:: yaml

        headGroupSpec:
          rayStartParams:
            num-cpus: "3"
            num-gpus: "4"
            resources: '"{\"special_hardware\": 1, \"custom_label\": 1}"'


.. _resource-requirements:

Specifying Task or Actor Resource Requirements
----------------------------------------------

Ray allows specifying a task or actor's resource requirements (e.g., CPU, GPU, and custom resources).
The task or actor will only run on a node if there are enough required resources
available to execute the task or actor.

By default, Ray tasks use 1 CPU resource and Ray actors use 1 CPU for scheduling and 0 CPU for running
(This means, by default, actors cannot get scheduled on a zero-cpu node, but an infinite number of them can run on any non-zero cpu node.
If resources are specified explicitly, they are required
for both scheduling and running. It's a confusing default behavior for actors due to historical reasons and it's recommended to always explicitly set ``num_cpus`` for actors to aovid any surprises).

You can also explicitly specify a task's or actor's resource requirements (for example, one task may require a GPU) instead of using default ones via :ref:`ray.remote() <ray-remote-ref>` and :ref:`.options() <ray-options-ref>`.

.. tabbed:: Python

    .. literalinclude:: ../doc_code/resources.py
        :language: python
        :start-after: __specifying_resource_requirements_start__
        :end-before: __specifying_resource_requirements_end__

.. tabbed:: Java

    .. code-block:: java

        // Specify required resources.
        Ray.task(MyRayApp::myFunction).setResource("CPU", 1.0).setResource("GPU", 0.5).setResource("special_hardware", 1.0).remote();

        Ray.actor(Counter::new).setResource("CPU", 2.0).setResource("GPU", 0.5).remote();

.. tabbed:: C++

    .. code-block:: c++

        // Specify required resources.
        ray::Task(MyFunction).SetResource("CPU", 1.0).SetResource("GPU", 0.5).SetResource("special_hardware", 1.0).Remote();

        ray::Actor(CreateCounter).SetResource("CPU", 2.0).SetResource("GPU", 0.5).Remote();

.. note::

  Ray supports fractional resource requirements. For example, if your task or actor is IO bound and has low CPU usage, you can specify fractional CPU ``num_cpus=0.5`` or even zero CPU ``num_cpus=0``.

.. tip::

  Besides resource requirements, you can also specify an environment for a task or actor to run in,
  which can include Python packages, local files, environment variables, and more---see :ref:`Runtime Environments <runtime-environments>` for details.

The resource requirements have implications for the Ray's scheduling concurrency.
In particular, the sum of the resource requirements of all of the
concurrently executing tasks and actors on a given node cannot exceed the node's total resources.
This property can be used to :ref:`limit the number of concurrently running tasks or actors to avoid issues like OOM <core-patterns-limit-running-tasks>`.
