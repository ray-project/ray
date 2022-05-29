.. _resource-requirements:

Specifying Required Resources
=============================

Oftentimes, you may want to specify a task's resource requirements (for example
one task may require a GPU).
Ray will automatically detect the available GPUs and CPUs on the machine (see :ref:`Configuring Ray <configuring-ray>` for more details).

Ray allows specifying a task's resources requirements (e.g., CPU, GPU, and custom resources).
The task will only run on a machine if there are enough resources
available to execute the task.

.. tabbed:: Python

    .. code-block:: python

        # Specify required resources.
        @ray.remote(num_cpus=4, num_gpus=2)
        def my_function():
            return 1

.. tabbed:: Java

    .. code-block:: java

        // Specify required resources.
        Ray.task(MyRayApp::myFunction).setResource("CPU", 1.0).setResource("GPU", 4.0).remote();

.. tabbed:: C++

    .. code-block:: c++

        // Specify required resources.
        ray::Task(MyFunction).SetResource("CPU", 1.0).SetResource("GPU", 4.0).Remote();

.. note::

    * If you do not specify any resources, the default is 1 CPU resource and
      no other resources.
    * If specifying CPUs, Ray does not enforce isolation (i.e., your task is
      expected to honor its request).
    * If specifying GPUs, Ray does provide isolation in forms of visible devices
      (setting the environment variable ``CUDA_VISIBLE_DEVICES``), but it is the
      task's responsibility to actually use the GPUs (e.g., through a deep
      learning framework like TensorFlow or PyTorch).

The resource requirements of a task have implications for the Ray's scheduling
concurrency. In particular, the sum of the resource requirements of all of the
concurrently executing tasks on a given node cannot exceed the node's total
resources.

Below are more examples of resource specifications:

.. tabbed:: Python

    .. code-block:: python

        # Ray also supports fractional resource requirements.
        @ray.remote(num_gpus=0.5)
        def h():
            return 1

        # Ray support custom resources too.
        @ray.remote(resources={'Custom': 1})
        def f():
            return 1

.. tabbed:: Java

    .. code-block:: java

        // Ray aslo supports fractional and custom resources.
        Ray.task(MyRayApp::myFunction).setResource("GPU", 0.5).setResource("Custom", 1.0).remote();

.. tabbed:: C++

    .. code-block:: c++

        // Ray aslo supports fractional and custom resources.
        ray::Task(MyFunction).SetResource("GPU", 0.5).SetResource("Custom", 1.0).Remote();

.. tip::

  Besides compute resources, you can also specify an environment for a task to run in,
  which can include Python packages, local files, environment variables, and more---see :ref:`Runtime Environments <runtime-environments>` for details.
