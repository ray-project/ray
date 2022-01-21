.. _gentle-intro:

============================
A Gentle Introduction to Ray
============================

.. include:: basics.rst

This tutorial will provide a tour of the core features of Ray.

Ray provides a Python and Java API.
To use Ray in Python, first install Ray with: ``pip install ray``.
To use Ray in Java, first add the `ray-api <https://mvnrepository.com/artifact/io.ray/ray-api>`__ and `ray-runtime <https://mvnrepository.com/artifact/io.ray/ray-runtime>`__ dependencies in your project.
Then we can use Ray to parallelize your program.

Parallelizing Python/Java Functions with Ray Tasks
==================================================

.. tabs::
  .. group-tab:: Python

    First, import ray and ``init`` the Ray service.
    Then decorate your function with ``@ray.remote`` to declare that you want to run this function
    remotely. Lastly, call that function with ``.remote()`` instead of calling it normally. This remote call yields a future, or ``ObjectRef`` that you can then
    fetch with ``ray.get``.

    .. code-block:: python

        import ray
        ray.init()

        @ray.remote
        def f(x):
            return x * x

        futures = [f.remote(i) for i in range(4)]
        print(ray.get(futures)) # [0, 1, 4, 9]

  .. group-tab:: Java

    First, use ``Ray.init`` to initialize Ray runtime.
    Then you can use ``Ray.task(...).remote()`` to convert any Java static method into a Ray task. The task will run asynchronously in a remote worker process. The ``remote`` method will return an ``ObjectRef``, and you can then fetch the actual result with ``get``.

    .. code-block:: java

      import io.ray.api.ObjectRef;
      import io.ray.api.Ray;
      import java.util.ArrayList;
      import java.util.List;

      public class RayDemo {

        public static int square(int x) {
          return x * x;
        }

        public static void main(String[] args) {
          // Intialize Ray runtime.
          Ray.init();
          List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
          // Invoke the `square` method 4 times remotely as Ray tasks.
          // The tasks will run in parallel in the background.
          for (int i = 0; i < 4; i++) {
            objectRefList.add(Ray.task(RayDemo::square, i).remote());
          }
          // Get the actual results of the tasks.
          System.out.println(Ray.get(objectRefList));  // [0, 1, 4, 9]
        }
      }

    In the above code block we defined some Ray Tasks. While these are great for stateless operations, sometimes you
    must maintain the state of your application. You can do that with Ray Actors.

Parallelizing Python/Java Classes with Ray Actors
=================================================

Ray provides actors to allow you to parallelize an instance of a class in Python/Java.
When you instantiate a class that is a Ray actor, Ray will start a remote instance
of that class in the cluster. This actor can then execute remote method calls and
maintain its own internal state.

.. tabs::
  .. code-tab:: python

    import ray
    ray.init() # Only call this once.

    @ray.remote
    class Counter(object):
        def __init__(self):
            self.n = 0

        def increment(self):
            self.n += 1

        def read(self):
            return self.n

    counters = [Counter.remote() for i in range(4)]
    [c.increment.remote() for c in counters]
    futures = [c.read.remote() for c in counters]
    print(ray.get(futures)) # [1, 1, 1, 1]

  .. code-tab:: java

    import io.ray.api.ActorHandle;
    import io.ray.api.ObjectRef;
    import io.ray.api.Ray;
    import java.util.ArrayList;
    import java.util.List;
    import java.util.stream.Collectors;

    public class RayDemo {

      public static class Counter {

        private int value = 0;

        public void increment() {
          this.value += 1;
        }

        public int read() {
          return this.value;
        }
      }

      public static void main(String[] args) {
        // Intialize Ray runtime.
        Ray.init();
        List<ActorHandle<Counter>> counters = new ArrayList<>();
        // Create 4 actors from the `Counter` class.
        // They will run in remote worker processes.
        for (int i = 0; i < 4; i++) {
          counters.add(Ray.actor(Counter::new).remote());
        }

        // Invoke the `increment` method on each actor.
        // This will send an actor task to each remote actor.
        for (ActorHandle<Counter> counter : counters) {
          counter.task(Counter::increment).remote();
        }
        // Invoke the `read` method on each actor, and print the results.
        List<ObjectRef<Integer>> objectRefList = counters.stream()
            .map(counter -> counter.task(Counter::read).remote())
            .collect(Collectors.toList());
        System.out.println(Ray.get(objectRefList));  // [1, 1, 1, 1]
      }
    }

An Overview of the Ray Libraries
================================

Ray has a rich ecosystem of libraries and frameworks built on top of it. The main ones being:

- :doc:`../tune/index`
- :ref:`rllib-index`
- :ref:`train-docs`
- :ref:`rayserve`


Tune Quick Start
----------------

`Tune`_ is a library for hyperparameter tuning at any scale. With Tune, you can launch a multi-node distributed hyperparameter sweep in less than 10 lines of code. Tune supports any deep learning framework, including PyTorch, TensorFlow, and Keras.

.. note::

    To run this example, you will need to install the following:

    .. code-block:: bash

        $ pip install "ray[tune]"


This example runs a small grid search with an iterative training function.

.. literalinclude:: ../../../python/ray/tune/tests/example.py
   :language: python
   :start-after: __quick_start_begin__
   :end-before: __quick_start_end__

If TensorBoard is installed, automatically visualize all trial results:

.. code-block:: bash

    tensorboard --logdir ~/ray_results

.. _`Tune`: tune.html

RLlib Quick Start
-----------------

`RLlib`_ is an industry-grade library for reinforcement learning (RL) built on top of Ray.
RLlib offers high scalability and unified APIs for a variety of industry- and research applications.

**Quick Installation:**

.. code-block:: bash

    $ pip install "ray[rllib]" tensorflow  # or torch

**Example Script:**

.. literalinclude:: ../../../rllib/examples/documentation/rllib_on_ray_readme.py
   :language: python
   :start-after: __quick_start_begin__
   :end-before: __quick_start_end__

.. _`RLlib`: rllib/index.html

Where to go next?
=================


Visit the :ref:`Walkthrough <core-walkthrough>` page a more comprehensive overview of Ray features.

Ray programs can run on a single machine, and can also seamlessly scale to large clusters. To execute the above Ray script in the cloud, just download `this configuration file <https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/aws/example-full.yaml>`__, and run:

``ray submit [CLUSTER.YAML] example.py --start``

Read more about :ref:`launching clusters <cluster-index>`.
