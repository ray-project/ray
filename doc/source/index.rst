What is Ray?
============

.. include:: ray-overview/basics.rst

Getting Started with Ray
------------------------

Check out :ref:`gentle-intro` to learn more about Ray and its ecosystem of libraries that enable things like distributed hyperparameter tuning,
reinforcement learning, and distributed training.

Ray provides Python, Java, and *EXPERIMENTAL* C++ API. And Ray uses Tasks (functions) and Actors (Classes) to allow you to parallelize your code.

.. tabs::
  .. group-tab:: Python

    .. code-block:: python

        # First, run `pip install ray`.

        import ray
        ray.init()

        @ray.remote
        def f(x):
            return x * x

        futures = [f.remote(i) for i in range(4)]
        print(ray.get(futures)) # [0, 1, 4, 9]

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

  .. group-tab:: Java

    First, add the `ray-api <https://mvnrepository.com/artifact/io.ray/ray-api>`__ and `ray-runtime <https://mvnrepository.com/artifact/io.ray/ray-runtime>`__ dependencies in your project.

    .. code-block:: java

        import io.ray.api.ActorHandle;
        import io.ray.api.ObjectRef;
        import io.ray.api.Ray;
        import java.util.ArrayList;
        import java.util.List;
        import java.util.stream.Collectors;

        public class RayDemo {

          public static int square(int x) {
            return x * x;
          }

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
            {
              List<ObjectRef<Integer>> objectRefList = new ArrayList<>();
              // Invoke the `square` method 4 times remotely as Ray tasks.
              // The tasks will run in parallel in the background.
              for (int i = 0; i < 4; i++) {
                objectRefList.add(Ray.task(RayDemo::square, i).remote());
              }
              // Get the actual results of the tasks with `get`.
              System.out.println(Ray.get(objectRefList));  // [0, 1, 4, 9]
            }

            {
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
        }

  .. group-tab:: C++

    | The C++ Ray API is currently experimental with limited support. You can track its development `here <https://github.com/ray-project/ray/milestone/17>`__ and report issues on GitHub.
    | Run the following commands to get started:

    | - Install ray with C++ API support and generate a bazel project with the ray command.

    .. code-block:: shell

      pip install "ray[cpp]"
      mkdir ray-template && ray cpp --generate-bazel-project-template-to ray-template

    | - The project template comes with a simple example application. You can try this example out in 2 ways:
    | - 1. Run the example application directly, which will start a Ray cluster locally.

    .. code-block:: shell

      cd ray-template && bash run.sh

    | - 2. Connect the example application to an existing Ray cluster by specifying the RAY_ADDRESS env var.

    .. code-block:: shell

      ray start --head
      RAY_ADDRESS=127.0.0.1:6379 bash run.sh

    | - Now you can build your own Ray C++ application based on this project template.

You can also get started by visiting our `Tutorials <https://github.com/ray-project/tutorial>`_. For the latest wheels (nightlies), see the `installation page <installation.html>`__.


Getting Involved
================

.. include:: ray-overview/involvement.rst

If you're interested in contributing to Ray, visit our page on :ref:`Getting Involved <getting-involved>` to read about the contribution process and see what you can work on!


More Information
================

Here are some talks, papers, and press coverage involving Ray and its libraries. Please raise an issue if any of the below links are broken, or if you'd like to add your own talk!

Blog and Press
--------------

  - `Modern Parallel and Distributed Python: A Quick Tutorial on Ray <https://towardsdatascience.com/modern-parallel-and-distributed-python-a-quick-tutorial-on-ray-99f8d70369b8>`_
  - `Why Every Python Developer Will Love Ray <https://www.datanami.com/2019/11/05/why-every-python-developer-will-love-ray/>`_
  - `Ray: A Distributed System for AI (BAIR) <http://bair.berkeley.edu/blog/2018/01/09/ray/>`_
  - `10x Faster Parallel Python Without Python Multiprocessing <https://towardsdatascience.com/10x-faster-parallel-python-without-python-multiprocessing-e5017c93cce1>`_
  - `Implementing A Parameter Server in 15 Lines of Python with Ray <https://ray-project.github.io/2018/07/15/parameter-server-in-fifteen-lines.html>`_
  - `Ray Distributed AI Framework Curriculum <https://rise.cs.berkeley.edu/blog/ray-intel-curriculum/>`_
  - `RayOnSpark: Running Emerging AI Applications on Big Data Clusters with Ray and Analytics Zoo <https://medium.com/riselab/rayonspark-running-emerging-ai-applications-on-big-data-clusters-with-ray-and-analytics-zoo-923e0136ed6a>`_
  - `First user tips for Ray <https://rise.cs.berkeley.edu/blog/ray-tips-for-first-time-users/>`_
  - [Tune] `Tune: a Python library for fast hyperparameter tuning at any scale <https://towardsdatascience.com/fast-hyperparameter-tuning-at-scale-d428223b081c>`_
  - [Tune] `Cutting edge hyperparameter tuning with Ray Tune <https://medium.com/riselab/cutting-edge-hyperparameter-tuning-with-ray-tune-be6c0447afdf>`_
  - [RLlib] `New Library Targets High Speed Reinforcement Learning <https://www.datanami.com/2018/02/01/rays-new-library-targets-high-speed-reinforcement-learning/>`_
  - [RLlib] `Scaling Multi Agent Reinforcement Learning <http://bair.berkeley.edu/blog/2018/12/12/rllib/>`_
  - [RLlib] `Functional RL with Keras and Tensorflow Eager <https://bair.berkeley.edu/blog/2019/10/14/functional-rl/>`_
  - [Modin] `How to Speed up Pandas by 4x with one line of code <https://www.kdnuggets.com/2019/11/speed-up-pandas-4x.html>`_
  - [Modin] `Quick Tip – Speed up Pandas using Modin <https://pythondata.com/quick-tip-speed-up-pandas-using-modin/>`_
  - `Ray Blog`_

.. _`Ray Blog`: https://ray-project.github.io/

Talks (Videos)
--------------

 - `Unifying Large Scale Data Preprocessing and Machine Learning Pipelines with Ray Datasets | PyData 2021 <https://zoom.us/rec/share/0cjbk_YdCTbiTm7gNhzSeNxxTCCEy1pCDUkkjfBjtvOsKGA8XmDOx82jflHdQCUP.fsjQkj5PWSYplOTz?startTime=1635456658000>`_ `(slides) <https://docs.google.com/presentation/d/19F_wxkpo1JAROPxULmJHYZd3sKryapkbMd0ib3ndMiU/edit?usp=sharing>`_
 - `Programming at any Scale with Ray | SF Python Meetup Sept 2019 <https://www.youtube.com/watch?v=LfpHyIXBhlE>`_
 - `Ray for Reinforcement Learning | Data Council 2019 <https://www.youtube.com/watch?v=Ayc0ca150HI>`_
 - `Scaling Interactive Pandas Workflows with Modin <https://www.youtube.com/watch?v=-HjLd_3ahCw>`_
 - `Ray: A Distributed Execution Framework for AI | SciPy 2018 <https://www.youtube.com/watch?v=D_oz7E4v-U0>`_
 - `Ray: A Cluster Computing Engine for Reinforcement Learning Applications | Spark Summit <https://www.youtube.com/watch?v=xadZRRB_TeI>`_
 - `RLlib: Ray Reinforcement Learning Library | RISECamp 2018 <https://www.youtube.com/watch?v=eeRGORQthaQ>`_
 - `Enabling Composition in Distributed Reinforcement Learning | Spark Summit 2018 <https://www.youtube.com/watch?v=jAEPqjkjth4>`_
 - `Tune: Distributed Hyperparameter Search | RISECamp 2018 <https://www.youtube.com/watch?v=38Yd_dXW51Q>`_

Slides
------

- `Talk given at UC Berkeley DS100 <https://docs.google.com/presentation/d/1sF5T_ePR9R6fAi2R6uxehHzXuieme63O2n_5i9m7mVE/edit?usp=sharing>`_
- `Talk given in October 2019 <https://docs.google.com/presentation/d/13K0JsogYQX3gUCGhmQ1PQ8HILwEDFysnq0cI2b88XbU/edit?usp=sharing>`_
- [Tune] `Talk given at RISECamp 2019 <https://docs.google.com/presentation/d/1v3IldXWrFNMK-vuONlSdEuM82fuGTrNUDuwtfx4axsQ/edit?usp=sharing>`_

Papers
------

- `Ray 1.0 Architecture whitepaper`_ **(new)**
- `Ray Design Patterns`_ **(new)**
- `RLlib paper`_
- `RLlib flow paper`_
- `Tune paper`_

*Older papers:*

- `Ray paper`_
- `Ray HotOS paper`_

.. _`Ray 1.0 Architecture whitepaper`: https://docs.google.com/document/d/1lAy0Owi-vPz2jEqBSaHNQcy2IBSDEHyXNOQZlGuj93c/preview
.. _`Ray Design Patterns`: https://docs.google.com/document/d/167rnnDFIVRhHhK4mznEIemOtj63IOhtIPvSYaPgI4Fg/edit
.. _`Ray paper`: https://arxiv.org/abs/1712.05889
.. _`Ray HotOS paper`: https://arxiv.org/abs/1703.03924
.. _`RLlib paper`: https://arxiv.org/abs/1712.09381
.. _`RLlib flow paper`: https://arxiv.org/abs/2011.12719
.. _`Tune paper`: https://arxiv.org/abs/1807.05118

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Overview of Ray

   ray-overview/index.rst
   ray-libraries.rst
   installation.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Ray Core

   walkthrough.rst
   using-ray.rst
   configure.rst
   ray-dashboard.rst
   Tutorial and Examples <auto_examples/overview.rst>
   Design patterns and anti-patterns <ray-design-patterns/index.rst>
   package-ref.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Multi-node Ray

   cluster/index.rst
   cluster/quickstart.rst
   cluster/guide.rst
   Ray Job Submission <ray-job-submission/overview.rst>
   cluster/ray-client.rst
   cluster/reference.rst
   cluster/cloud.rst
   cluster/deploy.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Ray Serve

   serve/index.rst
   serve/tutorial.rst
   serve/core-apis.rst
   serve/http-servehandle.rst
   serve/deployment.rst
   serve/ml-models.rst
   serve/pipeline.rst
   serve/performance.rst
   serve/architecture.rst
   serve/tutorials/index.rst
   serve/faq.rst
   serve/package-ref.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Ray Data

   data/dataset.rst
   data/dataset-pipeline.rst
   data/dataset-ml-preprocessing.rst
   data/dataset-execution-model.rst
   data/dataset-tensor-support.rst
   data/package-ref.rst
   data/examples/big_data_ingestion
   data/dask-on-ray.rst
   data/mars-on-ray.rst
   data/modin/index.rst
   data/raydp.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Ray Workflows

   workflows/concepts.rst
   workflows/basics.rst
   workflows/management.rst
   workflows/actors.rst
   workflows/metadata.rst
   workflows/events.rst
   workflows/comparison.rst
   workflows/advanced.rst
   workflows/package-ref.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Ray Tune

   tune/index.rst
   tune/key-concepts.rst
   tune/user-guide.rst
   tune/tutorials/overview.rst
   tune/examples/index.rst
   tune/api_docs/overview.rst
   tune/contrib.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Ray RLlib

   rllib/index.rst
   rllib-toc.rst
   rllib/core-concepts.rst
   rllib-training.rst
   rllib-env.rst
   rllib-models.rst
   rllib-algorithms.rst
   rllib-sample-collection.rst
   rllib-offline.rst
   rllib-concepts.rst
   rllib-examples.rst
   rllib/package_ref/index.rst
   rllib-dev.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Ray Train

   train/train.rst
   train/user_guide.rst
   train/examples.rst
   train/architecture.rst
   train/api.rst
   train/migration-guide.rst
   RaySGD v1: Distributed Training Wrappers <raysgd/raysgd.rst>

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: More Libraries

   multiprocessing.rst
   joblib.rst
   xgboost-ray.md
   lightgbm-ray.rst
   ray-lightning.rst
   ray-collective.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Observability

   ray-metrics.rst
   ray-debugging.rst
   ray-logging.rst
   ray-tracing.rst

.. toctree::
   :hidden:
   :maxdepth: -1
   :caption: Contributor Guide

   getting-involved.rst
   development.rst
   fake-autoscaler.rst
   whitepaper.rst
   debugging.rst
   profiling.rst
