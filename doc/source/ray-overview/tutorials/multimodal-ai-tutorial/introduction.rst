.. _tutorial-multimodal-ai-introduction:

Introduction
============

Welcome to this comprehensive tutorial on Ray! In this tutorial we'll learn Ray's core concepts by building a practical multimodal AI application for dog breed classification and semantic search. This hands-on approach will help you understand how Ray's components work together to lay the foundation for scalable distributed applications by using the Ray libraries, specifically :ref:`Ray Data <data>`, :ref:`Ray Train <train-docs>`, and :ref:`Ray Serve <serve>`. These libraries are built on top of Ray Core and provide a higher level of abstraction for common applications of distributed computing.

By the end of this tutorial, we will have built a production-ready Ray application that:

1. Processes and generates embeddings for a dataset of dog images using **Ray Data**
2. Trains a classifier to identify dog breeds using **Ray Train**
3. Serves a semantic search API that can find similar dog images using **Ray Serve**

.. note::

    This tutorial is designed for people who are familiar with Python and the machine learning ecosystem, but new to Ray. If you are already experienced with Ray and just want to dive into the example code, see the :ref:`example notebook <ref-e2e-multimodal-ai-workloads>` instead.

Before we dive in, let's provide a quick introduction to Ray and how it helps us to build distributed applications.

What is Ray?
------------

Ray is a unified framework for scaling AI and Python applications. It provides a simple, universal API for distributed computing that can scale from your laptop to a large cluster. Ray provides the foundational API and concepts of :ref:`tasks <ray-remote-functions>`, :ref:`actors <ray-remote-classes>`, and :ref:`objects <objects-in-ray>`:

- **Task**: A remote function invocation that executes on a process different from the caller, and potentially on a different machine, by a worker. Tasks are defined by applying the `@ray.remote` decorator to a function.
- **Actor**: A stateful worker process that can run tasks and maintain state across task executions. Actors are defined by applying the `@ray.remote` decorator to a class.
- **Object**: Immutable values that are returned by a task or created directly. Objects are created by calling a remote function, or by calling the `ray.put` function.

These building blocks make up the core of Ray's distributed computing model, and are the same building blocks that power each of the Ray libraries. The Ray libraries offer APIs and functionality at a higher level of abstraction for common distributed computing applications. The Ray libraries include:

- **Ray Data**: for scalable data processing and ML training data loading
- **Ray Train**: for distributed model training
- **Ray Tune**: for hyperparameter tuning
- **Ray Serve**: for serving models, APIs, and other applications
- **Ray RLlib**: for reinforcement learning

Ray also powers aspects of several other libraries developed and maintained by the open source community. Popular ones include:

- **`vLLM<https://github.com/vllm-project/vllm>`_**: a high-performance LLM inference engine, using Ray for distributed multi-GPU and multi-node inference
- **`veRL<https://github.com/volcengine/verl>`_**: RL training library for large language models, using Ray for distribution of training tasks
- **`LLaMA-Factory<https://github.com/simplescaling/llama-factory>`_**: a framework for fine-tuning and serving LLaMA models, using Ray to support multi-node fine-tuning

To learn more about the Ray ecosystem, see the :ref:`Ray Ecosystem <ray-oss-list>` page.

For a more bottoms-up introduction to Ray, see the :ref:`Ray Core walkthrough <core-walkthrough>` instead. For a deep-dive into Ray's core concepts and architecture, see the `Ray v2 Architecture paper <https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview?tab=t.0#heading=h.iyrm5j2gcdoq>`_ instead.

Getting Started with Ray
------------------------

Let's start by installing Ray:

.. code-block:: bash

    pip3 install "ray[default]"

 In your local IDE, create a new file called `intro.py` and add the following code:

.. code-block:: python

    import ray

    # Initialize Ray
    ray.init()

    print(f"Ray initialized with {ray.available_resources()['CPU']} CPUs")

This code creates a very simple Ray application. When you run the  inside of a Ray cluster and prints the number of CPUs available to the application.

What is a Ray Cluster?
----------------------

A Ray cluster is the runtime environment for a Ray application:

.. image:: /cluster/images/ray-cluster.svg
    :alt: Ray Cluster Architecture
    :width: 50%
    :align: center

The cluster provides all of the components that enable Ray's scaling and task orchestration capabilities, and consists of any number of worker nodes connected to exactly one head node:

- **Head Node**: The central node that runs the driver process. Only one node in the cluster is designated as the head node. Optionally, the head node can also act as a worker node, but only one node in the cluster can be designated as the head node.
- **Worker Nodes**: Compute nodes that run the worker processes.

The head node runs the **driver** process, which is responsible for running the root Ray application program. The worker nodes run the **worker** processes, which are responsible for executing tasks.

.. note::

    In a single-node setup, the single node serves as both the head node and the single worker node. In production setups, it is not recommended to run the head node as a worker node to avoid performance bottlenecks.

Each node in the cluster runs a **Raylet** process, which is responsible for manging shared resources on the node. The two main components of the Raylet are the **Scheduler** and the **Object Store**:

- **Scheduler**: Responsible for resource management, task placement, and object storage.
- **Object Store**: Responsible for storing, transferring, and spilling large objects.

A key design point is the distributed nature of the Raylet, which allows Ray to make decisions about how to place tasks and actors based on the available resources on the node. This is in contrast to other distributed systems that use a centralized scheduler, which can become a bottleneck as the system scales. For a deep dive on how distributed scheduling works in Ray, see the `Resource Management and Scheduling section <https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview?tab=t.0#heading=h.59ui0zg51ggk>`_ of the Ray v2 Architecture paper.

The designated head node also runs the **Global Control Store (GCS)**, which acts as the control plane for a Ray cluster and is responsible for managing cluster metadata and scheduling. The GCS provides a centralized source of truth for the cluster's state, and is used by external services such as the auto-scaler and the Ray dashboard. For a deep dive on the GCS, see the `GCS section <https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview?tab=t.0#heading=h.pbfdfypjcdk4>`_ of the Ray v2 Architecture paper.

.. warning::

    The GCS is a single point of failure for the cluster, and is a single point of contention for the cluster. For production deployments, adding GCS fault tolerance is a best practice. GCS fault tolerance is easy to configure out of the box using `Anyscale's production-ready managed platform for Ray <https://www.anyscale.com/>`.

When a Ray cluster does not have sufficient resources to run a task, Ray will automatically scale the cluster by adding worker nodes. This is done by the **auto-scaler**, which is an external service that is responsible for scaling the cluster based on the available resources and the demand for resources, depending on how the Ray cluster is deployed. For a deep dive on the auto-scaler, see the `Cluster Management section <https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview?tab=t.0#heading=h.na6spx4shhux>`_ of the Ray v2 Architecture paper.

Finally, the head node also runs the **Ray Dashboard**, which is a web-based UI for monitoring and managing the Ray cluster. The Ray Dashboard provides a centralized view of the cluster's state, and is used to monitor the cluster's health and performance. For a deep dive on the Ray Dashboard, see the `Dashboard section <https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview?tab=t.0#heading=h.pbfdfypjcdk4>`_ of the Ray v2 Architecture paper.

.. note::

    The Ray Dashboard can be integrated with Prometheus and Grafana to provide a comprehensive view of the cluster's metrics and performance. Anyscale provides a managed service for Prometheus and Grafana, which comes preconfigured with the Ray Dashboard out of the box and offers persistent metrics beyond the lifespan of the Ray cluster. If you are not using Anyscale, configuring Prometheus and Grafana to work with the Ray Dashboard is a best practice, but is outside the scope of this tutorial.

We will use the Ray dashboard throughout the tutorial to provide visibility into the Ray cluster and what the Ray application is doing under the hood.

Running a Simple Ray Application
--------------------------------

Let's create a simple Ray application that runs a task and prints the result:

.. code-block:: python

    import ray
    from time import sleep

    # Run a simple task
    @ray.remote
    def hello_world():
        sleep(300)
        return "Hello, World!"

    print(ray.get(hello_world.remote()))

This code creates a remote task than sleeps for 5 minutes. Let's explore the Ray dashboard to see what's happening under the hood. 

.. warning::

    The Ray dashboard is only available while the Ray cluster is running. Unless you are using a persistent Ray cluster, the cluster will be destroyed when the main process exits.

While the Ray application is running, visit the Ray dashboard in your browser at `http://localhost:8265/`. You can see that the job is running under the "Recent jobs" section of the Overview tab:

.. image:: /ray-overview/tutorials/multimodal-ai-tutorial/images/dashboard-jobs.png
    :alt: Recently submitted jobs
    :width: 25%
    :align: center

Clicking into the job, we can see that the task that we created is running:

.. image:: /ray-overview/tutorials/multimodal-ai-tutorial/images/dashboard-job-detail.png

Throughout this tutorial, we will use the Ray dashboard to provide visibility into what is happening under the hood of the Ray application.

Next Steps
----------

In the next section, we'll dive into data processing with Ray Data. We'll learn how to:

- Load and preprocess our dog breed dataset
- Generate embeddings efficiently using distributed processing
- Store and manage our processed data

Ready to begin? Let's move on to :doc:`data-processing`! 