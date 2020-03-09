Ray Dashboard
=============
Ray's built-in dashboard provides metrics, charts, and other features that help Ray users to understand Ray clusters and libraries.
This includes node level metrics, actor level metrics, cluster configuration information, tools for profiling and killing actors, and metrics for Ray libraries such as Tune.

Getting Started
---------------
If you start Ray on your laptop, the dashboard runs by default, and you can access it through its URL. 
By default the URL is **localhost:8265**. Note that the port will be changed if the default port is not available.

The URL is printed when Ray starts up.

.. code-block:: text

  INFO services.py:1093 -- View the Ray dashboard at localhost:8265

The dashboard is also available when using the autoscaler. Read about how to `use the dashboard with the autoscaler <autoscaling.html#monitoring-cluster-status>`_.

Example Usage
-------------

Debugging a Blocked Actor
~~~~~~~~~~~~~~~~~~~~~~~~~
If creating an actor requires resources (e.g., CPUs, GPUs, or other custom resources) that are not currently available, the actor cannot be created until those resources are added to the cluster or become available.
This can cause an application to hang. To alert you to this issue, infeasible tasks are shown in red in the dashboard, and pending tasks are shown in yellow.
You can find this information from the *Logical View* tab in the dashboard.

- **Infeasible Actor Creation** happens when an actor requires more resources than a Ray cluster can provide.
- **Pending Actor Creation** happens when there is no available resource for this actor because it is already taken by other tasks / actors.

.. code-block:: python

  import ray
  
  ray.init(num_gpus=2)

  @ray.remote(num_gpus=1)
  class Actor1:
      def __init__(self):
          pass

  @ray.remote(num_gpus=4)
  class Actor2:
      def __init__(self):
          pass

  actor1_list = [Actor1.remote() for _ in range(4)]
  actor2 = Actor2.remote()


.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/dashboard-pending-infeasible-actors.png
    :align: center

This cluster has two GPUs, and so it only has room to create two copies of Actor1. As a result, the rest of Actor1 will be pending.

You can also see it is infeasible to create Actor2 because it requires 4 GPUs which is bigger than the total gpus available in this cluster (2 GPUs). 

Inspect Memory Usage
~~~~~~~~~~~~~~~~~~~~
The logical view displays the following information about memory usage

- **Number of object IDs in scope**: number of local and remote objectIDs which actors can access.
- **Number of local objects**: Ray objects in a worker memory where actor resides in.
- **Used Local Object Memory**: Memory used by local objects in bytes.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/dashboard-inspect-local-memory-usage
    :align: center

Let's see how to use this information.

**Number of object IDs in scope keeps increasing.**
Number of object IDs in scope indicates the total number of object IDs this actor can access to. Object IDs that have reference to big objects are in plasma store, and others are staying in local memory. "In scope" means that object IDs are accessible by this actor. That typically means objectID is referred by some varaibles in actor code. 
If this number keeps increasing as you run your Ray cluster, that could mean that your objectIDs are not properly garbage collected. This can lead to OOM errors or eviction of objectIDs that your program still wants to use. 


**Number of local objects are way less than number of object IDs in scope**
This means many of objectIDs are referring to objects residing in plasma stores. It can happen either when objects are big or when you use ``ray.put``. 
If you have too many big objects and if your program uses plasma store heaviily, the program could be slower becasue using plasma stores is slower than local memory.

Profiling (Experimental)
~~~~~~~~~~~~~~~~~~~~~~~~

.. note::

  The profiling button currently only works when you use **passwordless** ``sudo``. 
  It is still experimental. Please report any issues you run into.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-profiling-buttons.png
    :align: center

Clicking one of the profiling buttons on the dashboard launches py-spy, which will profile your actor process for the given duration. Once the profiling has been done, you can click the "profiling result" button to visualize the profiling information as a flamegraph.
This visualization can help reveal computational bottlenecks.

More information on how to interpret the flamegraph is available at https://github.com/jlfwong/speedscope#usage.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-profiling.png
    :align: center

Components
----------

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-basic.png
    :align: center

Machine View
~~~~~~~~~~~~
The machine view provides node and process level information. You can see resource usage statistics for each node and each worker process.

Logical View (Experimental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~
The logical view renders actor information in a tree format. To illustrate this, in the code block below, the ``Parent`` actor creates two ``Child`` actors and each ``Child`` actor creates one ``GrandChild`` actor.
This relationship is visible in the dashboard *Logical View* tab.

.. code-block:: python

  import ray
  ray.init()

  @ray.remote
  class Grandchild:
      def __init__(self):
          pass

  @ray.remote
  class Child:
      def __init__(self):
          self.grandchild_handle = Grandchild.remote()
  
  @ray.remote
  class Parent:
      def __init__(self):
          self.children_handles = [Child.remote() for _ in range(2)]

  parent_handle = Parent.remote()

You can see that the dashboard shows the parent/child relationship as expected. 

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-tree-actors.png
    :align: center

Ray Config
~~~~~~~~~~~~
If you start your Ray cluster using the autoscaler, the cluster configuration will be displayed in this tab.


Tune (Experimental)
~~~~~~~~~~~~~~~~~~~
When Tune is running, you can find information for each Tune trial in this tab.