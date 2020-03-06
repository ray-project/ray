Ray Dashboard
==============
Welcome to the Ray dashboard page! 

Ray's built-in dashboard provides metrics, charts, and other features that help Ray users to understand Ray clusters and libraries.
This includes cluster node level metrics, actor level metrics, configurations, profiling & killing actor buttons, and Ray library metrics such as Tune metrics. 
Check out the documentation to understand a rich set of features we offer.

Getting Started
---------------
  
Access Dashboard
~~~~~~~~~~~~~~~~
If you start Ray on your laptop, dashboard is running by default, and you can access it through its URL. 
By default the URL is `localhost:8265`. Note that the port will be changed if the default port is not available.

The URL is printed when Ray starts up.

.. code-block:: text

  INFO resource_spec.py:212 -- Starting Ray with 4.2 GiB memory available for workers and up to 2.12 GiB for objects. You can adjust these settings with ray.init(memory=<bytes>, object_store_memory=<bytes>).
  INFO services.py:1093 -- View the Ray dashboard at localhost:8265

Dashboard is also available when using autoscaler. Check out `Monitoring Autoscaler Status <autoscaling.html#monitoring-cluster-status>`_ for more details.

Example Usage
-------------

To understand each components, look at `Dashboard Components <ray-dashboard.html#components>`_

Debugging a Blocked Actor
~~~~~~~~~~~~~~~~~~~~~~~~~
If creating an actor requires resources (e.g., CPUs, GPUs, or other custom resources) that are not currently available, the actor cannot be created until those resources are added to the cluster or become available.
This can cause an application to hang. To alert you to this issue, infeasible tasks are shown in red in the dashboard, and pending tasks are shown in yellow.
You can find this information from the `Logical View` tab. 

- **Infeasible Actor Creation** happens when an actor requires more resources than a Ray cluster can provide.
- **Pending Actor Creation** happens when there is no available resource for this actor because it is already taken by other tasks / actors.

.. code-block:: python
  
  ray.init(num_gpus=2)

  @ray.remote(num_gpus=1)
  class Actor1:
      def __init__(self):
          pass
  
  @ray.remote
  class Actor2:
      def __init__(self):
          self.actor1_list = [Actor1.remote() for _ in range(4)]

  @ray.remote(num_gpus=4)
  class Actor3:
      def __init__(self):
          pass

  actor2 = Actor2.remote()
  actor3 = Actor3.remote()


.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/dashboard-pending-infeasible-actors.png
    :align: center

This cluster has 2 GPUs. Actor1 requires 1 GPU to be created. Actor2 creates 4 Actor1. As a result of calling `Actor2.remote()`, 2 Actor1 creation tasks became pending. 

You can also see it is infeasible to create Actor3 because it requires 4 GPUs which is bigger than the total gpus available in this cluster (2 GPUs). 

Inspect Local Memory Usage
~~~~~~~~~~~~~~~~~~~~~~~~~~
The dashboard displays the following information about local memory usage:

- Number of object IDs in scope
- Number of local objects
- Used Object Memory

In the example below, all objects (strings) are stored in local object memory. Used local object memory increases as the remote function ``g`` is repeatedly called.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-inspect-local-memory-usage.png
    :align: center

.. code-block:: python

  @ray.remote
  def g():
      return "hello world!"
  
  @ray.remote
  class A:
      def f(self):
          self.object_ids = []
          for idx in range(50):
              ray.show_in_webui("Loop index = {}...".format(idx))
              self.object_ids.append(g.remote())
              time.sleep(0.5)
  
  a = A.remote()
  a.f.remote()


Inspect Node Memory Usage
~~~~~~~~~~~~~~~~~~~~~~~~~
In this example, you can see local object memory is not used because objects are stored on the node (Plasma Storage) through `ray.put`.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-inspect-node-memory-usage.png
    :align: center

.. code-block:: python

  @ray.remote
  class C(object):
      def __init__(self):
          self.object_ids = []
      
      def push(self):
          object_id = ray.put("test")
          self.object_ids.append(object_id)
          time.sleep(1)
          return object_id
      
      def clean_memory(self):
          del self.object_ids
          
  @ray.remote
  class D(object):
      def __init__(self):
          self.object_ids = []
  
      def fetch(self):
          c = C.remote()
          
          for idx in range(20):
              ray.show_in_webui("Loop index = {}...".format(idx))
              time.sleep(0.5)
              object_id = ray.get(c.push.remote())
              self.object_ids.append(object_id)  
  
      def clean_memory(self):
          del self.object_ids
  
  d = D.remote()
  _ = d.fetch.remote()

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
-----------

Machine View
~~~~~~~~~~~~
The machine view provides node and process level information. You can see resource usage statistics for each node and each worker process.

Logical View (Experimental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The logical view renders actor information in a tree format. To illustrate this, in the code block below, the ``Parent`` actor creates two ``Child`` actors and each ``Child`` actor creates one ``GrandChild`` actor.
This relationship will be visible at a dashboard logical view page.

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

For more information about what logical view provides, checkout `Dashboard Usage <ray-dashboard.html#example-usage>`_


Ray Config
~~~~~~~~~~~~
If you start your Ray cluster using the autoscaler, the cluster configuration will be displayed in this tab.


Tune (Experimental)
~~~~~~~~~~~~~~~~~~~
When Tune is running, you can find information for each Tune trial in this tab.