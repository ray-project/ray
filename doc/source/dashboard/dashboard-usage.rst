Example Usage
=============

Let's learn the basic components before learning various dashboard usages.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/dashboard-basic.png
    :align: center

There are three views:

- **Machine View**, which groups actors by nodes. Information about occupied and total memory, resources, node ip address, logs, errors, etc. will be displayed.
- **Logical View**, which groups actors by the hierachical structure. Actor A is the parent of actor B if A creates B. In this case, actor B will be placed as a nested actor of A.
- **Ray Config**, which shows a cluster configuration.

We will walkthrough some common problems you can face and demonstrate how to debug them.

.. note::

  Tune metrics are also accessible through a dashboard if Tune is running in a cluster. 

Debug Blocked Actor 
--------------------
If creating an actor requires resources (e.g. CPUs, GPUs, other custom resources) that are not currently available, the actor creation task becomes infeasible or pending. 
It might cause hanging programs. To make developers aware of this issue, infeasible tasks are shown in red in the dashboard, and pending tasks are shown in yellow in the dashboard.
You can find this information from the `Logical View` tab. 

- **Infeasible Actor Creation** happens when an actor requires more resources than a Ray cluster can provide.
- **Pending Actor Creation** happens when there is no available resource for this actor because it is already taken by other tasks / actors.

Example 1
~~~~~~~~~~

.. code-block:: python
  
  ray.init(num_gpus=2)
  @ray.remote(num_gpus=1)
  class Actor1:
      def __init__(self):
          pass
  
  @ray.remote
  class Actor2:
      def __init__(self):
          actor1_list = [Actor1.remote() for _ in range(4)]

  @ray.remote(num_gpus=4)
  class Actor4:
      def __init__(self):
          pass

  actor2 = Actor2.remote()
  actor4 = Actor4.remote()


.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/dashboard-pending-infeasible-actors.png
    :align: center

This cluster has 2 GPUs. Actor1 requires 1 GPU to be created. Actor2 creates 4 Actor1. As a result of calling `Actor2.remote()`, 2 Actor1 creation tasks became pending. 

You can also see it is infeasible to create Actor4 because it requires 4 GPUs which is bigger than the total gpus available in this cluster (2 GPUs). 

Example 2
~~~~~~~~~~
The code snipet below demonstrates a situation where the session hangs because `A` cannot be created. It is because it requires a `"Custom"` resources that are not available in the cluster.

.. code-block:: python

  @ray.remote(resources={"Custom": 1}, num_cpus=0)
  class A(object):
      def __init__(self):
          pass
      
      def f(self):
          return 0
  
  @ray.remote
  class B(object):
      def __init__(self):
          self.a = A.remote(3, y=5)
          
      def f(self):
          return ray.get(self.a.f.remote())
  
  b = B.remote()
  
  try:
      ray.get(b.f.remote(), timeout=2)
  except ray.exceptions.RayTimeoutError:
      print("Session hangs because actor A cannot be created. ")

.. code-block:: bash

  2020-01-24 15:24:29,294	WARNING worker.py:1063 -- The actor or task with ID ffffffffffffffff1cc4b74c0100 is infeasible and cannot 
  currently be scheduled. It requires {Custom: 1.000000} for execution and {Custom: 1.000000} for placement, however there are no nodes 
  in the cluster that can provide the requested resources. To resolve this issue, consider reducing the resource requests of this task or 
  add nodes that can fit the task.
  Session hangs because actor A cannot be created. 

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/dashboard/dashboard-infeasible-actor-example-2.png
    :align: center


Inspect Local Memory Usage
--------------------------
The dashboard shows the following informaiton of local memory usage:

- Number of object ids in scope
- Number of local objects
- Used Object Memory

In the example below, all objects (strings) are stored in local object memory. Used local object memory increases as the remote function g is repeatedly called.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-inspect-local-memory-usage.png
    :align: center

.. code-block:: python

  @ray.remote
  def g():
      return "hello world!"
  
  @ray.remote
  class A(object):
      def f(self):
          object_ids = []
          for idx in range(50):
              ray.show_in_webui("Loop index = {}...".format(idx))
              object_ids.append(g.remote())
              time.sleep(0.5)
  
  a = A.remote()
  _ = a.f.remote()


Inspect Node Memory Usage
--------------------------
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
--------------------------

.. note::

  Currently, profiling button works only when you use passwordless `sudo`. 
  Also, it is still experimental and probably not robust enough. Please report issues if you find any problems.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-profiling-buttons.png
    :align: center

Clicking the profling button on the dashboard launches py-spy that times your python program. The timing information will be visualized as flamegraph in a new browser tab.

Checkout the example Learning to play Pong on ray documentation: `Pong Example <https://ray.readthedocs.io/en/latest/auto_examples/plot_pong_example.html>`_

Click profiling, and click Profiling result when it is ready. Note that there could be multiple threads in the process and some are ray internal threads and the timing information may not be so interesting. Click the left and right arrow on the middle top to see profiling results on different threads.
Now you can intuitively see where could be the computation bottleneck. 

More information on how to interpret the flamegraph is available at https://github.com/jlfwong/speedscope#usage.

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-profiling.png
    :align: center
