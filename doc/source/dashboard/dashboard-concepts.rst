Concepts
==========

Components
-----------

Machine View
~~~~~~~~~~~~
Machine view provides node level information. You can see resource usages of each node and processes residing in the node.  

Logical View (Experimental)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Logical view renders actor information in a tree format. Let's see the code block below. Parent actor creates 2 Child actors and each Child actor creates one GrandChild Actor.
This relationship will be visible at a dashboard logical view page.

.. code-block:: python

  import ray
  ray.init()

  @ray.remote
  class GrandChild:
      def __init__(self):
          pass

  @ray.remote
  class Child:
      def __init__(self):
          grand_child_handle = GrandChild.remote()
  
  @ray.remote
  class Parent:
      def __init__(self):
          children_handles = [Child.remote() for _ in range(2)]

  parent_handle = Parent.remote()

You can see dashboard shows the parent/child relationship as expected. 

.. image:: https://raw.githubusercontent.com/ray-project/images/master/docs/dashboard/dashboard-tree-actors.png
    :align: center

For more information about what logical view provides, checkout `Dashboard Usage <dashboard-usage.html>`_


Ray Config
~~~~~~~~~~~~
Ray's cluster configuration. If you run Ray with cluster configuration, it will be visible at this tab.


Tune (Experimental)
~~~~~~~~~~~~~~~~~~~
When Tune is running, you can find information of each tuning job at this tab. Tune tab is probably not visible if you don't run Tune in the Ray cluster.
.. note::

  This note is written when Ray 0.8.2 was released. 
  
  At this point, Tune dashboard requires users to have $HOME/ray_results directory or an environment variable, TUNE_RESULT_DIR to store Tune results.

Internal
---------
When Ray is initiated, it runs a dashboard process at a head node. It consists of 4 components.

  - **HTTP Server**: It runs at a head node and serve the dashboard frontend.
  - **Get Endpoints**: Several endpoints to serve data that is collected at a dashboard process. E.g. Ray config, node info, and etc.
  - **Action Endpoints**: Endpoints that perform actions upon requests. For example, it can kill actors or start profiling.
  - **Collectors**: Threads that collect metrics from a Ray cluster. Currently, it collects metrics from nodes, raylets, and tune (if running).

All the available metrics are exposed to users through a Web UI frontend. The frontend receives metrics by sending requests every second to the dashboard backend.

Can I access dashboard APIs?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Yes. But we don't recommend you to use these APIs directly without a built-in frontend because it is still subject to change. 

How are metrics collected?
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Currently, collector threads obtain metrics from Ray nodes and Raylets and aggregate them to produce json blobs that represent node and raylet information.

Can I configure metrics update frequency?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
For now, the update frequency is one second, and it is not configurable.
