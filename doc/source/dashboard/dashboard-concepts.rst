Concepts
========

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

For more information about what logical view provides, checkout `Dashboard Usage <dashboard-usage.html>`_


Ray Config
~~~~~~~~~~~~
If you start your Ray cluster using the autoscaler, the cluster configuration will be displayed in this tab.


Tune (Experimental)
~~~~~~~~~~~~~~~~~~~
When Tune is running, you can find information for each Tune trial in this tab.

