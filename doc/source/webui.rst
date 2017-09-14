Web UI
======

Dependencies
------------

You need jupyter, ipywidgets, and bokeh for the web UI to run. If you are using Anaconda,
these should already be installed.

Note: If you are building from source, you will also require python2 to build the wheel.

Running the Web UI
------------------

Currently, the web UI is launched automatically when `ray.init` is called. `ray.init` 
should print a URL of the form:

  ===========================================================================================================================
  View the web UI at http://localhost:8889/notebooks/ray_ui92131.ipynb?token=89354a314e5a81bf56e023ad18bda3a3d272ee216f342938
  ===========================================================================================================================

If you are running Ray on your local machine, then you can head directly to that URL 
with your browser to see the jupyter notebook. Otherwise, if you are using Ray remotely
such as on EC2, you will need to ensure that port is open on that machine.
Typically, when you ssh into the machine, you can also port forward with the -L option as such:

.. code-block:: bash

  ssh -L <port>:localhost:<port> <user>@<ip-address>

So for the above URL, you would use the port as 8889. The jupyter notebook attempts to run on port 8888, but if that fails
tries successive ports until it finds an open port. 

You can also open the port on the machine as well, which is not recommended for security as anybody could port scan and
access the machine as well.

Once you have navigated to the URL, you simply need to run Kernel -> Restart and Run all to launch the widgets.

Features
--------

The UI supports a search for additional details on Task IDs and Object IDs, a task timeline, 
a distribution of task completion times, and time series for CPU utilization and cluster usage.

Task and Object IDs
~~~~~~~~~~~~~~~~~~~

These widgets show additional details about an object or task given the ID. If you have the object in Python, the ID can be found by simply doing

.. code-block:: python

   e.hex()

and pasting in the printed string. Otherwise, they can be found in the task timeline in the output area below the timeline when you select a task.

For Task IDs, they can be found by searching for an object ID the task created, or via the task timeline in the output area.

The additional details here can also be found in the task timeline; the search just provides an easier method to find a specific object/task when you have millions of tasks.

Task Timeline
~~~~~~~~~~~~~

There are three components to this widget; the controls for the widget, the timeline itself, and the details area. In the controls,
you first select whether you want to select a subset of tasks via the time they were completed or by the number of tasks. You can control the percentages either via a
double sided slider, or by setting specific values in the text boxes. If you choose to select by the number of tasks, then entering a negative number I in the text field denotes the last I tasks.
If there are ten tasks and I enter -1 into the field, then the slider will show 90% to 100%, where 1 would show 0% to 10%.
Finally, you can choose if you want edges for task submission or object dependencies are to be added, and if you want the different phases of a task broken up into separate tasks in the timeline.

For the timeline, each node has its own dropdown with tasks, and each row in the dropdown is a worker. Moving and zooming are handled by selecting the appropiate icons on the floating taskbar.
The first is selection, the second panning, the third zooming, and the fourth timing. To shown edges, you can enable Flow Events in View Options. 

If you have selection enabled in the floating taskbar and tap on a task, then the details area will fill up with information such as task ID, function ID,
and the time the task took as well as how long each phase of the task took in seconds.

Time Distributions and Time Series
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The completion time distribution, CPU utilization, and cluster usage all have the same controls as the task timeline, though without the additional options solely for the timeline.

The task completion time distribution tracks the histogram of completion tasks for all tasks selected.

CPU utilization gives you a count of how many CPU cores are being used at a given time. As typically each core has a worker assigned to it, this is equivalent to utilization of the workers running in Ray.

Cluster Usage gives you a heat-map with time on the x-axis, node IP addresses on the y-axis, and coloring based on how many tasks were running on that node at that given time.
