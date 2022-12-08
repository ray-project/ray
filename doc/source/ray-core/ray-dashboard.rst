.. _ray-dashboard:

Ray Dashboard
=============
Ray's built-in dashboard provides metrics, charts, and other features that help
Ray users to understand Ray clusters and libraries.

The dashboard lets you:

- View cluster metrics including time-series visualizations.
- See errors and exceptions at a glance.
- View logs across many machines.
- See all your ray jobs and the logs for those jobs.
- See your ray actors and their logs

Getting Started
---------------

To use the dashboard, first install Ray with the proper dependencies:

.. code-block:: bash

  pip install -U "ray[default]"

You can access the dashboard through a URL printed when Ray is initialized (the default URL is **http://localhost:8265**).

If you prefer to explicitly set the port on which the dashboard will run, you can pass
the ``--dashboard-port`` argument with ``ray start`` in the command line, or you can pass the
keyword argument ``dashboard_port`` in your call to ``ray.init()``.

.. code-block:: text

  INFO worker.py:1487 -- Connected to Ray cluster. View the dashboard at 127.0.0.1:8265.

The dashboard is also available :ref:`when using the cluster launcher <monitor-cluster>`.

Views
-----

There are 4 different views that the Dashboard provides: :ref:`Node view <dash-node-view>`,
:ref:`Jobs view <dash-jobs-view>`, :ref:`Actors view <dash-actors-view>`, and :ref:`Logs view <dash-logs-view>`.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/overview.png
    :align: center
    :width: 20%

.. _dash-node-view:

Node View
~~~~~~~~~

The Node view lets you see resource utilization information on a per-node and per-worker basis.
This also shows the assignment of GPU resources to specific actors or tasks.

In addition, the machine view lets you see **logs** for a node or a worker.

Finally, you can see the task that each worker is currently performing.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/nodes.png
   :align: center


.. _dash-jobs-view:

Jobs View
~~~~~~~~~

The Jobs view lets you monitor the different jobs that ran on your Ray cluster.
A job is a ray workload that was initiated by an entry point.
Typically, jobs are initiated via directly calling `ray.init` or a Ray library from a python script or by using the :ref:`job submission api <jobs-overview>`.


.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/jobs.png
    :align: center


.. _dash-actors-view:

Actors View
~~~~~~~~~~~

The Actors view lets you see information about the actors that have existed on the ray cluster.

You can view the logs for an actor and you can see which job created the actor.
The information of up to 1000 dead actors will be stored.
This value can be overridden by using the `RAY_DASHBOARD_MAX_ACTORS_TO_CACHE` environment variable
when starting Ray.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/actors.png
    :align: center

.. _dash-logs-view:

Logs view
~~~~~~~~~~
The logs view lets you view all the ray logs that are in your cluster. It is organized by node and log file name. Many log links in the other pages will link to this view and filter the list so the relevant logs appear.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/logs.png
    :align: center

The log viewer provides various search functionality to help find the log messages you are looking for.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/logs-content.png
    :align: center

Metrics View
~~~~~~~~~~~~~
The metrics view lets you view visualizations of the time series metrics emitted by Ray. It requires that prometheus and grafana is running for your cluster.
Instructions about that can be found :ref:`here <ray-metrics>`.

You can select the time range of the metrics in the top right corner. The graphs refresh automatically every 15 seconds.

There is also a convenient button to open the grafana UI from the dashboard. The Grafana UI provides additional customizability of the charts.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/metrics.png
    :align: center

Advanced Usage
--------------

Viewing built-in dashboard API metrics
--------------------------------------
The dashboard is powered by a server that serves both the UI code and the data about the cluster via API endpoints.
There are basic prometheus metrics that are emitted for each of these API endpoints:

`ray_dashboard_api_requests_count_requests_total`: Collects the total count of requests. This is tagged by endpoint, method, and http_status.

`ray_dashboard_api_requests_duration_seconds_bucket`: Collects the duration of requests. This is tagged by endpoint and method.

For example, you can view the p95 duration of all requests with this query:

.. code-block:: text

  histogram_quantile(0.95, sum(rate(ray_dashboard_api_requests_duration_seconds_bucket[5m])) by (le))

These metrics can be queried via prometheus or grafana UI. Instructions on how to set these tools up can be found :ref:`here <ray-metrics>`.

Debugging ObjectStoreFullError and Memory Leaks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can view information for object store usage in the Nodes view.
Use it to debug memory leaks, especially `ObjectStoreFullError`.

One common cause of these memory errors is that there are objects which never go out of scope.
In order to find these, you can use the :ref:`ray memory command <debug-with-ray-memory>`.

For details about the information contained in the table, please see the :ref:`ray memory command <debug-with-ray-memory>` documentation.


Running Behind a Reverse Proxy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The dashboard should work out-of-the-box when accessed via a reverse proxy. API requests don't need to be proxied individually.

Always access the dashboard with a trailing ``/`` at the end of the URL.
For example, if your proxy is set up to handle requests to ``/ray/dashboard``, view the dashboard at ``www.my-website.com/ray/dashboard/``.

The dashboard now sends HTTP requests with relative URL paths. Browsers will handle these requests as expected when the ``window.location.href`` ends in a trailing ``/``.

This is a peculiarity of how many browsers handle requests with relative URLs, despite what `MDN <https://developer.mozilla.org/en-US/docs/Learn/Common_questions/What_is_a_URL#examples_of_relative_urls>`_
defines as the expected behavior.

Make your dashboard visible without a trailing ``/`` by including a rule in your reverse proxy that
redirects the user's browser to ``/``, i.e. ``/ray/dashboard`` --> ``/ray/dashboard/``.

Below is an example with a `traefik <https://doc.traefik.io/traefik/getting-started/quick-start/>`_ TOML file that accomplishes this:

.. code-block:: yaml

  [http]
    [http.routers]
      [http.routers.to-dashboard]
        rule = "PathPrefix(`/ray/dashboard`)"
        middlewares = ["test-redirectregex", "strip"]
        service = "dashboard"
    [http.middlewares]
      [http.middlewares.test-redirectregex.redirectRegex]
        regex = "^(.*)/ray/dashboard$"
        replacement = "${1}/ray/dashboard/"
      [http.middlewares.strip.stripPrefix]
        prefixes = ["/ray/dashboard"]
    [http.services]
      [http.services.dashboard.loadBalancer]
        [[http.services.dashboard.loadBalancer.servers]]
          url = "http://localhost:8265"

References
----------

Node View
~~~~~~~~~

**Node/Worker Hierarchy**: The dashboard visualizes hierarchical relationship of
machines (nodes) and workers (processes). Each host consists of many workers, and
you can see them by clicking the + button. The first node is always expanded by default.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/nodes-view-expand.png
    :align: center

You can hide it again by clicking the - button.

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/new-dashboard/node-detail.png
    :align: center

You can also click the node id to go into a node detail page where you can see more information.


.. list-table:: Node View Reference
  :widths: 25 75
  :header-rows: 1

  * - Term
    - Description
  * - **State**
    - Whether the node or worker is alive or dead.
  * - **ID**
    - The ID of the node or the workerId for the worker.
  * - **Host / Cmd line**
    - If it is a node, it shows host information. If it is a worker, it shows the name of the task that is being run.
  * - **IP / PID**
    - If it is a node, it shows the IP address of the node. If it's a worker, it shows the PID of the worker process.
  * - **CPU Usage**
    - CPU usage of each node and worker.
  * - **Memory**
    - RAM usage of each node and worker.
  * - **GPU**
    - GPU usage of the node.
  * - **GRAM**
    - GPU memory usage of the node.
  * - **Object Store Memory**
    - Amount of memory used by the object store for this node.
  * - **Disk**
    - Disk usage of the node.
  * - **Sent**
    - Network bytes sent for each node and worker.
  * - **Received**
    - Network bytes received for each node and worker.
  * - **Log**
    - Logs messages at each node and worker. You can see log files relevant to a node or worker by clicking this link.
  * - **Stack Trace**
    - Get the Python stack trace for the specified worker. Refer to :ref:`dashboard-profiling` for more information.
  * - **Flame Graph**
    - Get a CPU flame graph for the specified worker. Refer to :ref:`dashboard-profiling` for more information.


Jobs View
~~~~~~~~~

.. list-table:: Jobs View Reference
  :widths: 25 75
  :header-rows: 1

  * - Term
    - Description
  * - **Job ID**
    - The ID of the job. This is the primary id that associates tasks and actors to this job.
  * - **Submission ID**
    - An alternate ID that can be provided by a user or generated for all ray job submissions.
      It's useful if you would like to associate your job with an ID that is provided by some external system.
  * - **Status**
    - Describes the state of a job. One of:
        * PENDING: The job has not started yet, likely waiting for the runtime_env to be set up.
        * RUNNING: The job is currently running.
        * STOPPED: The job was intentionally stopped by the user.
        * SUCCEEDED: The job finished successfully.
        * FAILED: The job failed.
  * - **Logs**
    - A link to the logs for this job.
  * - **StartTime**
    - The time the job was started.
  * - **EndTime**
    - The time the job finished.
  * - **DriverPid**
    - The PID for the driver process that is started the job.

Actors
~~~~~~

.. list-table:: Actor View Reference
  :widths: 25 75
  :header-rows: 1

  * - Term
    - Description
  * - **Actor ID**
    - The ID of the actor.
  * - **Restart Times**
    - Number of times this actor has been restarted.
  * - **Name**
    - The name of an actor. This can be user defined.
  * - **Class**
    - The class of the actor.
  * - **Function**
    - The current function the actor is running.
  * - **Job ID**
    - The job in which this actor was created.
  * - **Pid**
    - ID of the worker process on which the actor is running.
  * - **IP**
    - Node IP Address where the actor is located.
  * - **Port**
    - The Port for the actor.
  * - **State**
    - Either one of "ALIVE" or "DEAD".
  * - **Log**
    - A link to the logs that are relevant to this actor.
  * - **Stack Trace**
    - Get the Python stack trace for the specified actor. Refer to :ref:`dashboard-profiling` for more information.
  * - **Flame Graph**
    - Get a CPU flame graph for the specified actor. Refer to :ref:`dashboard-profiling` for more information.

Logs
~~~~
The top level page for the logs view shows the list of nodes in the cluster. After clicking into a node, you can now see a list of all log files.

Details of the different log files can be found here: :ref:`ray-logging`.
