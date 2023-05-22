.. _observability-configure-manage-dashboard:

Configuring and Managing the Dashboard
======================================

Setting up the dashboard may require some configuration depending on your use model and cluster environment. Integrations with Prometheus and Grafana are optional for extending visualization capabilities.

Port forwarding
---------------

:ref:`The dashboard <observability-getting-started>` provides detailed information about the state of the cluster,
including the running jobs, actors, workers, nodes, etc.
By default, the :ref:`cluster launcher <vm-cluster-quick-start>` and :ref:`KubeRay operator <kuberay-quickstart>` will launch the dashboard, but will
not publicly expose the port.

.. tab-set::

    .. tab-item:: VM

        You can securely port-forward local traffic to the dashboard via the ``ray
        dashboard`` command.

        .. code-block:: shell

            $ ray dashboard [-p <port, 8265 by default>] <cluster config file>

        The dashboard is now be visible at ``http://localhost:8265``.

    .. tab-item:: Kubernetes

        The KubeRay operator makes the dashboard available via a Service targeting
        the Ray head pod, named ``<RayCluster name>-head-svc``. You can access the
        dashboard from within the Kubernetes cluster at ``http://<RayCluster name>-head-svc:8265``.

        You can also view the dashboard from outside the Kubernetes cluster by
        using port-forwarding:

        .. code-block:: shell

            $ kubectl port-forward service/raycluster-autoscaler-head-svc 8265:8265

        For more information about configuring network access to a Ray cluster on
        Kubernetes, see the :ref:`networking notes <kuberay-networking>`.

Changing Dashboard Ports
------------------------

.. tab-set::

    .. tab-item:: Single-node local cluster

      **CLI**

      To customize the port on which the dashboard runs, you can pass
      the ``--dashboard-port`` argument with ``ray start`` in the command line.

      **ray.init**

      If you need to customize the port on which the dashboard runs, you can pass the
      keyword argument ``dashboard_port`` in your call to ``ray.init()``.

    .. tab-item:: VM Cluster Launcher

      To disable the dashboard while using the "VM cluster launcher", include the "ray start --head --include-dashboard=False" argument
      and specify the desired port number in the "head_start_ray_commands" section of the `cluster launcher's YAML file <https://github.com/ray-project/ray/blob/0574620d454952556fa1befc7694353d68c72049/python/ray/autoscaler/aws/example-full.yaml#L172>`_.

    .. tab-item:: Kuberay

      See the `Specifying non-default ports <https://docs.ray.io/en/latest/cluster/kubernetes/user-guides/config.html#specifying-non-default-ports>`_ page.


Running Behind a Reverse Proxy
------------------------------

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

Viewing Built-in Dashboard API Metrics
--------------------------------------

The dashboard is powered by a server that serves both the UI code and the data about the cluster via API endpoints.
There are basic Prometheus metrics that are emitted for each of these API endpoints:

`ray_dashboard_api_requests_count_requests_total`: Collects the total count of requests. This is tagged by endpoint, method, and http_status.

`ray_dashboard_api_requests_duration_seconds_bucket`: Collects the duration of requests. This is tagged by endpoint and method.

For example, you can view the p95 duration of all requests with this query:

.. code-block:: text

  histogram_quantile(0.95, sum(rate(ray_dashboard_api_requests_duration_seconds_bucket[5m])) by (le))

These metrics can be queried via Prometheus or Grafana UI. Instructions on how to set these tools up can be found :ref:`here <observability-visualization-setup>`.

Disabling the Dashboard
-----------------------

Dashboard is included in the `ray[default]` installation by default and automatically started.

To disable the dashboard, use the following arguments `--include-dashboard`.

.. tab-set::

    .. tab-item:: Single-node local cluster

      **CLI**

      .. code-block:: bash

          ray start --include-dashboard=False

      **ray.init**

      .. testcode::
        :hide:

        import ray
        ray.shutdown()

      .. testcode::

        import ray
        ray.init(include_dashboard=False)

    .. tab-item:: VM Cluster Launcher

      To disable the dashboard while using the "VM cluster launcher", include the "ray start --head --include-dashboard=False" argument
      in the "head_start_ray_commands" section of the `cluster launcher's YAML file <https://github.com/ray-project/ray/blob/0574620d454952556fa1befc7694353d68c72049/python/ray/autoscaler/aws/example-full.yaml#L172>`_.

    .. tab-item:: Kuberay

      TODO

.. _observability-visualization-setup:

Integrating with Prometheus and Grafana
---------------------------------------

Setting up Prometheus
~~~~~~~~~~~~~~~~~~~~~

.. tip::

  The below instructions for Prometheus to enable a basic workflow of running and accessing the dashboard on your local machine.
  For more information about how to run Prometheus on a remote cluster, see :ref:`here <multi-node-metrics>`.

Ray exposes its metrics in Prometheus format. This allows us to easily scrape them using Prometheus.

First, `download Prometheus <https://prometheus.io/download/>`_. Make sure to download the correct binary for your operating system. (Ex: darwin for mac osx)

Then, unzip the archive into a local directory using the following command.

.. code-block:: bash

    tar xvfz prometheus-*.tar.gz
    cd prometheus-*

Ray exports metrics only when ``ray[default]`` is installed.

.. code-block:: bash

  pip install "ray[default]"

Ray provides a prometheus config that works out of the box. After running ray, it can be found at `/tmp/ray/session_latest/metrics/prometheus/prometheus.yml`.

.. code-block:: yaml

    global:
      scrape_interval: 15s
      evaluation_interval: 15s

    scrape_configs:
    # Scrape from each ray node as defined in the service_discovery.json provided by ray.
    - job_name: 'ray'
      file_sd_configs:
      - files:
        - '/tmp/ray/prom_metrics_service_discovery.json'


Next, let's start Prometheus.

.. code-block:: shell

    ./prometheus --config.file=/tmp/ray/session_latest/metrics/prometheus/prometheus.yml

.. note::
    If you are using mac, you may receive an error at this point about trying to launch an application where the developer has not been verified. See :ref:`this link <unverified-developer>` to fix the issue.

Now, you can access Ray metrics from the default Prometheus url, `http://localhost:9090`.

See :ref:`here <multi-node-metrics>` for more information on how to set up Prometheus on a Ray Cluster.

.. _grafana:

Setting up Grafana
~~~~~~~~~~~~~~~~~~

.. tip::

  The below instructions for Grafana setup to enable a basic workflow of running and accessing the dashboard on your local machine.
  For more information about how to run Grafana on a remote cluster, see :ref:`here <multi-node-metrics-grafana>`.

Grafana is a tool that supports more advanced visualizations of prometheus metrics and
allows you to create custom dashboards with your favorite metrics. Ray exports some default
configurations which includes a default dashboard showing some of the most valuable metrics
for debugging ray applications.


Deploying Grafana
*****************

First, `download Grafana <https://grafana.com/grafana/download>`_. Follow the instructions on the download page to download the right binary for your operating system.

Then go to to the location of the binary and run grafana using the built in configuration found in `/tmp/ray/session_latest/metrics/grafana` folder.

.. code-block:: shell

    ./bin/grafana-server --config /tmp/ray/session_latest/metrics/grafana/grafana.ini web

Now, you can access grafana using the default grafana url, `http://localhost:3000`.
You can then see the default dashboard by going to dashboards -> manage -> Ray -> Default Dashboard. The same :ref:`metric graphs <system-metrics>` are also accessible via :ref:`Ray Dashboard <observability-getting-started>`.

.. tip::

  If this is your first time using Grafana, you can login with the username: `admin` and password `admin`.

.. image:: images/graphs.png
    :align: center


See :ref:`here <multi-node-metrics-grafana>` for more information on how to set up Grafana on a Ray Cluster.

Customizing the Prometheus export port
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Ray by default provides the service discovery file, but you can directly scrape metrics from prometheus ports.
To do that, you may want to customize the port that metrics gets exposed to a pre-defined port.

.. code-block:: bash

    ray start --head --metrics-export-port=8080 # Assign metrics export port on a head node.

Now, you can scrape Ray's metrics using Prometheus via ``<ip>:8080``.

Alternate Prometheus host location
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can choose to run Prometheus on a non-default port or on a different machine. When doing so, you should
make sure that prometheus can scrape the metrics from your ray nodes following instructions :ref:`here <multi-node-metrics>`.

In addition, both Ray and Grafana needs to know how to access this prometheus instance. This can be configured
by setting the `RAY_PROMETHEUS_HOST` env var when launching ray. The env var takes in the address to access Prometheus. More
info can be found :ref:`here <multi-node-metrics-grafana>`. By default, we assume Prometheus is hosted at `localhost:9090`.

For example, if Prometheus is hosted at port 9000 on a node with ip 55.66.77.88, One should set the value to
`RAY_PROMETHEUS_HOST=http://55.66.77.88:9000`.


Alternate Grafana host location
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
You can choose to run Grafana on a non-default port or on a different machine. If you choose to do this, the
:ref:`Dashboard <observability-getting-started>` needs to be configured with a public address to that service so the web page
can load the graphs. This can be done with the `RAY_GRAFANA_HOST` env var when launching ray. The env var takes
in the address to access Grafana. More info can be found :ref:`here <multi-node-metrics-grafana>`. Instructions
to use an existing Grafana instance can be found :ref:`here <multi-node-metrics-grafana-existing>`.

For the Grafana charts to work on the Ray dashboard, the user of the dashboard's browser must be able to reach
the Grafana service. If this browser cannot reach Grafana the same way the Ray head node can, you can use a separate
env var `RAY_GRAFANA_IFRAME_HOST` to customize the host the browser users to attempt to reach Grafana. If this is not set,
we use the value of `RAY_GRAFANA_HOST` by default.

For example, if Grafana is hosted at is 55.66.77.88 on port 3000. One should set the value
to `RAY_GRAFANA_HOST=http://55.66.77.88:3000`.


Troubleshooting
~~~~~~~~~~~~~~~

Getting Prometheus and Grafana to use the Ray configurations when installed via homebrew on macOS X
***************************************************************************************************

With homebrew, Prometheus and Grafana are installed as services that are automatically launched for you.
Therefore, to configure these services, you cannot simply pass in the config files as command line arguments.

Instead, follow these instructions:
1. Change the --config-file line in `/usr/local/etc/prometheus.args` to read `--config.file /tmp/ray/session_latest/metrics/prometheus/prometheus.yml`.
2. Update `/usr/local/etc/grafana/grafana.ini` file so that it matches the contents of `/tmp/ray/session_latest/metrics/grafana/grafana.ini`.

You can then start or restart the services with `brew services start grafana` and `brew services start prometheus`.

.. _unverified-developer:

MacOS does not trust the developer to install Prometheus or Grafana
*******************************************************************

You may have received an error that looks like this:

.. image:: https://raw.githubusercontent.com/ray-project/Images/master/docs/troubleshooting/prometheus-trusted-developer.png
    :align: center

When downloading binaries from the internet, Mac requires that the binary be signed by a trusted developer ID.
Unfortunately, many developers today are not trusted by Mac and so this requirement must be overridden by the user manaully.

See `these instructions <https://support.apple.com/guide/mac-help/open-a-mac-app-from-an-unidentified-developer-mh40616/mac>`_ on how to override the restriction and install or run the application.

Grafana dashboards are not embedded in the Ray dashboard
********************************************************
If you're getting an error that says `RAY_GRAFANA_HOST` is not setup despite having set it up, check that:
You've included the protocol in the URL (e.g., `http://your-grafana-url.com` instead of `your-grafana-url.com`).
The URL doesn't have a trailing slash (e.g., `http://your-grafana-url.com` instead of `http://your-grafana-url.com/`).

Certificate Authority (CA error)
********************************
You may see a CA error if your Grafana instance is hosted behind HTTPS. Contact the Grafana service owner to properly enable HTTPS traffic.

