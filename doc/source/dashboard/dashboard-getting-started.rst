Getting Started
===================

Run Dashboard
-----------------
Dashboard is configured when a Ray cluster begins. 
If you want to run a dashboard, provide `include_webui=True` when you initialize a Ray cluster. 

.. code-block:: python

  ray.init(include_webui=True)

By default, this argument is set to be `None`. In this case, dashboard is running only when required dependencies are installed in your environment. 
For more details, checkout the link. `ray.init reference <https://ray.readthedocs.io/en/latest/package-ref.html>`_

Once dashboard is running, it starts a lightweight http server inside a head node. Metrics will be accessible through this web server.

Access Dashboard
-----------------
Once you start your cluster with a dashboard, you can access it through its url. By default the url is localhost:8265. 
You can see the url when the ray cluster starts up.

.. code-block:: bash

  INFO resource_spec.py:212 -- Starting Ray with 4.2 GiB memory available for workers and up to 2.12 GiB for objects. You can adjust these settings with ray.init(memory=<bytes>, object_store_memory=<bytes>).
  INFO services.py:1089 -- Dashboard is running in a host localhost:8266
  INFO services.py:1093 -- View the Ray dashboard at localhost:8266

This url could also be found from the return value of `ray.init` 

.. code-block:: python

  addresses = ray.init()
  print("Click here to open the dashboard: http://{}".format(addresses["webui_url"]))

The url is also logged at `/tmp/ray/dashboard_url`. (Read a note below if you cannot find the url from this path).

.. note::
  
  The temp directory that stores this url is configurable if you run 
  .. code-block:: python
  
    `ray.init(temp_dir=[somewhere else])`. 

