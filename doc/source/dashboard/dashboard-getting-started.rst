Getting Started
===================

Run Dashboard
-----------------
When a Ray cluster begins, you can specify whether or not you use dashboard. 
If you want to run a dashboard, provide `include_webui=True` when you initialize a Ray cluster. 

.. code-block:: python

  ray.init(include_webui=True)

By default, this argument is set to be `None`. In this case, dashboard is running only when required dependencies are installed in your environment. 
For more details, checkout the `include_webui` arguments at `ray.init reference <https://ray.readthedocs.io/en/latest/package-ref.html>`_

.. note::

  Please take a look at links below to run dashboard when Ray is built from source.

    - `Install Ray <https://ray.readthedocs.io/en/latest/installation.html#install-ray>`_
    - `Optional Dashboard Support <https://ray.readthedocs.io/en/latest/installation.html#optional-dashboard-support>`_
  
Access Dashboard
-----------------
Once you start your cluster with a dashboard, you can access it through its url. By default the url is `localhost:8265`. Note that the port number can be changed if the default port is not available.
You can see the url when the ray cluster starts up.

.. code-block:: bash

  INFO resource_spec.py:212 -- Starting Ray with 4.2 GiB memory available for workers and up to 2.12 GiB for objects. You can adjust these settings with ray.init(memory=<bytes>, object_store_memory=<bytes>).
  INFO services.py:1089 -- Dashboard is running in a host localhost:8265
  INFO services.py:1093 -- View the Ray dashboard at localhost:8265

This url could also be found from the return value of `ray.init` 

.. code-block:: python

  addresses = ray.init()
  print("Click here to open the dashboard: http://{}".format(addresses["webui_url"]))

The url is also logged at `/tmp/ray/dashboard_url`.

.. note::
  
  The temp directory that stores this url is configurable if you run 

  .. code-block:: python
  
    ray.init(temp_dir='/tmp') 

