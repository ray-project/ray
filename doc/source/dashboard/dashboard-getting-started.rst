Getting Started
===================
  
Access Dashboard
-----------------
If you start Ray on your laptop, dashboard is running by default, and you can access it through its URL. 
By default the URL is `localhost:8265`. Note that the port will be changed if the default port is not available.

The URL is printed when Ray starts up.
.. code-block:: text

  INFO resource_spec.py:212 -- Starting Ray with 4.2 GiB memory available for workers and up to 2.12 GiB for objects. You can adjust these settings with ray.init(memory=<bytes>, object_store_memory=<bytes>).
  INFO services.py:1093 -- View the Ray dashboard at localhost:8265
