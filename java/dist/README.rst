Ray Java Dist
=============
This is a ray java wheel dist which packages ray java jars into a wheel, and provide a ``resource_util.get_ray_jars_dir()`` function to get jars directory in runtime.

Build Wheels
------------
``python setup.py bdist_wheel``
The wheels will be located at ``java/dist/dist``

Jars Directory
--------------
.. code-block:: python

    from ray_java import resource_util
    resource_util.get_ray_jars_dir()
