
.. image:: https://github.com/ray-project/ray/raw/master/doc/source/images/ray_header_logo.png

**Ray provides a simple, universal API for building distributed applications.**

Ray accomplishes this mission by:

1. Providing simple primitives for building and running distributed applications.
2. Enabling end users to parallelize single machine code, with little to zero code changes.
3. Including a large ecosystem of applications, libraries, and tools on top of the core Ray to enable complex applications.

**Ray Core** provides the simple primitives for application building.

On top of **Ray Core** are several libraries for solving problems in machine learning:

- :doc:`../tune/index`
- :ref:`rllib-index`
- :ref:`sgd-index`
- :ref:`rayserve`

Ray also has a number of other community contributed libraries:

- :doc:`../dask-on-ray`
- `Mars on Ray <https://github.com/mars-project/mars/pull/1508>`__
- :doc:`../pandas_on_ray`
- :doc:`../joblib`
- :doc:`../multiprocessing`
