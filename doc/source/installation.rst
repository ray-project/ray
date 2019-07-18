Installing Ray
==============

Ray should work with Python 2 and Python 3. We have tested Ray on Ubuntu 14.04, Ubuntu 16.04, Ubuntu 18.04,
MacOS 10.11, 10.12, 10.13, and 10.14.

Latest stable version
---------------------

You can install the latest stable version of Ray as follows.

.. code-block:: bash

  pip install -U ray  # also recommended: ray[debug]

Trying snapshots from master
----------------------------

Here are links to the latest wheels (which are built off of master). To install these wheels, run the following command:

.. code-block:: bash

  pip install -U [link to wheel]


===================  ===================
       Linux                MacOS
===================  ===================
`Linux Python 3.7`_  `MacOS Python 3.7`_
`Linux Python 3.6`_  `MacOS Python 3.6`_
`Linux Python 3.5`_  `MacOS Python 3.5`_
`Linux Python 2.7`_  `MacOS Python 2.7`_
===================  ===================


.. _`Linux Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp37-cp37m-manylinux1_x86_64.whl
.. _`Linux Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp36-cp36m-manylinux1_x86_64.whl
.. _`Linux Python 3.5`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp35-cp35m-manylinux1_x86_64.whl
.. _`Linux Python 2.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp27-cp27mu-manylinux1_x86_64.whl
.. _`MacOS Python 3.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp37-cp37m-macosx_10_6_intel.whl
.. _`MacOS Python 3.6`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp36-cp36m-macosx_10_6_intel.whl
.. _`MacOS Python 3.5`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp35-cp35m-macosx_10_6_intel.whl
.. _`MacOS Python 2.7`: https://s3-us-west-2.amazonaws.com/ray-wheels/latest/ray-0.8.0.dev2-cp27-cp27m-macosx_10_6_intel.whl

Trouble installing or running Ray
---------------------------------

One of the Ray libraries is compiled against the wrong version of Python
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

If there is a segfault or a sigabort immediately upon importing Ray, one of the
components may have been compiled against the wrong Python libraries. Bazel
should normally find the right version of Python, but this process is not
completely reliable. In this case, check the Bazel output from installation and
make sure that the version of the Python libraries that were found match the
version of Python that you're using.

Note that it's common to have multiple versions of Python on your machine (for
example both Python 2 and Python 3). Ray will be compiled against whichever
version of Python is found when you run the ``python`` command from the
command line, so make sure this is the version you wish to use.
