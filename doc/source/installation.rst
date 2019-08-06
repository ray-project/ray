Installing Ray
==============

Ray supports Python 2 and Python 3 as well as MacOS and Linux. Windows support
is planned for the future.

Latest stable version
---------------------

You can install the latest stable version of Ray as follows.

.. code-block:: bash

  pip install -U ray  # also recommended: ray[debug]

Trying snapshots from master
----------------------------

Here are links to the latest wheels (which are built for each commit on the
master branch). To install these wheels, run the following command:

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
