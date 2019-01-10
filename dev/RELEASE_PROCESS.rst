Release Process
===============

This document describes the process for creating new releases.

1. First, you should build wheels that you'd like to use for testing. That can
   be done by following the `documentation for building wheels`_.

2. **Testing:** Before a release is created, significant testing should be done.
   Run the script `test/stress_tests/run_stress_tests.sh`_ and make sure it
   passes. *And make sure it is testing the right version of Ray!* This will use
   the autoscaler to start a bunch of machines and run some tests. Any new
   stress tests should be added to this script so that they will be run
   automatically for future release testing.

3. **Libraries:** Make sure that the libraries (e.g., RLlib, Tune, SGD) are in a
   releasable state. TODO(rkn): These libraries should be tested automatically
   by the script above, but they aren't yet.

4. **Increment the Python version:** Create a PR that increments the Python
   package version. See `this example`_.

5. **Create a GitHub release:** Create a GitHub release through the `GitHub
   website`_. The release should be created at the commit from the previous
   step. This should include **release notes**. Copy the style and formatting
   used by previous releases.

6. **Python wheels:** The Python wheels will automatically be built on Travis
   and uploaded to the ``ray-wheels`` S3 bucket. Download these wheels (e.g.,
   using ``wget``) and install them with ``pip`` and run some simple Ray scripts
   to verify that they work.

7. **Upload to PyPI Test:** Upload the wheels to the PyPI test site using
   ``twine`` (ask Robert to add you as a maintainer to the PyPI project). You'll
   need to run a command like

   .. code-block:: bash

     twine upload --repository-url https://test.pypi.org/legacy/ ray/.whl/*

   assuming that you've downloaded the wheels from the ``ray-wheels`` S3 bucket
   and put them in ``ray/.whl``, that you've installed ``twine`` through
   ``pip``, and that you've made PyPI accounts.

   Test that you can install the wheels with pip from the PyPI test repository
   with

   .. code-block:: bash

     pip install --index-url https://test.pypi.org/simple/ ray

   Then start Python, make sure you can ``import ray`` and run some simple Ray
   scripts. Make sure that it is finding the version of Ray that you just
   installed by checking ``ray.__version__`` and ``ray.__file__``.

   Do this at least for MacOS and for Linux, as well as for Python 2 and Python
   3. Also do this for different versions of MacOS.

8. **Upload to PyPI:** Now that you've tested the wheels on the PyPI test
   repository, they can be uploaded to the main PyPI repository. Be careful,
   **it will not be possible to modify wheels once you upload them**, so any
   mistake will require a new release. You can upload the wheels with a command
   like

   .. code-block:: bash

     twine upload --repository-url https://upload.pypi.org/legacy/ ray/.whl/*

   Verify that

   .. code-block:: bash

     pip install -U ray

   finds the correct Ray version, and successfully runs some simple scripts on
   both MacOS and Linux as well as Python 2 and Python 3.

.. _`documentation for building wheels`: https://github.com/ray-project/ray/blob/master/python/README-building-wheels.md
.. _`test/stress_tests/run_stress_tests.sh`: https://github.com/ray-project/ray/blob/master/test/stress_tests/run_stress_tests.sh
.. _`this example`: https://github.com/ray-project/ray/pull/3420
.. _`GitHub website`: https://github.com/ray-project/ray/releases
