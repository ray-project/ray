Release Process
===============

This document describes the process for creating new releases.

1. **Testing:** Before a release is created, significant testing should be done.
   In particular, our continuous integration currently does not include
   sufficient coverage for testing at scale and for testing fault tolerance, so
   it is important to run tests on clusters of hundreds of machines and to make
   sure that the tests complete when some machines are killed.

2. **Libraries:** Make sure that the libraries (e.g., RLlib, Tune) are in a
   releasable state.

3. **Increment the Python version:** Create a PR that increments the Python
   package version. See `this example`_.

4. **Create a GitHub release:** Create a GitHub release through the `GitHub
   website`_. The release should be created at the commit from the previous
   step. This should include release notes.

5. **Upload to PyPI Test:** Upload the wheels to the PyPI test site using
   ``twine`` (ask Robert to add you as a maintainer to the PyPI project). You'll
   need to run a command like

   .. code-block:: bash

     twine upload --repository-url https://test.pypi.org/legacy/ ray/.whl/*

   assuming that you've built all of the wheels and put them in ``ray/.whl``
   (note that you can also get them from the "ray-wheels" S3 bucket),
   that you've installed ``twine``, and that you've made PyPI accounts.

   Test that you can install the wheels with pip from the PyPI test repository
   with

   .. code-block:: bash

     pip install --index-url https://test.pypi.org/simple/ ray

   Then start Python, make sure you can ``import ray`` and run some simple Ray
   scripts. Make sure that it is finding the version of Ray that you just
   installed by checking ``ray.__version__`` and ``ray.__file__``.

   Do this at least for MacOS and for Linux, as well as for Python 2 and Python
   3.

6. **Upload to PyPI:** Now that you've tested the wheels on the PyPI test
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

.. _`this example`: https://github.com/ray-project/ray/pull/1745
.. _`GitHub website`: https://github.com/ray-project/ray/releases
