Release Process
===============

This document describes the process for creating new releases.

1. **Increment the Python version:** Create a PR that increments the Python
   package version. See `this example`_.

2. **Download the Travis-built wheels:** Once Travis has completed the tests,
   the wheels from this commit can be downloaded from S3 to do testing, etc.
   The URL is structured like this:
   ``https://s3-us-west-2.amazonaws.com/ray-wheels/<hash>/<wheel-name>``
   where ``<hash>`` is replaced by the ID of the commit and the ``<version>``
   is the incremented version from the previous step. The ``<wheel-name>`` can
   be determined by looking at the OS/Version matrix in the documentation_.

3. **Create a release branch:** This branch should also have the same commit ID as the
   previous two steps. In order to create the branch, locally checkout the commit ID
   i.e. ``git checkout <hash>``. Then checkout a new branch of the format
   ``releases/<release_number>``. The release number must match the increment in
   the first step. Then push that branch to the ray repo:
   ``git push upstream releases/<release_number>``.

4. **Testing:** Before a release is created, significant testing should be done.
   Run the scripts `ci/stress_tests/run_stress_tests.sh`_ and
   `ci/stress_tests/run_application_stress_tests.sh`_ and make sure they
   pass. You **MUST** modify the autoscaler config file and replace
   ``<<RAY_RELEASE_HASH>>`` and ``<<RAY_RELEASE_VERSION>>`` with the appropriate
   values to test the correct wheels. This will use the autoscaler to start a bunch of
   machines and run some tests. Any new stress tests should be added to this
   script so that they will be run automatically for future release testing.

5. **Resolve release-blockers:** Should any release blocking issues arise,
   there are two ways these issues are resolved: A PR to patch the issue or a
   revert commit that removes the breaking change from the release. In the case
   of a PR, that PR should be created against master. Once it is merged, the
   release master should ``git cherry-pick`` the commit to the release branch.
   If the decision is to revert a commit that caused the release blocker, the
   release master should ``git revert`` the commit to be reverted on the
   release branch. Push these changes directly to the release branch.

6. **Download all the wheels:** Now the release is ready to begin final
   testing. The wheels are automatically uploaded to S3, even on the release
   branch. The wheels can ``pip install``ed from the following URLs:

   .. code-block:: bash

       export RAY_HASH=...  # e.g., 618147f57fb40368448da3b2fb4fd213828fa12b
       export RAY_VERSION=...  # e.g., 0.6.6
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/$RAY_HASH/ray-$RAY_VERSION-cp27-cp27mu-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/$RAY_HASH/ray-$RAY_VERSION-cp35-cp35m-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/$RAY_HASH/ray-$RAY_VERSION-cp27-cp27m-macosx_10_6_intel.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/$RAY_HASH/ray-$RAY_VERSION-cp35-cp35m-macosx_10_6_intel.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-macosx_10_6_intel.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-macosx_10_6_intel.whl

7. **Final Testing:** Send a link to the wheels to the other contributors and
   core members of the Ray project. Make sure the wheels are tested on Ubuntu,
   Mac OSX 10.12, and Mac OSX 10.13+. This testing should verify that the
   wheels are correct and that all release blockers have been resolved. Should
   a new release blocker be found, repeat steps 5-7.

8. **Upload to PyPI Test:** Upload the wheels to the PyPI test site using
   ``twine`` (ask Robert to add you as a maintainer to the PyPI project). You'll
   need to run a command like

   .. code-block:: bash

     twine upload --repository-url https://test.pypi.org/legacy/ray/.whl/*

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

9. **Upload to PyPI:** Now that you've tested the wheels on the PyPI test
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

10. **Create a GitHub release:** Create a GitHub release through the `GitHub website`_.
    The release should be created at the commit from the previous
    step. This should include **release notes**. Copy the style and formatting
    used by previous releases. Create a draft of the release notes containing
    information about substantial changes/updates/bugfixes and their PR number.
    Once you have a draft, make sure you solicit feedback from other Ray
    developers before publishing. Use the following to get started:

    .. code-block:: bash

      git pull origin master --tags
      git log $(git describe --tags --abbrev=0)..HEAD --pretty=format:"%s" | sort

.. _documentation: https://ray.readthedocs.io/en/latest/installation.html#trying-snapshots-from-master
.. _`documentation for building wheels`: https://github.com/ray-project/ray/blob/master/python/README-building-wheels.md
.. _`ci/stress_tests/run_stress_tests.sh`: https://github.com/ray-project/ray/blob/master/ci/stress_tests/run_stress_tests.sh
.. _`ci/stress_tests/run_application_stress_tests.sh`: https://github.com/ray-project/ray/blob/master/ci/stress_tests/run_application_stress_tests.sh
.. _`this example`: https://github.com/ray-project/ray/pull/4226
.. _`these wheels here`: https://ray.readthedocs.io/en/latest/installation.html
.. _`GitHub website`: https://github.com/ray-project/ray/releases
