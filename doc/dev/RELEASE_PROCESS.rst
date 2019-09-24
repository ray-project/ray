Release Process
===============

This document describes the process for creating new releases.

1. **Create a release branch:** Create the branch from the desired commit on master
   In order to create the branch, locally checkout the commit ID i.e.,
   ``git checkout <hash>``. Then checkout a new branch of the format
   ``releases/<release-version>``. Then push that branch to the ray repo:
   ``git push upstream releases/<release-version>``.

2. **Update the release branch version:** Push a commit that increments the Python
   package version in python/ray/__init__.py. You can push this directly to the
   release branch.

3. **Update the master branch version:** Create a pull request to
   increment the version of the master branch, see `this PR`_.
   The format of the new version is as follows:

   New minor release (e.g., 0.7.0): Increment the minor version and append
   ``.dev0`` to the version. For example, if the version of the new release is
   0.7.0, the master branch needs to be updated to 0.8.0.dev0.

   New micro release (e.g., 0.7.1): Increment the ``dev`` number, such that the
   number after ``dev`` equals the micro version. For example, if the version
   of the new release is 0.7.1, the master branch needs to be updated to
   0.8.0.dev1.

   After the wheels for the new version are built, create and merge a
   `PR like this`_.

   These should be merged as soon as step 1 is complete to make sure the links
   in the documentation keep working and the master stays on the development
   version.

4. **Testing:** Before a release is created, significant testing should be done.
   Run the following scripts

   .. code-block:: bash

       ray/ci/stress_tests/run_stress_tests.sh <release-version> <release-commit>
       ray/ci/stress_tests/run_application_stress_tests.sh <release-version> <release-commit>

   and make sure they pass. If they pass, it will be obvious that they passed.
   This will use the autoscaler to start a bunch of machines and run some tests.
   **Caution!**: By default, the stress tests will require expensive GPU instances.

   You'll also want to kick off the long-running tests:

   .. code-block:: bash

       ray/ci/long_running_tests/start_workloads.sh

   You can use the `check_workloads.sh` script to verify the workloads are running.
   Let them run for at least 24 hours, and check them again. They should all still
   be running (printing new iterations), and their CPU load should be stable when
   you view them in the AWS monitoring console (not increasing over time).

5. **Resolve release-blockers:** If a release blocking issue arises, there are
   two ways the issue can be resolved: 1) Fix the issue on the master branch and
   cherry-pick the relevant commit  (using ``git cherry-pick``) onto the release
   branch (recommended). 2) Revert the commit that introduced the bug on the
   release branch (using ``git revert``), but not on the master (not recommended).

   These changes should then be pushed directly to the release branch.

6. **Download all the wheels:** Now the release is ready to begin final
   testing. The wheels are automatically uploaded to S3, even on the release
   branch. To test, ``pip install`` from the following URLs:

   .. code-block:: bash

       export RAY_HASH=...  # e.g., 618147f57fb40368448da3b2fb4fd213828fa12b
       export RAY_VERSION=...  # e.g., 0.7.0
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp27-cp27mu-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp35-cp35m-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp27-cp27m-macosx_10_6_intel.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp35-cp35m-macosx_10_6_intel.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-macosx_10_6_intel.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-macosx_10_6_intel.whl

7. **Upload to PyPI Test:** Upload the wheels to the PyPI test site using
   ``twine`` (ask Robert to add you as a maintainer to the PyPI project). You'll
   need to run a command like

   .. code-block:: bash

     twine upload --repository-url https://test.pypi.org/legacy/ ray/.whl/*

   assuming that you've downloaded the wheels from the ``ray-wheels`` S3 bucket
   and put them in ``ray/.whl``, that you've installed ``twine`` through
   ``pip``, and that you've created both PyPI accounts.

   Test that you can install the wheels with pip from the PyPI test repository
   with

   .. code-block:: bash

     pip install --index-url https://test.pypi.org/simple/ ray

   Then start Python, make sure you can ``import ray`` and run some simple Ray
   scripts. Make sure that it is finding the version of Ray that you just
   installed by checking ``ray.__version__`` and ``ray.__file__``.

   Do this at least for MacOS and for Linux, as well as for Python 2 and Python
   3.

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

9. **Create a GitHub release:** Create a GitHub release through the
    `GitHub website`_. The release should be created at the commit from the
    previous step. This should include **release notes**. Copy the style and
    formatting used by previous releases. Create a draft of the release notes
    containing information about substantial changes/updates/bugfixes and their
    PR numbers. Once you have a draft, make sure you solicit feedback from other
    Ray developers before publishing. Use the following to get started:

    .. code-block:: bash

      git pull origin master --tags
      git log $(git describe --tags --abbrev=0)..HEAD --pretty=format:"%s" | sort


    At the end of the release note, you can add a list of contributors that help
    creating this release. Use the ``doc/dev/get_contributors.py`` to generate this
    list. You will need to create a GitHub token for this task. Example usage:

    .. code-block:: bash

      python get_contributors.py --help
      python get_contributors.py \
        --access-token=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx \
        --ray-path=$HOME/ray/ray \
        --prev-branch="ray-0.7.1" \
        --curr-branch="ray-0.7.2"

    Run `ray microbenchmark` to get the latest microbenchmark numbers, and
    update their numbers in `profiling.rst`.

    .. code-block:: bash

      ray microbenchmark

10. **Update version numbers throughout codebase:** Suppose we just released
    0.7.1. The previous release version number (in this case 0.7.0) and the
    previous dev version number (in this case 0.8.0.dev0) appear in many places
    throughout the code base including the installation documentation, the
    example autoscaler config files, and the testing scripts. Search for all of
    the occurrences of these version numbers and update them to use the new
    release and dev version numbers. **NOTE:** Not all of the version numbers
    should be replaced. For example, ``0.7.0`` appears in this file but should
    not be updated.

11. **Improve the release process:** Find some way to improve the release
    process so that whoever manages the release next will have an easier time.

.. _`this example`: https://github.com/ray-project/ray/pull/4226
.. _`this PR`: https://github.com/ray-project/ray/pull/5523
.. _`PR like this`: https://github.com/ray-project/ray/pull/5585
.. _`GitHub website`: https://github.com/ray-project/ray/releases
