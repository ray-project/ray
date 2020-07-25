Release Process
===============

This document describes the process for creating new releases.

1. **Create a release branch:** Create the branch from the desired commit on master
   In order to create the branch, locally checkout the commit ID i.e.,
   ``git checkout <hash>``. Then checkout a new branch of the format
   ``releases/<release-version>``. Then push that branch to the ray repo:
   ``git push upstream releases/<release-version>``.

2. **Update the release branch version:** Push a commit directly to the
   newly-created release branch that increments the Python package version in
   python/ray/__init__.py and src/ray/raylet/main.cc. See this
   `sample commit for bumping the release branch version`_.

3. **Update the master branch version:**

   For a new minor release (e.g., 0.7.0): Create a pull request to
   increment the dev version in of the master branch. See this
   `sample PR for bumping a minor release version`_. **NOTE:** Not all of
   the version numbers should be replaced. For example, ``0.7.0`` appears in
   this file but should not be updated.

   For a new micro release (e.g., 0.7.1): No action is required.

4. **Testing:** Before releasing, the following sets of tests should be run.
   The results of each of these tests for previous releases are checked in
   under ``doc/dev/release_logs``, and should be compared against to identify
   any regressions.

   1. Long-running tests

   .. code-block:: bash

       ray/ci/long_running_tests/README.rst

   Follow the instructions to kick off the tests and check the status of the workloads.
   These tests should run for at least 24 hours without erroring or hanging (ensure that it is printing new iterations and CPU load is
   stable in the AWS console).

   2. Long-running multi-node tests

   .. code-block:: bash

      ray/ci/long_running_distributed_tests/README.rst

   Follow the instructions to kick off the tests and check the status of the workloads.
   These suite of tests are similar to the standard long running tests, except these actually run in a multi-node cluster instead of just a simulated one.
   These tests should also run for at least 24 hours without erroring or hanging.

   3. Multi-node regression tests

   Follow the same instruction as long running stress tests. The large scale distributed
   regression tests identify potential performance regression in distributed environment.
   The following test should be ran:

   - ``ci/regression_test/rllib_regression-tests`` run the compact regression test for rllib.
   - ``ci/regression_test/rllib_stress_tests`` run multinode 8hr IMPALA trial.
   - ``ci/regression_test/stress_tests`` contains two tests: ``many_tasks`` and ``dead_actors``.
     Each of the test runs on 105 spot instances.

   Make sure that these pass. For the RLlib regression tests, see the comment on the
   file for the pass criteria. For the rest, it will be obvious if they passed.
   This will use the autoscaler to start a bunch of machines and run some tests.
   **Caution!**: By default, the stress tests will require expensive GPU instances.

   The summaries printed by each test should be checked in under
   ``doc/dev/release_logs/<version>``.

   4. Microbenchmarks

   Run the ``ci/microbenchmark`` with the commit. Under the hood, the session will
   run `ray microbenchmark` on an `m4.16xl` instance running `Ubuntu 18.04` with `Python 3`
   to get the latest microbenchmark numbers.

   The results should be checked in under ``doc/dev/release_logs/<version>``.

   You can also get the performance change rate from the previous version using
   microbenchmark_analysis.py

   5. ASAN tests

   Run the ``ci/asan_tests`` with the commit. This will enable ASAN build and run the
   whole Python tests to detect memory leaks.

5. **Resolve release-blockers:** If a release blocking issue arises, there are
   two ways the issue can be resolved: 1) Fix the issue on the master branch and
   cherry-pick the relevant commit  (using ``git cherry-pick``) onto the release
   branch (recommended). 2) Revert the commit that introduced the bug on the
   release branch (using ``git revert``), but not on the master (not recommended).

   These changes should then be pushed directly to the release branch.

6. **Create a GitHub release:** Create a `GitHub release`_. This should include
   **release notes**. Copy the style and formatting used by previous releases.
   Create a draft of the release notes containing information about substantial
   changes/updates/bugfixes and their PR numbers. Once you have a draft, send it
   out to other Ray developers (especially those who contributed heavily during
   this release) for feedback. At the end of the release note, you should also
   add a list of contributors. Make sure Ray, Tune, RLLib, Autoscaler are
   capitalized correctly.

   Run ``doc/dev/get_contributors.py`` to generate the list of commits corresponding
   to this release and the formatted list of contributors.
   You will need to provide a GitHub personal access token
   (github.com -> settings -> developer settings -> personal access tokens).

    .. code-block:: bash

      # Must be run from inside the Ray repository.
      pip install PyGitHub tqdm
      python get_contributors.py --help
      python get_contributors.py \
        --access-token=xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx \
        --prev-release-commit="<COMMIT_SHA>" \
        --curr-release-commit="<COMMIT_SHA>"

7. **Download all the wheels:** Now the release is ready to begin final
   testing. The wheels are automatically uploaded to S3, even on the release
   branch. To test, ``pip install`` from the following URLs:

   .. code-block:: bash

       export RAY_HASH=...  # e.g., 618147f57fb40368448da3b2fb4fd213828fa12b
       export RAY_VERSION=...  # e.g., 0.7.0

       # Linux Wheels
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-manylinux1_x86_64.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-manylinux1_x86_64.whl

       # Mac Wheels
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp36-cp36m-macosx_10_13_intel.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp37-cp37m-macosx_10_13_intel.whl
       pip install -U https://s3-us-west-2.amazonaws.com/ray-wheels/releases/$RAY_VERSION/$RAY_HASH/ray-$RAY_VERSION-cp38-cp38-macosx_10_13_x86_64.whl

   This can be tested if you use the script source ./bin/download_wheels.sh

8. **Upload to PyPI Test:** Upload the wheels to the PyPI test site using
   ``twine``.

   .. code-block:: bash

     # Downloads all of the wheels to the current directory.
     RAY_VERSION=<version> RAY_HASH=<commit_sha> bash download_wheels.sh

     # Will ask for your PyPI test credentials and require that you're a maintainer
     # on PyPI test. If you are not, ask @robertnishihara to add you.
     pip install twine
     twine upload --repository-url https://test.pypi.org/legacy/ *.whl

   Test that you can install the wheels with pip from the PyPI test repository:

   .. code-block:: bash

     # First install ray normally because installing from test.pypi.org won't
     # be able to install some of the other dependencies.
     pip install ray
     pip uninstall ray

     pip install --index-url https://test.pypi.org/simple/ ray

   Then start Python, make sure you can ``import ray`` and run some simple Ray
   scripts. Make sure that it is finding the version of Ray that you just
   installed by checking ``ray.__version__`` and ``ray.__file__``.

   Do this for MacOS, Linux, and Windows.

   This process is automated. Run ./bin/pip_download_test.sh.
   This will download the ray from the test pypi repository and run the minimum
   sanity check from all the Python version supported. (3.6, 3.7, 3.8)

   Windows sanity check test is currently not automated.

9. **Upload to PyPI:** Now that you've tested the wheels on the PyPI test
   repository, they can be uploaded to the main PyPI repository. Be careful,
   **it will not be possible to modify wheels once you upload them**, so any
   mistake will require a new release.

   .. code-block:: bash

     # Will ask for your real PyPI credentials and require that you're a maintainer
     # on real PyPI. If you are not, ask @robertnishihara to add you.
     twine upload --repository-url https://upload.pypi.org/legacy/ *.whl

   Now, try installing from the real PyPI mirror. Verify that the correct version is
   installed and that you can run some simple scripts.

   .. code-block:: bash

     pip install -U ray

10. **Create a point release on readthedocs page:** Go to the `Ray Readthedocs version page`_.
    Scroll to "Activate a version" and mark the *release branch* as "active" and "public". This creates a point release for the documentation.
    Message @richardliaw to add you if you don't have access.

11. **Update 'Default Branch' on the readthedocs page:** Go to the `Ray Readthedocs advanced settings page`_.
    In 'Global Settings', set the 'Default Branch' to the *release branch*. This redirects the documentation to the latest pip release.
    Message @richardliaw to add you if you don't have access.

12. **Improve the release process:** Find some way to improve the release
    process so that whoever manages the release next will have an easier time.

.. _`sample PR for bumping a minor release version`: https://github.com/ray-project/ray/pull/6303
.. _`sample commit for bumping the release branch version`: https://github.com/ray-project/ray/commit/a39325d818339970e51677708d5596f4b8f790ce
.. _`GitHub release`: https://github.com/ray-project/ray/releases
.. _`Ray Readthedocs version page`: https://readthedocs.org/projects/ray/versions/
.. _`Ray Readthedocs advanced settings page`: https://readthedocs.org/dashboard/ray/advanced/
