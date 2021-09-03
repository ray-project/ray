Release Process
===============

The following documents the Ray release process. Please use the
`Release Checklist <RELEASE_CHECKLIST.md>`_ to keep track of your progress, as it is meant
to be used alongside this process document. Also, please keep the
team up-to-date on any major regressions or changes to the timeline
via emails to the engineering@anyscale.com Google Group.

Before Branch Cut
-----------------
1. **Create a document to track release-blocking commits.** These may be pull
   requests that are not ready at the time of branch cut, or they may be
   fixes for issues that you encounter during release testing later.
   The only PRs that should be considered release-blocking are those which
   fix a MAJOR REGRESSION (P0) or deliver an absolutely critical piece of
   functionality that has been promised for the release (though this should
   be avoided where possible).
   You may make a copy of the following `template <https://docs.google.com/spreadsheets/d/1qeOYErAn3BzGgtEilBePjN6tavdbabCEEqglDsjrq1g/edit#gid=0>`_.

   Make sure to share this document with major contributors who may have release blockers.

2. **Announce the release** over email to the engineering@anyscale.com mailing 
   group. The announcement should
   contain at least the following information: the release version, 
   the date when branch-cut will occur, the date the release is expected
   to go out (generally a week or so after branch cut depending on how
   testing goes), and a link to the document for tracking release blockers.

After Branch Cut
----------------
1. **Create a release branch:** Create the branch from the desired commit on master
   In order to create the branch, locally checkout the commit ID i.e.,
   ``git checkout <hash>``. Then checkout a new branch of the format
   ``releases/<release-version>`` (e.g. ``releases/1.3.1``). Then push that branch to the ray repo:
   ``git push upstream releases/<release-version>``.

2. **Update the release branch version:** Push a commit directly to the
   newly-created release branch that increments the Python package version in
   ``python/ray/__init__.py``, ``build-docker.sh``, ``src/ray/raylet/main.cc``, and any other files that use ``ray::stats::VersionKey``. See this
   `sample commit for bumping the release branch version`_.

3. **Create a document to collect release-notes:** You can clone `this document <https://docs.google.com/document/d/1vzcNHulHCrq1PrXWkGBwwtOK53vY2-Ol8SXbnvKPw1s/edit?usp=sharing>`_.

   You will also need to create a spreadsheet with information about the PRs 
   included in the release to jog people's memories. You can collect this
   information by running
   .. code-block:: bash
     git log --date=local --pretty=format:"%h%x09%an%x09%ad%x09%s" releases/1.0.1..releases/1.1.0 > release-commits.tsv

   Then, upload this tsv file to Google sheets
   and sort by description. 

   Ask team leads to contribute notes for their teams' projects. Include both
   the spreadsheet and document in your message.
   (Some people to message are Richard Liaw, Eric Liang, Edward
   Oakes, Simon Mo, Sven Mika, and Ameer Haj Ali. Please tag these people in the
   document or @mention them in your release announcement.)


Release Testing
---------------
Before each release, we run the following tests to make sure that there are
no major functionality OR performance regressions. You should start running
these tests right after branch cut in order to identify any regressions early.
The `Releaser`_ tool is used to run release tests in the Anyscale product, and
is generally the easiest way to run release tests. 


1. **Microbenchmark** 

   This is a simple test of Ray functionality and performance
   across several dimensions. You can run it locally with the release commit
   installed using the command ``ray microbenchmark`` for a quick sanity check.

   However, for the official results, you will need to run the 
   microbenchmark in the same setting as previous runs--on an `m4.16xl` instance running `Ubuntu 18.04` with `Python 3`
   You can do this using the `Releaser`_ tool mentioned above, or 
   manually by running ``ray up ray/release/microbenchmark/cluster.yaml``
   followed by ``ray exec ray/release/microbenchmark/cluster.yaml 'ray microbenchmark'``

   The results should be checked in under ``release_logs/<version>/microbenchmark.txt``.

   You can also get the performance change rate from the previous version using
   ``util/microbenchmark_analysis.py``.

2. **Long-running tests**

   These tests should run for at least 24 hours without erroring or hanging (ensure that it is printing new iterations and CPU load is
   stable in the AWS console or in the Anyscale Product's Grafana integration).

   .. code-block:: bash

      long_running_tests/README.rst

   Follow the instructions to kick off the tests and check the status of the workloads.

3. **Long-running multi-node tests**

   .. code-block:: bash

      long_running_distributed_tests/README.rst

   Follow the instructions to kick off the tests and check the status of the workloads.
   These suite of tests are similar to the standard long running tests, except these actually run in a multi-node cluster instead of just a simulated one.
   These tests should also run for at least 24 hours without erroring or hanging.

   **IMPORTANT**: check that the test are actually running (printing output regularly) and aren't
   just stuck at an iteration. You must also check that the node CPU usage is stable
   (and not increasing or decreasing over time, which indicates a leak). You can see the head node
   and worker node CPU utilizations in the AWS console.

4. **Multi-node regression tests**

   Follow the same instruction as long running stress tests. The large scale distributed
   regression tests identify potential performance regression in distributed environment.
   The following test should be run, and can be run with the `Releaser`_ tool
   like other tests:

   - ``rllib_tests/regression_tests`` run the compact regression test for rllib.
   - ``rllib_tests/stress_tests`` run multinode 8hr IMPALA trial.
   - ``stress_tests`` contains two tests: ``many_tasks`` and ``dead_actors``.
      Each of the test runs on 105 spot instances.
   - ``stress_tests/workloads/placement_group`` contains a Python script to run tests.
      It currently uses ``cluster_util`` to emulate the cluster testing. It will be converted to 
      real multi-node tests in the future. For now, just make sure the test succeed locally.

   Make sure that these pass. For the RLlib regression tests, there shouldn't be any errors
   and the rewards should be similar to previous releases. For the rest, it will be obvious if
   they passed, as they will output metrics about their execution times and results that can be compared to previous releases. 

   **IMPORTANT**: You must get signoff from the RLlib team for the RLlib test results.

   The summaries printed by each test should be checked in under
   ``release_logs/<version>`` on the **master** branch (make a pull request).

5. **Scalability envelope tests**

   - Run the tests in `benchmarks/` (with `ray submit --start cluster.yaml <test file>`)
   - Record the outputted times.
     - Whether the results are acceptable is a judgement call.

6. **ASAN tests**

   Run the ``ci/asan_tests`` with the commit. This will enable ASAN build and run the whole Python tests to detect memory leaks.

7. **K8s operator tests**

   Refer to ``kubernetes_tests/README.md``. These tests verify basic functionality of the Ray Operator and Helm chart.

8. **Data processing tests**

   .. code-block:: bash

      data_processing_tests/README.rst

   Follow the instructions to kick off the tests and check the status of the workloads.
   Data processing tests make sure all the data processing features are reliable and performant.
   The following tests should be run.

   - ``data_processing_tests/workloads/streaming_shuffle.py`` run the 100GB streaming shuffle in a single node & fake 4 nodes cluster.
   - ``data_processing_tests/workloads/dask_on_ray_large_scale_test.py`` runs the large scale dask on ray test in 250 nodes cluster.

   **IMPORTANT** Check if the workload scripts has terminated. If so, please record the result (both read/write bandwidth and the shuffle result) to the ``release_logs/data_processing_tests/[test_name]``.
   Both shuffling runtime and read/write bandwidth shouldn't be decreasing more than 15% compared to the previous release. For the dask on ray test, just make sure it runs for at least 30 minutes without the driver crash.

9. **Ray Tune release tests**

   General Ray Tune functionality is implicitly tested via RLLib and XGBoost release tests.
   We are in the process of introducing scalability envelopes for Ray Tune.

   Release tests are expected to run through without errors and to pass within a pre-specified time.
   The time is checked in the test function and the output will let you know if a run was fast enough and
   thus passed the test.

10. **XGBoost release tests**

    .. code-block:: bash

       xgboost_tests/README.rst

    Follow the instructions to kick off the tests and check the status of the workloads.
    The XGBoost release tests use assertions or fail with exceptions and thus
    should automatically tell you if they failed or not.
    Only in the case of the fault tolerance tests you might want
    to check the logs. See the readme for more information.


Identify and Resolve Release Blockers
-------------------------------------
If a release blocking issue arises in the course of testing, you should
reach out to the team to which the issue corresponds. They should either
work on a fix immediately or tell you which changes ought to be reverted.

There are two ways the issue can be resolved: 

1. Fix the issue on the master branch and
   cherry-pick the relevant commit (using ``git cherry-pick``) onto the release
   branch (recommended). 
2. Revert the commit that introduced the bug on the
   release branch (using ``git revert``), but not on the master (not recommended).

These changes should then be pushed directly to the release branch.

Once Release Blockers are Resolved
----------------------------------
After all release blockers are resolved and testing complete, you are ready
to proceed with the final stages of the release!

1. **Update the Anyscale product Docker images:** The Anyscale product team
   builds new Docker images using the latest release candidate wheels. This
   image is then made available to Anyscale users in a new deployment.
   This should happen before the release is published on open source,
   as compatibility with Anyscale is a hard requirement. If this step fails
   or is delayed, the rest of the release process is blocked until the
   issues have been resolved.

2. **Create a GitHub release:** Create a `GitHub release`_. This should include
   **release notes**. Copy the style and formatting used by previous releases.
   Create a draft of the release notes containing information about substantial
   changes/updates/bugfixes and their PR numbers. Once you have a draft, send it
   out to other Ray developers (especially those who contributed heavily during
   this release) for feedback. At the end of the release note, you should also
   add a list of contributors. Make sure Ray, Tune, RLLib, Autoscaler are
   capitalized correctly.

   Run ``util/get_contributors.py`` to generate the list of commits corresponding
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

3. **Download all the wheels:** Now the release is ready to begin final
   testing. The wheels are automatically uploaded to S3, even on the release
   branch. To download them, use ``util/download_wheels.sh``:

   .. code-block:: bash

       export RAY_HASH=...  # e.g., 618147f57fb40368448da3b2fb4fd213828fa12b
       export RAY_VERSION=...  # e.g., 0.7.0
       ./bin/download_wheels.sh

   This can be tested if you use the script source ./bin/download_wheels.sh

4. **Upload to PyPI Test:** Upload the wheels to the PyPI test site using
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

     # Need to specify extra URL since some dependencies are not on test.pypi
     pip install --index-url https://test.pypi.org/simple/ --extra-index-url https://pypi.org/simple ray

   Then start Python, make sure you can ``import ray`` and run some simple Ray
   scripts. Make sure that it is finding the version of Ray that you just
   installed by checking ``ray.__version__`` and ``ray.__file__``.

   Do this for MacOS, Linux, and Windows.

   This process is automated. Run ./bin/pip_download_test.sh.
   This will download the ray from the test pypi repository and run the minimum
   sanity check from all the Python version supported. (3.6, 3.7, 3.8)

   The Windows sanity check test is currently not automated. 
   You can start a Windows
   VM in the AWS console running the Deep Learning AMI, then install the correct
   version of Ray using the Anaconda prompt.

5. **Upload to PyPI:** Now that you've tested the wheels on the PyPI test
   repository, they can be uploaded to the main PyPI repository. **Be careful,
   it will not be possible to modify wheels once you upload them**, so any
   mistake will require a new release.

   .. code-block:: bash

     # Will ask for your real PyPI credentials and require that you're a maintainer
     # on real PyPI. If you are not, ask @robertnishihara to add you.
     twine upload --repository-url https://upload.pypi.org/legacy/ *.whl

   Now, try installing from the real PyPI mirror. Verify that the correct version is
   installed and that you can run some simple scripts.

   .. code-block:: bash

     pip install -U ray

6. **Create a point release on readthedocs page:** Go to the `Ray Readthedocs version page`_.
   Scroll to "Activate a version" and mark the *release branch* as "active" and "public". This creates a point release for the documentation.
   Message @richardliaw to add you if you don't have access.

7. **Update 'Default Branch' on the readthedocs page:**
   Go to the `Ray Readthedocs advanced settings page`_.
   In 'Global Settings', set the 'Default Branch' to the *release branch*. This redirects the documentation to the latest pip release.
   Message @richardliaw to add you if you don't have access.

   If, after completing this step, you still do not see the correct version
   of the docs, trigger a new build of the "latest" branch in
   readthedocs to see if that fixes it.

8. **Update ML Docker Image:** Upgrade the ``requirements_ml_docker.txt`` dependencies to use the same Tensorflow and Torch version as
   minimum of ``requirements_tune.txt`` and ``requirements_rllib.txt``. Make any changes to the CUDA
   version so that it is compatible with these Tensorflow (https://www.tensorflow.org/install/source#gpu) or Torch (https://pytorch.org/get-started/locally/, https://pytorch.org/get-started/previous-versions/)
   versions. Ping @ijrsvt or @amogkam for assistance.

9. **Update latest Docker Image:** SET THE VERSION NUMBER IN `docker/fix-docker-latest.sh`, then run the script ot update the "latest" tag
   in Dockerhub for the 
   ``rayproject/ray`` and ``rayproject/ray-ml`` Docker images to point to the Docker images built from the release. (Make sure there is no permission denied error, you will likely have to ask Thomas for permissions).
   
   Check the dockerhub to verify the update worked. https://hub.docker.com/repository/docker/rayproject/ray/tags?page=1&name=latest&ordering=last_updated

10. **Send out an email announcing the release** to the employees@anyscale.com
   Google group, and post a slack message in the Announcements channel of the
   Ray slack (message a team lead if you do not have permissions.)

11. **Improve the release process:** Find some way to improve the release
    process so that whoever manages the release next will have an easier time.
    If you had to make any changes to tests or cluster configurations, make
    sure they are contributed back! If you've noticed anything in the docs that
    was out-of-date, please patch them.

**You're done! Congratulations and good job!**

Resources and Troubleshooting
-----------------------------
**Link to latest wheel:**

Assuming you followed the naming convention and have completed the step of
updating the version on the release branch, you will be able to find wheels
for your release at the following URL (with, e.g. VERSION=1.3.0): ``https://s3-us-west-2.amazonaws.com/ray-wheels/releases/<VERSION>/bfc8d1be43b86a9d3008aa07ca9f36664e02d1ba1/<VERSION>-cp37-cp37m-macosx_10_13_intel.whl``
(Note, the exact URL varies a bit by python version and platform,
this is for OSX on Python 3.7)

**AWS link for all Ray wheels:**

The AWS s3 file hierarchy for Ray wheels can be found `here <https://s3.console.aws.amazon.com/s3/buckets/ray-wheels/?region=us-west-2&tab=objects>`_
in case you're having trouble with the above link.

.. _`sample commit for bumping the release branch version`: https://github.com/ray-project/ray/commit/c589de6bc888eb26c87647f5560d6b0b21fbe537
.. _`GitHub release`: https://github.com/ray-project/ray/releases
.. _`Ray Readthedocs version page`: https://readthedocs.org/projects/ray/versions/
.. _`Ray Readthedocs advanced settings page`: https://readthedocs.org/dashboard/ray/advanced/
.. _`Release Checklist`: https://github.com/ray-project/ray/release/RELEASE_CHECKLIST.md
.. _`Releaser`: https://github.com/ray-project/releaser
