Release Process
===============

The following documents the Ray release process. Please use the
`Release Checklist <RELEASE_CHECKLIST.md>`_ to keep track of your progress, as it is meant
to be used alongside this process document. Also, please keep the
team up-to-date on any major regressions or changes to the timeline
via emails to the engineering@anyscale.com Google Group.

Release tests are run automatically and periodically
(weekly, nightly, and 4x daily) for the latest master branch.
A branch cut should only happen when all tests are passing.

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

3. **Considers asking for an internal point of contact for Ant contributions**.
   Ant financial are external contributors to the Ray project. If you're not
   familiar with their contributions to Ray core, consider asking someone from
   the core team to be responsible for communication and deciding which PRs
   and features can be release blocking.

Branch Cut
----------
1. **Observe the status of the periodic release tests** (weekly, nightly, and 4x daily).
   These are tracked on `buildkite <https://buildkite.com/ray-project/periodic-ci>`__.
   The branch cut should only occur when all of these are passing.
   Since the weekly tests are run on Sunday, you should usually cut the branch
   on the following Monday, so the chance that new regressions are introduced
   in new commits is low. Alternatively you can kick off the weekly tests
   on master manually during the week and cut on the next day.

2. **Ping test owners for failing tests**. If some of the tests are not passing,
   this should be fixed on master as soon as possible. Ping the respective
   test owners/teams, or ask them if a failing test is acceptable for the release.

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

   Please also update the Java version strings. These are usually found in
   the ``pom.xml`` or ``pom_template.xml`` files. You can search for ``2.0.0-SNAPSHOT``
   to find code occurrences.
   See this `commit for updating the Java version`_.

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

Release tests are run on the Anyscale product using our automatic release
test tool on `Buildkite <https://buildkite.com/ray-project/periodic-ci>`__.

Release tests are added and maintained by the respective teams.

1. **Kick off the tests on the release branch**. Even if all tests passed
   on master, we still want to make sure they also pass on the release branch.

   a. Navigate to the `buildkite periodic CI pipeline <https://buildkite.com/ray-project/periodic-ci>`__
      and click on "New build". Type in a message which should usually include
      the release version (e.g. "Release 1.X.0: Weekly tests"). The rest of the
      fields can stay as is (``Commit = HEAD`` and ``Branch = master``).

   b. Wait a couple of seconds (usually less than two minutes) until Buildkite
      asks you for input ("Input required: Specify tests to run")

   c. Click the button and enter the required information:

      - Specify the **branch** (second field): ``releases/1.x.0``
      - Specify the **version** (third field): ``1.x.0``
      - Select one of the release test suites (core-nightly, nightly, or weekly)

   d. Hit on "Continue". The tests will now be run.

   e. Repeat this process for the other two test suites. You should have kicked
      off three builds for the release branch (core-nightly, nightly, weekly).

2. **Track the progress**. You can keep a look at the tests to track their status.
   If a test fails, take a look at the output. Sometimes failures can be due
   to the product or AWS instance availabilities. If a failure occurs in the test,
   check if the same test failed on master and ping the test owner/team.
   Please note that some of the ``weekly`` tests run for 24 hours.

3. **Collect release test logs**. For some tests we collect performance metrics
   and commit them to the Ray repo at ``release/release_logs/<version>``. These
   are currently:

   - Microbenchmark results. This is part of the ``nightly`` release test suite.
   - Benchmark results (``many_actors``, ``many_nodes``, ``many_pgs``)
     These are part of the ``core-nightly`` release test suite.
     Please note that some of the names duplicate those from the long running tests.
     The tests are not the same - make sure you collect the right results.
   - Scalability results (``object_store``, ``single_node``)
     These are part of the ``core-nightly`` release test suite.
   - Stress tests (``dead_actors``, ``many_tasks``, ``placement_group``).
     These are part of the ``core-nightly`` release test suite.

   When you take a look at the test output, you'll find that the logs have been
   saved to S3. If you're logged in in AWS (as the ``anyscale-dev-ossci`` user), you
   can download the results e.g. like this:

   .. code-block:: bash

       aws s3 cp s3://ray-release-automation-results/dev/microbenchmark_1630573490/microbenchmark/output.log microbenchmark.txt

   Clean up the output logfile (e.g. remove TQDM progress bars) before committing the
   release test results.

   The PR should be filed for the Ray ``master`` branch, not the release branch.

4. **For performance tests, check with the teams if the results are acceptable**.
   When a test passes on buildkite it just means that it ran to completion. Some
   tests, especially benchmarks, can pass but still show performance regressions.
   For these tests (usually the same we collect logs for), check with the respective
   teams if the test performance is acceptable.

5. **Repeating release tests**. If one or more tests failed and you need to run
   them again, follow the instructions from the first bullet point. Instead of
   running the full suite, you can use the last two fields to filter the name
   of the test file or the name of the test itself. This is a simple ``if filter in name``
   filter, and only matching tests will be included in the run.

   For instance, if you just want to kick off the ``benchmark_tests/many_actors``
   test, you could specify ``benchmark`` in the test file filter and ``actors``
   in the test name filter.

   As another example, if you just want to kick off all nightly RLLib tests,
   select the respective test suite and specify ``rllib`` in the test file filter.

6. **Kubernetes tests must be run manually.** Refer to ``kubernetes_manual_tests/README.md``.
   Feel free to ping code owner(s) of OSS Kubernetes support to run these.

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

   To have the product Docker images built, ping the product team on the
   Slack channel (#product) and ask them to build the image. Provide
   the latest commit hash and make sure all wheels (Linux, Mac, Windows
   for Python 3.6, 3.7, 3.8, 3.9) are available on S3:

   .. code-block::

       aws s3 ls s3://ray-wheels/releases/1.x.0/<hash>/

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

   Tip: Because downloading the wheels can take a long time, you should
   consider starting an AWS instance just for this. The download will take
   seconds rather than minutes or hours (and even more so the following upload).

4. **Upload to PyPI Test:** Upload the wheels to the PyPI test site using
   ``twine``.

   .. code-block:: bash

     # Downloads all of the wheels to the current directory.
     RAY_VERSION=<version> RAY_HASH=<commit_sha> bash download_wheels.sh

     # Will ask for your PyPI test credentials and require that you're a maintainer
     # on PyPI test. If you are not, ask @robertnishihara to add you.
     pip install twine
     twine upload --repository-url https://test.pypi.org/legacy/ *.whl

   Note that this will upload wheels to two separate repositories:
   `ray <https://test.pypi.org/project/ray/>`__ and
   `ray_cpp <https://test.pypi.org/project/ray_cpp/>`__.
   You'll need access to both repositories in order to complete this step.

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
   sanity check from all the Python version supported. (3.6, 3.7, 3.8, 3.9)

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

   Note that this will upload wheels to two separate repositories:
   `ray <https://pypi.org/project/ray/>`__ and
   `ray_cpp <https://pypi.org/project/ray_cpp/>`__.
   You'll need access to both repositories in order to complete this step.

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

10. **Release the Java packages to Maven**.

    As a prerequisite, you'll need GPG installed and configured.
    `You can download GPG here <https://gpgtools.org/>`_. After setting up
    your key, make sure to publish it to a server so users can validate it.

    You'll also need java 8 and maven set up. On MacOS e.g. via:

    .. code-block:: bash

        brew install openjdk@8
        brew install maven

    Make sure that the Java version strings in the release branch
    have been updated to the current version.

    You'll need to obtain the Maven credentials. These can be found in the
    shared Anyscale 1password (search for "Maven").

    Also look up the latest commit hash for the release branch. Then, run
    the following script to generate the multiplatform jars and publish
    them on Maven:

    .. code-block:: bash

        # Make sure you are under the Ray root source directory.
        export RELEASE_VERSION=1.x.0  # Set the release version
        export OSSRH_KEY=xxx  # Maven username
        export OSSRH_TOKEN=xxx  # Maven password
        export TRAVIS_BRANCH=releases/${RELEASE_VERSION}
        export TRAVIS_COMMIT=xxxxxxxxxxx  # The commit hash
        git checkout $TRAVIS_COMMIT
        sh java/build-jar-multiplatform.sh multiplatform
        export GPG_SKIP=false
        cd java && mvn versions:set -DnewVersion=${RELEASE_VERSION} && cd -
        cd streaming/java && mvn versions:set -DnewVersion=${RELEASE_VERSION} && cd -
        sh java/build-jar-multiplatform.sh deploy_jars

    After that, `log into Sonatype <https://oss.sonatype.org/>`_ and log in
    using the same Maven credentials. Click on "Staging repositories", select
    the respective staging repository, click on "Close" and after that has
    been processed, click on "Release". This will publish the release
    onto the main Maven repository.

    You can check the releases on `mvnrepository.com <https://mvnrepository.com/artifact/io.ray/ray-api>`_.

11. **Send out an email announcing the release** to the employees@anyscale.com
    Google group, and post a slack message in the Announcements channel of the
    Ray slack (message a team lead if you do not have permissions.)

12. **Improve the release process:** Find some way to improve the release
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
.. _`commit for updating the Java version`: https://github.com/ray-project/ray/pull/15394/files
.. _`GitHub release`: https://github.com/ray-project/ray/releases
.. _`Ray Readthedocs version page`: https://readthedocs.org/projects/ray/versions/
.. _`Ray Readthedocs advanced settings page`: https://readthedocs.org/dashboard/ray/advanced/
.. _`Release Checklist`: https://github.com/ray-project/ray/release/RELEASE_CHECKLIST.md
.. _`Releaser`: https://github.com/ray-project/releaser
