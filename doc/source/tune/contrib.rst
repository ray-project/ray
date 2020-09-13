.. _tune-contrib:

Contributing to Tune
====================

We welcome (and encourage!) all forms of contributions to Tune, including and not limited to:

- Code reviewing of patches and PRs.
- Pushing patches.
- Documentation and examples.
- Community participation in forums and issues.
- Code readability and code comments to improve readability.
- Test cases to make the codebase more robust.
- Tutorials, blog posts, talks that promote the project.

Developing Tune
---------------

First, following the instructions in :ref:`python-develop` to develop Tune without compiling Ray.

After Ray is set up, run ``pip install -r ray/python/ray/tune/requirements-dev.txt`` to install all packages required for Tune development.

Submitting and Merging a Contribution
-------------------------------------

There are a couple steps to merge a contribution.

1. First rebase your development branch on the most recent version of master.

   .. code:: bash

     git remote add upstream https://github.com/ray-project/ray.git
     git fetch upstream
     git rebase upstream/master # or git pull . upstream/master

2. Make sure all existing tests `pass <tune-contrib.html#testing>`__.
3. If introducing a new feature or patching a bug, be sure to add new test cases
   in the relevant file in ``tune/tests/``.
4. Document the code. Public functions need to be documented, and remember to provide a usage
   example if applicable.
5. Request code reviews from other contributors and address their comments. One fast way to get reviews is
   to help review others' code so that they return the favor. You should aim to improve the code as much as
   possible before the review. We highly value patches that can get in without extensive reviews.
6. Reviewers will merge and approve the pull request; be sure to ping them if
   the pull request is getting stale.


Testing
-------

Even though we have hooks to run unit tests automatically for each pull request,
we recommend you to run unit tests locally beforehand to reduce reviewers’
burden and speedup review process.


.. code-block:: shell

    pytest ray/python/ray/tune/tests/

Documentation should be documented in `Google style <https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`__ format.

We also have tests for code formatting and linting that need to pass before merge. You can run the following locally:

.. code-block:: shell

    ray/scripts/format.sh


What can I work on?
-------------------

We use Github to track issues, feature requests, and bugs. Take a look at the
ones labeled `"good first issue" <https://github.com/ray-project/ray/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22>`__ and `"help wanted" <https://github.com/ray-project/ray/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22>`__ for a place to start. Look for issues with "[tune]" in the title.

.. note::

  If raising a new issue or PR related to Tune, be sure to include "[tune]" in the title and add a ``tune`` label.

For project organization, Tune maintains a relatively up-to-date organization of
issues on the `Tune Github Project Board <https://github.com/ray-project/ray/projects/4>`__.
Here, you can track and identify how issues are organized.


Becoming a Reviewer
-------------------

We identify reviewers from active contributors. Reviewers are individuals who
not only actively contribute to the project and are also willing
to participate in the code review of new contributions.
A pull request to the project has to be reviewed by at least one reviewer in order to be merged.
There is currently no formal process, but active contributors to Tune will be
solicited by current reviewers.


.. note::

    These tips are based off of the TVM `contributor guide <https://github.com/dmlc/tvm>`__.
