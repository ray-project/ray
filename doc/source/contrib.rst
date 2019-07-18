Contributing to Ray
====================

We welcome (and encourage!) all forms of contributions to Ray, including and not limited to:

- Code reviewing of patches and PRs.
- Pushing patches.
- Documentation and examples.
- Community participation in forums and issues.
- Code readability and code comments to improve readability.
- Test cases to make the codebase more robust.
- Tutorials, blog posts, talks that promote the project.


What can I work on?
-------------------

We use Github to track issues, feature requests, and bugs. Take a look at the
ones labeled `"good first issue" <https://github.com/ray-project/ray/issues?utf8=%E2%9C%93&q=is%3Aissue+is%3Aopen+label%3A%22good+first+issue%22>`__ and `"help wanted" <https://github.com/ray-project/ray/issues?q=is%3Aopen+is%3Aissue+label%3A%22help+wanted%22>`__ for a place to start.

Submitting and Merging a Contribution
-------------------------------------

There are a couple steps to merge a contribution.

1. First rebase your development branch on the most recent version of master.

   .. code:: bash

     git remote add upstream https://github.com/ray-project/ray.git
     git fetch upstream
     git rebase upstream/master

2. Make sure all existing tests `pass <contrib.html#testing>`__.
3. If introducing a new feature or patching a bug, be sure to add new test cases
   in the relevant file in `ray/python/ray/tests/`.
4. Document the code. Public functions need to be documented, and remember to provide an usage
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

    pytest ray/python/ray/Ray/tests/

Documentation should be documented in `Google style <https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`__ format.

We also have tests for code formatting and linting that need to pass before merge.
Install ``yapf==0.23, flake8, flake8-quotes``. You can run the following locally:

.. code-block:: shell

    ray/scripts/format.sh


Becoming a Reviewer
-------------------

We identify reviewers from active contributors. Reviewers are individuals who
not only actively contribute to the project and are also willing
to participate in the code review of new contributions.
A pull request to the project has to be reviewed by at least one reviewer in order to be merged.
There is currently no formal process, but active contributors to Ray will be
solicited by current reviewers.


.. note::

    These tips are based off of the TVM `contributor guide <https://github.com/dmlc/tvm>`__.
