.. _getting-involved:

Getting Involved / Contributing
===============================

Ray is more than a framework for distributed applications but also an active community of developers,
researchers, and folks that love machine learning.

.. tip:: Ask questions on `our forum <https://discuss.ray.io/>`_! The
  community is extremely active in helping people succeed in building their
  Ray applications.

You can join (and Star!) us on `on GitHub`_.

.. _`on GitHub`: https://github.com/ray-project/ray

Contributing to Ray
-------------------

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

Setting up your development environment
---------------------------------------

To edit the Ray source code, you'll want to checkout the repository and also build Ray from source. Follow :ref:`these instructions for building <building-ray>` a local copy of Ray to easily make changes.

Submitting and Merging a Contribution
-------------------------------------

There are a couple steps to merge a contribution.

1. First merge the most recent version of master into your development branch.

   .. code:: bash

     git remote add upstream https://github.com/ray-project/ray.git
     git pull . upstream/master

2. Make sure all existing `tests <getting-involved.html#testing>`__ and `linters <getting-involved.html#lint-and-formatting>`__ pass.
   Run ``setup_hooks.sh`` to create a git hook that will run the linter before you push your changes.
3. If introducing a new feature or patching a bug, be sure to add new test cases
   in the relevant file in ``ray/python/ray/tests/``.
4. Document the code. Public functions need to be documented, and remember to provide an usage
   example if applicable. See ``doc/README.md`` for instructions on editing and building public documentation.
5. Address comments on your PR. During the review
   process you may need to address merge conflicts with other changes. To resolve merge conflicts,
   run ``git pull . upstream/master`` on your branch (please do not use rebase, as it is less
   friendly to the GitHub review tool. All commits will be squashed on merge.)
6. Reviewers will merge and approve the pull request; be sure to ping them if
   the pull request is getting stale.

PR Review Process
-----------------

For contributors who are in the ``ray-project`` organization:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- When you first create a PR, add an reviewer to the `assignee` section.
- Assignees will review your PR and add the `@author-action-required` label if further actions are required.
- Address their comments and remove the `@author-action-required` label from the PR.
- Repeat this process until assignees approve your PR.
- Once the PR is approved, the author is in charge of ensuring the PR passes the build. Add the `test-ok` label if the build succeeds.
- Committers will merge the PR once the build is passing.

For contributors who are not in the ``ray-project`` organization:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Your PRs will have assignees shortly. Assignees of PRs will be actively engaging with contributors to merge the PR.
- Please actively ping assignees after you address your comments!

Testing
-------

Even though we have hooks to run unit tests automatically for each pull request,
we recommend you to run unit tests locally beforehand to reduce reviewers’
burden and speedup review process.

If you are running tests for the first time, you can install the required dependencies with:

.. code-block:: shell

    pip install -r python/requirements.txt

Testing for Python development
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The full suite of tests is too large to run on a single machine. However, you can run individual relevant Python test files. Suppose that one of the tests in a file of tests, e.g., ``python/ray/tests/test_basic.py``, is failing. You can run just that test file locally as follows:


.. code-block:: shell

    pytest -v -s python/ray/tests/test_basic.py

This will run all of the tests in the file. To run a specific test, use the following:

.. code-block:: shell

    pytest -v -s test_file.py::name_of_the_test

Testing for C++ development
~~~~~~~~~~~~~~~~~~~~~~~~~~~

To compile and run all C++ tests, you can run:

.. code-block:: shell

 bazel test $(bazel query 'kind(cc_test, ...)')

Alternatively, you can also run one specific C++ test. You can use:

.. code-block:: shell

 bazel test $(bazel query 'kind(cc_test, ...)') --test_filter=ClientConnectionTest --test_output=streamed

Code Style
----------

In general, we follow the `Google style guide <https://google.github.io/styleguide/>`__ for C++ code and the `Black code style <https://black.readthedocs.io/en/stable/the_black_code_style/current_style.html>`__ for Python code. However, it is more important for code to be in a locally consistent style than to strictly follow guidelines. Whenever in doubt, follow the local code style of the component.

For Python documentation, we follow a subset of the `Google pydoc format <https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`__. The following code snippet demonstrates the canonical Ray pydoc formatting:

.. code-block:: python

    def ray_canonical_doc_style(param1: int, param2: str) -> bool:
        """First sentence MUST be inline with the quotes and fit on one line.

        Additional explanatory text can be added in paragraphs such as this one.
        Do not introduce multi-line first sentences.

        Examples:
            >>> # Provide code examples as possible.
            >>> ray_canonical_doc_style(41, "hello")
            True

            >>> # A second example.
            >>> ray_canonical_doc_style(72, "goodbye")
            False

        Args:
            param1: The first parameter. Do not include the types in the
                docstring (they should be defined only in the signature).
                Multi-line parameter docs should be indented by four spaces.
            param2: The second parameter.

        Returns:
            The return value. Do not include types here.
        """

Lint and Formatting
~~~~~~~~~~~~~~~~~~~

We also have tests for code formatting and linting that need to pass before merge.

* For Python formatting, install the `required dependencies <https://github.com/ray-project/ray/blob/master/python/requirements_linters.txt>`_ first with:

.. code-block:: shell

  pip install -r python/requirements_linters.txt

* If developing for C++, you will need `clang-format <https://www.kernel.org/doc/html/latest/process/clang-format.html>`_ version ``12`` (download this version of Clang from `here <http://releases.llvm.org/download.html>`_)

You can run the following locally:

.. code-block:: shell

    scripts/format.sh

An output like the following indicates failure:

.. code-block:: shell

  WARNING: clang-format is not installed!  # This is harmless
  From https://github.com/ray-project/ray
   * branch                master     -> FETCH_HEAD
  python/ray/util/sgd/tf/tf_runner.py:4:1: F401 'numpy as np' imported but unused  # Below is the failure

In addition, there are other formatting and semantic checkers for components like the following (not included in ``scripts/format.sh``):

* Python README format:

.. code-block:: shell

    cd python
    python setup.py check --restructuredtext --strict --metadata

* Python & Docs banned words check

.. code-block:: shell

    ./ci/lint/check-banned-words.sh

* Bazel format:

.. code-block:: shell

    ./ci/lint/bazel-format.sh

* clang-tidy for C++ lint, requires ``clang`` and ``clang-tidy`` version 12 to be installed:

.. code-block:: shell

    ./ci/lint/check-git-clang-tidy-output.sh

You can run ``setup_hooks.sh`` to create a git hook that will run the linter before you push your changes.

Understanding CI test jobs
--------------------------

The Ray project automatically runs continuous integration (CI) tests once a PR
is opened using `Buildkite <https://buildkite.com/ray-project/>`_ with
multiple CI test jobs.

The `CI`_ test folder contains all integration test scripts and they
invoke other test scripts via ``pytest``, ``bazel``-based test or other bash
scripts. Some of the examples include:

* Raylet integration tests commands:
    * ``bazel test //:core_worker_test``

* Bazel test command:
    * ``bazel test --build_tests_only //:all``

* Ray serving test commands:
    * ``pytest python/ray/serve/tests``
    * ``python python/ray/serve/examples/echo_full.py``

If a CI build exception doesn't appear to be related to your change,
please visit `this link <https://flakey-tests.ray.io/>`_ to
check recent tests known to be flaky.

.. _`CI`: https://github.com/ray-project/ray/tree/master/ci

API stability
-------------

Ray provides stability guarantees for its public APIs in Ray core and libraries. The level of stability provided depends on how the API is annotated.

.. autofunction:: ray.util.annotations.PublicAPI
.. autofunction:: ray.util.annotations.DeveloperAPI
.. autofunction:: ray.util.annotations.Deprecated

Undecorated functions can be generally assumed to not be part of the Ray public API.

API compatibility style guide
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

It's hard to fully capture the semantics of API compatibility into a single annotation (for example, public APIs may have "experimental" arguments). For more granular stability contracts, those can be noted in the pydoc (e.g., "the ``random_shuffle`` option is experimental"). When possible, experimental arguments should also be prefixed by underscores in Python (e.g., `_owner=`).

**Other recommendations**:

In Python APIs, consider forcing the use of kwargs instead of positional arguments (with the ``*`` operator). Kwargs are easier to keep backwards compatible than positional arguments, e.g. imagine if you needed to deprecate "opt1" below, it's easier with forced kwargs:

.. code-block:: python

    def foo_bar(file, *, opt1=x, opt2=y)
        pass

For callback APIs, consider adding a ``**kwargs`` placeholder as a "forward compatibility placeholder" in case more args need to be passed to the callback in the future, e.g.:
 
.. code-block:: python

    def tune_user_callback(model, score, **future_kwargs):
        pass

Becoming a Reviewer
-------------------

We identify reviewers from active contributors. Reviewers are individuals who
not only actively contribute to the project and are also willing
to participate in the code review of new contributions.
A pull request to the project has to be reviewed by at least one reviewer in order to be merged.
There is currently no formal process, but active contributors to Ray will be
solicited by current reviewers.


More Resources for Getting Involved
-----------------------------------

.. include:: ../ray-contribute/involvement.rst


.. note::

    These tips are based off of the TVM `contributor guide <https://github.com/dmlc/tvm>`__.
