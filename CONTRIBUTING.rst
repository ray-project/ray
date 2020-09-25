Contributing to Ray
===================

Reporting bugs and asking questions
-----------------------------------

You can post questions or issues or feedback through the following channels:

1. `Github Discussions`_: For discussions about development, questions about usage, and feature requests.
2. `GitHub Issues`_: For bug reports and feature requests.
3. `StackOverflow`_

To contribute a patch:
----------------------

1. Break your work into small, single-purpose patches if possible. It's much
   harder to merge in a large change with a lot of disjoint features.
2. Submit the patch as a GitHub pull request against the master branch.
3. Make sure that your code passes the unit tests.
4. Make sure that your code passes the linter. Run setup_hooks.sh to create
   a git hook that will run the linter before you push your changes.
5. Add new unit tests for your code.

.. _`Github Discussions`: https://github.com/ray-project/ray/discussions
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray

PR Review Process
-----------------

For contributors who are in the ray-project organization:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- When you first create a PR, add an reviewer to the `assignee` section.
- Assignees will review your PR and add `@author-action-required` label if further actions are required.
- Address their comments and remove `@author-action-required` label from the PR.
- Repeat this process until assignees approve your PR.
- Once the PR is approved, the author is in charge of ensuring the PR passes the build. Add `test-ok` label if the build succeeds.
- Committers will merge the PR once the build is passing.

For contributors who are not in the ray-project organization:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Your PRs will have assignees shortly. Assignees or PRs will be actively engaging with contributors to merge the PR.
- Please actively ping assignees after you address your comments!
