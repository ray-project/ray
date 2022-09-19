Contributing to Ray
===================

Reporting bugs and asking questions
-----------------------------------

You can post questions or issues or feedback through the following channels:

1. `Discourse forum`_: For discussions about development and questions about usage.
2. `GitHub Issues`_: For bug reports and feature requests.
3. `StackOverflow`_

To contribute a patch:
----------------------

We welcome your contribution!  Please see https://docs.ray.io/en/master/getting-involved.html for instructions.

For instructions on setting up your development environment, check out the
`getting involved`_ documentation page.


.. _`Discourse forum`: https://discuss.ray.io/
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`getting involved`: https://docs.ray.io/en/master/getting-involved.html

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
