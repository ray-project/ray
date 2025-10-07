Contributing to Ray
===================

Reporting bugs and asking questions
-----------------------------------

You can post questions or issues or feedback through the following channels:

1. `Discourse forum`_: For discussions about development and questions about usage.
2. `GitHub Issues`_: For bug reports and feature requests.
3. `Join Slack`_: For real-time community discussions and support.
4. `StackOverflow`_

To contribute a patch:
----------------------

Before submitting a pull request:

1. Review the `Getting Involved`_ guide for detailed contribution guidelines
2. Set up your `Setting up your development environment`_
3. Follow the `Code Style`_ guidelines (lint and formatting) and the `Testing guide`_ for tests.
4. Sign off all commits with ``git commit -s``
5. Write tests and update docs for your changes

Our PR template will guide you through the rest when you open the PR. 
See the PR Review Process section below for what happens after you submit your PR.

PR Review Process
-----------------

For contributors who are in the ray-project organization:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- When you first create a PR, add a reviewer to the `assignee` section.
- Assignees will review your PR and add `@author-action-required` label if further actions are required.
- Address their comments and remove `@author-action-required` label from the PR.
- Repeat this process until assignees approve your PR.
- Once the PR is approved, the author is in charge of ensuring the PR passes the build. Add `test-ok` label if the build succeeds.
- Committers will merge the PR once the build is passing.

For contributors who are not in the ray-project organization:
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Your PRs will have assignees shortly. Assignees or PRs will be actively engaging with contributors to merge the PR.
- Please actively ping assignees after you address your comments!

.. _`Discourse forum`: https://discuss.ray.io/
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`Join Slack`: https://www.ray.io/join-slack
.. _`Getting Involved`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html
.. _`Setting up your development environment`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#setting-up-your-development-environment
.. _`Code Style`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#code-style
.. _`Testing guide`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#testing
