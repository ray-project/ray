Contributing to Ray
===================

Reporting bugs and asking questions
-----------------------------------

You can post questions or issues or feedback through the following channels:

1. `Discourse forum`_: For discussions about development and questions about usage.
2. `GitHub Issues`_: For bug reports and feature requests.
3. `StackOverflow`_

Finding issues to work on
-------------------------

We use GitHub labels to categorize issues and help contributors find work that matches their interests. See `What can I work on?`_ for a list of labels by component and type.

To contribute a patch:
----------------------

We welcome contributions! See `Getting Involved`_. To set up your development environment, see
the `Setting up your development environment`_ section.


.. _`Discourse forum`: https://discuss.ray.io/
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
.. _`Getting Involved`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html
.. _`Setting up your development environment`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#setting-up-your-development-environment
.. _`What can I work on?`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#what-can-i-work-on
.. _`PR review process`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#pr-review-process
.. _`AI-assisted contributions`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#ai-assisted-contributions
.. _`CODEOWNERS`: https://github.com/ray-project/ray/blob/master/.github/CODEOWNERS

PR Review Process
-----------------

GitHub automatically requests reviews from the `CODEOWNERS`_ of the files you change. Address their comments and
re-request review until they approve. A committer then adds the ``go`` label to run the full CI, and the PR merges
once CI passes. See `PR review process`_ for details, and `AI-assisted contributions`_ if you used an AI coding agent.
