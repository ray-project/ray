Contributing to Ray
===================

Reporting bugs and asking questions
-----------------------------------

Post questions, issues, and feedback through the following channels:

1. `GitHub Issues`_: For bug reports and feature requests.
2. `Discourse forum`_: For discussions about development and questions about usage.
3. `Stack Overflow`_: For usage questions tagged ``ray``.

Finding issues to work on
-------------------------

Ray uses GitHub labels to categorize issues and help contributors find work that matches their interests. See `What can I work on?`_ for a list of labels by component and type.

Contributing a patch
--------------------

The Ray project welcomes contributions. To get started, see `Getting involved and contributing`_. To set up your
development environment, see `Setting up your development environment`_.

.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`Getting involved and contributing`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html
.. _`Setting up your development environment`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#setting-up-your-development-environment
.. _`What can I work on?`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#what-can-i-work-on
.. _`PR review process`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#pr-review-process
.. _`AI-assisted contributions`: https://docs.ray.io/en/latest/ray-contribute/getting-involved.html#ai-assisted-contributions
.. _`code owners`: https://github.com/ray-project/ray/blob/master/.github/CODEOWNERS
.. _`Discourse forum`: https://discuss.ray.io/
.. _`Stack Overflow`: https://stackoverflow.com/questions/tagged/ray

PR review process
-----------------

GitHub automatically requests reviews from the `code owners`_ of the files you change. Address their comments and
re-request review until they approve. A committer then adds the ``go`` label to run the full CI test suite, and the PR
merges after CI passes. For details, see `PR review process`_. If you used an AI coding agent, also see
`AI-assisted contributions`_.
