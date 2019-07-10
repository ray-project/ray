Contributing to Ray
===================

Reporting bugs and asking questions
-----------------------------------

You can post questions or issues or feedback through the following channels:

1. `ray-dev@googlegroups.com`_: For discussions about development or any general
   questions and feedback.
2. `StackOverflow`_: For questions about how to use Ray.
3. `GitHub Issues`_: For bug reports and feature requests.

To contribute a patch:
----------------------

1. Break your work into small, single-purpose patches if possible. It's much
   harder to merge in a large change with a lot of disjoint features.
2. Submit the patch as a GitHub pull request against the master branch.
3. Make sure that your code passes the unit tests.
4. Make sure that your code passes the linter. Run setup_hooks.sh to create
   a git hook that will run the linter before you push your changes.
5. Add new unit tests for your code.

.. _`ray-dev@googlegroups.com`: https://groups.google.com/forum/#!forum/ray-dev
.. _`GitHub Issues`: https://github.com/ray-project/ray/issues
.. _`StackOverflow`: https://stackoverflow.com/questions/tagged/ray
