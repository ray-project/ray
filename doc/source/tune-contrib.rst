Contributing to Tune
====================

We welcome all forms of contributions to Tune, including and not limited to:

- Code reviewing of patches and PRs.
- Pushing patches
- Documentation and examples
- Community participation in forums and issues.
- Code readability and code comments to improve readability
- Test cases to make the codebase more robust
- Tutorials, blog posts, talks that promote the project.


Setting up a development environment
------------------------------------

You can develop Tune locally without needing to compile Ray by using the `setup-dev.py <https://github.com/ray-project/ray/blob/master/python/ray/setup-dev.py>`__ script.
This sets up links between the ``tune`` dir in your git repo and the one bundled with the ``ray`` package.
When using this script, make sure that your git branch is in sync with the installed Ray binaries (i.e., you are up-to-date on `master <https://github.com/ray-project/ray>`__ and have the latest `wheel <https://ray.readthedocs.io/en/latest/installation.html>`__ installed.)


Your first contribution
-----------------------


Take a look at TODO(rliaw) for new issues to work on.


Forking
-------
You will need your own fork to work on the code. Go to the ray project page and hit the Fork button. You will want to clone your fork to your machine:

.. code-block:: shell

    git clone https://github.com/your-user-name/ray.git ray-yourname
    cd ray-yourname
    git remote add upstream https://github.com/ray-project/ray.git


This creates the directory pandas-yourname and connects your repository to the upstream (main project) pandas repository.



Submitting a Pull Request
-------------------------




Testing
-------

Even though we have hooks to run unit tests automatically for each pull request, It’s always recommended to run unit tests locally beforehand to reduce reviewers’ burden and speedup review process.



Becoming a Reviewer
-------------------

Reviewers are individuals who actively contributed to the project and are willing to participate in the code review of new contributions. We identify reviewers from active contributors. The committers should explicitly solicit reviews from reviewers. High-quality code reviews prevent technical debt for long-term and are crucial to the success of the project. A pull request to the project has to be reviewed by at least one reviewer in order to be merged.

