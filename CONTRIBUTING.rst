Contributing
============

Instructions for contributors
-----------------------------


In order to make a clone of the GitHub_ repo: open the link and press the
"Fork" button on the upper-right menu of the web page.

I hope everybody knows how to work with git and github nowadays :)

Workflow is pretty straightforward:

  1. Clone the GitHub_ repo

  2. Make a change

  3. Make sure all tests passed

  4. Add a file into ``CHANGES`` folder (`Changelog update`_).

  5. Commit changes to own ray clone

  6. Make pull request from github page for your clone against master branch

  .. note::
     If your PR has long history or many commits
     please rebase it from main repo before creating PR.

Preconditions for running ray test suite
--------------------------------------------

We expect you to use a python virtual environment to run our tests.

There are several ways to make a virtual environment.

If you like to use *virtualenv* please run:

.. code-block:: shell

   $ cd ray
   $ virtualenv --python=`which python3` venv
   $ . venv/bin/activate

For standard python *venv*:

.. code-block:: shell

   $ cd ray
   $ python3 -m venv venv
   $ . venv/bin/activate

For *virtualenvwrapper*:

.. code-block:: shell

   $ cd ray
   $ mkvirtualenv --python=`which python3` ray

There are other tools like *pyvenv* but you know the rule of thumb
now: create a python3 virtual environment and activate it.

After that please install libraries required for development:

.. code-block:: shell

   $ pip install -r requirements/dev.txt

.. note::
  If you plan to use ``pdb`` or ``ipdb`` within the test suite, execute:

.. code-block:: shell

    $ py.test tests -s

  command to run the tests with disabled output capturing.

Congratulations, you are ready to run the test suite!


Run ray test suite
----------------------

After all the preconditions are met you can run tests typing the next
command:

.. code-block:: shell

   $ python test/runtest.py

The command at first will run the *flake8* tool (sorry, we don't accept
pull requests with pep8 or pyflakes errors).

On *flake8* success the tests will be run.

Please take a look on the produced output.

Any extra texts (print statements and so on) should be removed.


Documentation
-------------

We encourage documentation improvements.

Please before making a Pull Request about documentation changes run:

.. code-block:: shell

   $ pip install -r requirements-doc.txt
   $ make html

Once it finishes it will output the index html page
``open file:///.../ray/docs/_build/html/index.html``.

Go to the link and make sure your doc changes looks good.


The End
-------

After finishing all steps make a GitHub_ Pull Request, thanks.


.. _GitHub: https://github.com/ray-project/ray

.. _ipdb: https://pypi.python.org/pypi/ipdb
