.. _handling_dependencies:

Handling Dependencies
=====================

This page might be useful for you if you're trying to:

* Run your distributed Ray library or application.
* Run your distributed Ray script, which imports some local files.
* Quickly iterating on a project with changing dependencies and files while running on a Ray cluster.


What problem does this page solve?
----------------------------------

Your Ray application may have a couple "dependencies" that exist outside of your Ray script. For example:

* Your Ray script may import/depend on some Python packages.
* Your Ray script may be looking for some specific environment variables to be available.
* Your Ray script may import some files outside of the script (i.e., via relative imports)


One frequent problem when running on a cluster is that Ray expects these "dependencies" to exist on each Ray node. Otherwise, you may run into different issues such as <TODO ADD ISSUE LINK>.


To address this problem, you can leverage Ray's **Runtime environments**.


Concepts
--------

- **Ray Application**

- **Local machine** and **Cluster**.  The recommended way to connect to a remote Ray cluster is to use :ref:`Ray Client<ray-client>`, and we will call the machine running Ray Client your *local machine*.  Note: you can also start a single-node Ray cluster on your local machine---in this case your Ray cluster is not really “remote”, but any comments in this documentation referring to a “remote cluster” will also apply to this setup.

- **Files**: These are the files that your Ray application needs to run.  These can include code files or data files.  For a development workflow, these might live on your local machine, but when it comes time to run things at scale, you will need to get them to your remote cluster.  For how to do this, see :ref:`Workflow: Local Files<workflow-local-files>` below.

- **Packages**: These are external libraries or executables required by your Ray application, often installed via ``pip`` or ``conda``.

.. Alternatively, you can prepare your Ray cluster's environment when your cluster nodes start up, and modify it later from the command line.
.. Packages can be installed using ``setup_commands`` in the Ray Cluster configuration file (:ref:`docs<cluster-configuration-setup-commands>`) and files can be pushed to the cluster using ``ray rsync_up`` (:ref:`docs<ray-rsync>`).


.. _runtime-environments:

Runtime Environments
--------------------

.. note::

    This feature requires a full installation of Ray using ``pip install "ray[default]>=1.4"``, and is currently only supported on macOS and Linux.

A **runtime environment** describes the dependencies your Ray application needs to run, including :ref:`files, packages, environment variables, and more <runtime-environments-api-ref>`.

Runtime environment enable the transition of running of your Ray application on your local machine to a remote cluster, without any code changes or manual setup.

..
  TODO(architkulkarni): run working_dir doc example in CI

.. code-block:: python

    runtime_env = {"working_dir": "/data/my_files", "pip": ["requests", "pendulum==2.1.2"]}

    # RLIAW: ADD example of this actually working

.. literalinclude:: ../examples/doc_code/runtime_env_example.py
   :language: python
   :start-after: __runtime_env_conda_def_start__
   :end-before: __runtime_env_conda_def_end__

Jump to the :ref:`API Reference<runtime-environments-api-ref>`.


There are two primary ways of using runtime environments:

* Per Ray Job (TODO: define job) <link down>
* Per Ray Task/Actor, within a job <link down>


Specifying a Runtime Environment Per-Job
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can specify a runtime environment for your whole job, whether running a script directly on the cluster or using :ref:`Ray Client<ray-client>`:

.. literalinclude:: ../examples/doc_code/runtime_env_example.py
   :language: python
   :start-after: __ray_init_start__
   :end-before: __ray_init_end__

..
  TODO(architkulkarni): run Ray Client doc example in CI

.. code-block:: python

    # Running on a local machine, connecting to remote cluster using Ray Client
    ray.init("ray://123.456.7.89:10001", runtime_env=runtime_env)

.. note::

  This will eagerly install the environment when ``ray.init()`` is called.  To disable this, add ``"eager_install": False`` to the ``runtime_env``.  This will only install the environment when a task is invoked or an actor is created.

Specifying a Runtime Environment Per-Task or Per-Actor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can specify different runtime environments per-actor or per-task using ``.options()`` or the ``@ray.remote()`` decorator:

.. literalinclude:: ../examples/doc_code/runtime_env_example.py
   :language: python
   :start-after: __per_task_per_actor_start__
   :end-before: __per_task_per_actor_end__

This allows you to have actors and tasks running in their own environments, independent of the surrounding environment. (The surrounding environment could be the job's runtime environment, or the base environment of the cluster.)

.. _workflow-local-files:

Common Workflow: Local files
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Your Ray application might depend on source files or data files.
The following simple example explains how to get your local files on the cluster.

.. code-block:: python

  # /path/to/files is a directory on the local machine.
  # /path/to/files/hello.txt contains the string "Hello World!"

  import ray

  # Specify a runtime environment for the entire Ray "Job"
  ray.init(runtime_env={"working_dir": "/path/to/files"})

  # Create a Ray task, which inherits the above runtime env.
  @ray.remote
  def f():
      # The function will have its working_dir changed to /path/to/files

      return open("hello.txt").read()

  print(ray.get(f.remote())) # Hello World!

.. note::
  The example works on a local machine, but this also works when specifying an Ray cluster/client address (e.g. ``ray.init("ray://123.456.7.89:10001", runtime_env=...)`` to connect to a remote cluster.

The specified local directory will automatically be pushed to the cluster nodes when ``ray.init()`` is called.

Workflow: Ray Library development
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Suppose you are developing a library ``my_module`` on Ray.

A typical iteration cycle will involve
* making some changes to the source code of ``my_module``
* Running a Ray script on the cluster to test the changes, perhaps on a distributed cluster.

To ensure your local changes show up across all Ray workers and can be imported properly, use the ``py_modules`` field.

.. code-block:: python

  # TODO: add comment about module.

  import ray
  import my_module

  ray.init("ray://123.456.7.89:10001", runtime_env={"py_modules": [my_module]})

  @ray.remote
  def test_my_module():
      # No need to import my_module inside this function.
      my_module.test()

  ray.get(f.remote())

.. _runtime-environments-api-ref:

API Reference
^^^^^^^^^^^^^

The ``runtime_env`` is a Python dictionary including one or more of the following keys:

- ``working_dir`` (str): Specifies the working directory for the Ray workers. This must either be an existing directory on the local machine with total size at most 100 MiB, or a path to a zip file stored in Amazon S3.
  If a local directory is specified, it will be uploaded to each node on the cluster.
  Ray workers will be started in their node's copy of this directory.

  - Examples

    - ``"."  # cwd``

    - ``"/src/my_project"``

    - ``"s3://path/to/my_dir.zip"``

  Note: Setting a local directory per-task or per-actor is currently unsupported.

  Note: If your local directory contains a ``.gitignore`` file, the files and paths specified therein will not be uploaded to the cluster.

- ``py_modules`` (List[str|module]): Specifies Python modules to import in the Ray workers.  (For more ways to specify packages, see also the ``pip`` and ``conda`` fields below.)
  Each entry must be either (1) a path to a local directory, (2) a path to a zip file stored in Amazon S3, or (3) a Python module object.

  - Examples

    - ``"."``

    - ``"/src/my_module"``

    - ``"s3://path/to/my_module.zip"``

    - ``my_module # Assumes my_module has already been imported, e.g. via 'import my_module'``

  Note: Note: Setting options (1) and (3) per-task or per-actor is currently unsupported.

  Note: For option (1), if your local directory contains a ``.gitignore`` file, the files and paths specified therein will not be uploaded to the cluster.
- ``excludes`` (List[str]): When used with ``working_dir`` or ``py_modules``, specifies a list of files or paths to exclude from being uploaded to the cluster.
  This field also supports the pattern-matching syntax used by ``.gitignore`` files: see `<https://git-scm.com/docs/gitignore>`_ for details.

  - Example: ``["my_file.txt", "path/to/dir", "*.log"]``

- ``pip`` (List[str] | str): Either a list of pip `requirements specifiers <https://pip.pypa.io/en/stable/cli/pip_install/#requirement-specifiers>`_, or a string containing the path to a pip
  `“requirements.txt” <https://pip.pypa.io/en/stable/user_guide/#requirements-files>`_ file.
  This will be installed in the Ray workers at runtime.
  To use a library like Ray Serve or Ray Tune, you will need to include ``"ray[serve]"`` or ``"ray[tune]"`` here.

  - Example: ``["requests==1.0.0", "aiohttp", "ray[serve]"]``

  - Example: ``"./requirements.txt"``

- ``conda`` (dict | str): Either (1) a dict representing the conda environment YAML, (2) a string containing the path to a
  `conda “environment.yml” <https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually>`_ file,
  or (3) the name of a local conda environment already installed on each node in your cluster (e.g., ``"pytorch_p36"``).
  In the first two cases, the Ray and Python dependencies will be automatically injected into the environment to ensure compatibility, so there is no need to manually include them.
  Note that the ``conda`` and ``pip`` keys of ``runtime_env`` cannot both be specified at the same time---to use them together, please use ``conda`` and add your pip dependencies in the ``"pip"`` field in your conda ``environment.yaml``.

  - Example: ``{"dependencies": ["pytorch", “torchvision”, "pip", {"pip": ["pendulum"]}]}``

  - Example: ``"./environment.yml"``

  - Example: ``"pytorch_p36"``


- ``env_vars`` (Dict[str, str]): Environment variables to set.

  - Example: ``{"OMP_NUM_THREADS": "32", "TF_WARNINGS": "none"}``

- ``eager_install`` (bool): Indicates whether to install the runtime env at `ray.init()` time, before the workers are leased. This flag is set to ``True`` by default.
  Currently, specifying this option per-actor or per-task is not supported.

  - Example: ``{"eager_install": False}``

The runtime environment is inheritable, so it will apply to all tasks/actors within a job and all child tasks/actors of a task or actor, once set.

If a child actor or task specifies a new ``runtime_env``, it will be merged with the parent’s ``runtime_env`` via a simple dict update.
For example, if ``runtime_env["pip"]`` is specified, it will override the ``runtime_env["pip"]`` field of the parent.
The one exception is the field ``runtime_env["env_vars"]``.  This field will be `merged` with the ``runtime_env["env_vars"]`` dict of the parent.
This allows for an environment variables set in the parent's runtime environment to be automatically propagated to the child, even if new environment variables are set in the child's runtime environment.

Here are some examples of runtime environments combining multiple options:

