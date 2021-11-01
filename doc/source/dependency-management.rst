.. _dependency_management:

Dependency Management
=====================

Your Ray project may depend on environment variables, local files, and Python packages.
Ray makes managing these dependencies easy, even when working with a remote cluster.

You can specify dependencies dynamically at runtime using :ref:`Runtime Environments<runtime-environments>`.  This is useful for quickly iterating on a project with changing dependencies and local code files, or for running jobs, tasks and actors with different environments all on the same Ray cluster.

Alternatively, you can prepare your Ray cluster's environment once, when your cluster nodes start up.  This can be
accomplished using ``setup_commands`` in the Ray Cluster launcher; see the :ref:`documentation<cluster-configuration-setup-commands>` for details.
You can still use
runtime environments on top of this, but they will not inherit anything from the base
cluster environment.

.. _runtime-environments:

Runtime Environments
--------------------

.. note::

    This API is in beta and may change before becoming stable.

.. note::

    This feature requires a full installation of Ray using ``pip install "ray[default]"``.

On Mac OS and Linux, Ray 1.4+ supports dynamically setting the runtime environment of tasks, actors, and jobs so that they can depend on different Python libraries (e.g., conda environments, pip dependencies) while all running on the same Ray cluster.

The ``runtime_env`` is a (JSON-serializable) dictionary that can be passed as an option to tasks and actors, and can also be passed to ``ray.init()``.
The runtime environment defines the dependencies required for your workload.

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

Or specify per-actor or per-task in the ``@ray.remote()`` decorator or by using ``.options()``:

.. literalinclude:: ../examples/doc_code/runtime_env_example.py
   :language: python
   :start-after: __per_task_per_actor_start__
   :end-before: __per_task_per_actor_end__

The ``runtime_env`` is a Python dictionary including one or more of the following arguments:

- ``working_dir`` (Path): Specifies the working directory for your job. This must be an existing local directory with total size at most 100 MiB.
  It will be cached on the cluster, so the next time you connect with Ray Client you will be able to skip uploading the directory contents.
  All Ray workers for your job will be started in their node's local copy of this working directory.

  - Examples

    - ``"."  # cwd``

    - ``"/code/my_project"``

  Note: Setting this option per-task or per-actor is currently unsupported.

  Note: If your working directory contains a `.gitignore` file, the files and paths specified therein will not be uploaded to the cluster.

- ``excludes`` (List[str]): When used with ``working_dir``, specifies a list of files or paths to exclude from being uploaded to the cluster.
  This field also supports the pattern-matching syntax used by ``.gitignore`` files: see `<https://git-scm.com/docs/gitignore>`_ for details.

  - Example: ``["my_file.txt", "path/to/dir", "*.log"]``

- ``pip`` (List[str] | str): Either a list of pip packages, or a string containing the path to a pip
  `“requirements.txt” <https://pip.pypa.io/en/stable/user_guide/#requirements-files>`_ file.  The path may be an absolute path or a relative path.
  This will be dynamically installed in the ``runtime_env``.
  To use a library like Ray Serve or Ray Tune, you will need to include ``"ray[serve]"`` or ``"ray[tune]"`` here.

  - Example: ``["requests==1.0.0", "aiohttp"]``

  - Example: ``"./requirements.txt"``

- ``conda`` (dict | str): Either (1) a dict representing the conda environment YAML, (2) a string containing the absolute or relative path to a
  `conda “environment.yml” <https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually>`_ file,
  or (3) the name of a local conda environment already installed on each node in your cluster (e.g., ``"pytorch_p36"``).
  In the first two cases, the Ray and Python dependencies will be automatically injected into the environment to ensure compatibility, so there is no need to manually include them.
  Note that the ``conda`` and ``pip`` keys of ``runtime_env`` cannot both be specified at the same time---to use them together, please use ``conda`` and add your pip dependencies in the ``"pip"`` field in your conda ``environment.yaml``.

  - Example: ``{"dependencies": ["pytorch", “torchvision”, "pip", {"pip": ["pendulum"]}]}``

  - Example: ``"./environment.yml"``

  - Example: ``"pytorch_p36"``


- ``env_vars`` (Dict[str, str]): Environment variables to set.

  - Example: ``{"OMP_NUM_THREADS": "32", "TF_WARNINGS": "none"}``

- ``eager_install`` (bool): A boolean indicates whether to install runtime env eagerly before the workers are leased. This flag is set to True by default and only job level is supported now.

  - Example: ``{"eager_install": False}``

The runtime environment is inheritable, so it will apply to all tasks/actors within a job and all child tasks/actors of a task or actor, once set.

If a child actor or task specifies a new ``runtime_env``, it will be merged with the parent’s ``runtime_env`` via a simple dict update.
For example, if ``runtime_env["pip"]`` is specified, it will override the ``runtime_env["pip"]`` field of the parent.
The one exception is the field ``runtime_env["env_vars"]``.  This field will be `merged` with the ``runtime_env["env_vars"]`` dict of the parent.
This allows for an environment variables set in the parent's runtime environment to be automatically propagated to the child, even if new environment variables are set in the child's runtime environment.

Here are some examples of runtime environments combining multiple options:

..
  TODO(architkulkarni): run working_dir doc example in CI

.. code-block:: python

    runtime_env = {"working_dir": "/files/my_project", "pip": ["pendulum=2.1.2"]}

.. literalinclude:: ../examples/doc_code/runtime_env_example.py
   :language: python
   :start-after: __runtime_env_conda_def_start__
   :end-before: __runtime_env_conda_def_end__
