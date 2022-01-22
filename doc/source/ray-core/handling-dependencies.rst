.. _handling_dependencies:

Handling Dependencies
=====================

This page might be useful for you if you're trying to:

* Run a distributed Ray library or application.
* Run a distributed Ray script which imports some local files.
* Quickly iterate on a project with changing dependencies and files while running on a Ray cluster.


What problem does this page solve?
----------------------------------

Your Ray application may have dependencies that exist outside of your Ray script. For example:

* Your Ray script may import/depend on some Python packages.
* Your Ray script may be looking for some specific environment variables to be available.
* Your Ray script may import some files outside of the script.


One frequent problem when running on a cluster is that Ray expects these "dependencies" to exist on each Ray node. If these are not present, you may run into issues such as ``ModuleNotFoundError``, ``FileNotFoundError`` and so on.



To address this problem, you can use Ray's **runtime environments**.


Concepts
--------

- **Ray Application**.  A program including a Ray script that calls ``ray.init()`` and uses Ray tasks or actors.

- **Dependencies**, or **Environment**.  Anything outside of the Ray script that your application needs to run, including files, packages, and environment variables.

- **Files**: Code files, data files or other files that your Ray application needs to run.

- **Packages**: External libraries or executables required by your Ray application, often installed via ``pip`` or ``conda``.

- **Local machine** and **Cluster**.  Usually, you may want to separate the Ray cluster compute machines/pods from the machine/pod that handles and submits the application. You can submit a Ray Job via :ref:`the Ray Job Submission mechanism <jobs-overview>`, or the :ref:`Ray Client<ray-client>` to connect to a cluster interactively. We call the machine submitting the job your *local machine*.

- **Job**.  A period of execution between connecting to a cluster with ``ray.init()`` and disconnecting by calling ``ray.shutdown()`` or exiting the Ray script.


.. Alternatively, you can prepare your Ray cluster's environment when your cluster nodes start up, and modify it later from the command line.
.. Packages can be installed using ``setup_commands`` in the Ray Cluster configuration file (:ref:`docs<cluster-configuration-setup-commands>`) and files can be pushed to the cluster using ``ray rsync_up`` (:ref:`docs<ray-rsync>`).


.. _runtime-environments:

Runtime Environments
--------------------

.. note::

    This feature requires a full installation of Ray using ``pip install "ray[default]"``. This feature is available starting with Ray 1.4.0 and is currently only supported on macOS and Linux.

A **runtime environment** describes the dependencies your Ray application needs to run, including :ref:`files, packages, environment variables, and more <runtime-environments-api-ref>`.  It is installed dynamically on the cluster at runtime.

Runtime environments let you transition your Ray application from running on your local machine to running on a remote cluster, without any manual environment setup.

..
  TODO(architkulkarni): run working_dir doc example in CI

.. code-block:: python

    import ray
    import requests

    runtime_env = {"working_dir": "/data/my_files", "pip": ["requests", "pendulum==2.1.2"]}

    # To transition from a local single-node cluster to a remote cluster,
    # simply change to ray.init("ray://123.456.7.8:10001", runtime_env=...)
    ray.init(runtime_env=runtime_env)

    @ray.remote()
    def f():
      open("my_datafile.txt").read()
      return requests.get("https://www.ray.io")

.. literalinclude:: /ray-core/_examples/doc_code/runtime_env_example.py
   :language: python
   :start-after: __runtime_env_conda_def_start__
   :end-before: __runtime_env_conda_def_end__

Jump to the :ref:`API Reference<runtime-environments-api-ref>`.


There are two primary scopes for which you can specify a runtime environment:

* :ref:`Per-Job <rte-per-job>`, and
* :ref:`Per-Task/Actor, within a job <rte-per-task-actor>`.

.. _rte-per-job:

Specifying a Runtime Environment Per-Job
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can specify a runtime environment for your whole job, whether running a script directly on the cluster, using :ref:`Ray Job submission <jobs-overview>`, or using :ref:`Ray Client<ray-client>`:

.. literalinclude:: /ray-core/_examples/doc_code/runtime_env_example.py
   :language: python
   :start-after: __ray_init_start__
   :end-before: __ray_init_end__

..
  TODO(architkulkarni): run Ray Client doc example in CI

.. code-block:: python

    # Connecting to remote cluster using Ray Client
    ray.init("ray://123.456.7.89:10001", runtime_env=runtime_env)

This will install the dependencies to the remote cluster.  Any tasks and actors used in the job will use this runtime environment unless otherwise specified.

.. note::

  There are two options for when to install the runtime environment:

    1. As soon as the job starts (i.e., as soon as ``ray.init()`` is called), the dependencies are eagerly downloaded and installed.
    2. The dependencies are installed only when a task is invoked or an actor is created.

  The default is option 1. To change the behavior to option 2, add ``"eager_install": False`` to the ``runtime_env``.

.. _rte-per-task-actor:

Specifying a Runtime Environment Per-Task or Per-Actor
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can specify different runtime environments per-actor or per-task using ``.options()`` or the ``@ray.remote()`` decorator:

.. literalinclude:: /ray-core/_examples/doc_code/runtime_env_example.py
   :language: python
   :start-after: __per_task_per_actor_start__
   :end-before: __per_task_per_actor_end__

This allows you to have actors and tasks running in their own environments, independent of the surrounding environment. (The surrounding environment could be the job's runtime environment, or the system environment of the cluster.)

.. warning::

  Ray does not guarantee compatibility between tasks and actors with conflicting runtime environments.
  For example, if an actor whose runtime environment contains a ``pip`` package tries to communicate with an actor with a different version of that package, it can lead to unexpected behavior such as unpickling errors.

Common Workflows
^^^^^^^^^^^^^^^^

This section describes some common use cases for runtime environments. These use cases are not mutually exclusive; all of the options described below can be combined in a single runtime environment.

.. _workflow-local-files:

Using Local Files
"""""""""""""""""

Your Ray application might depend on source files or data files.
For a development workflow, these might live on your local machine, but when it comes time to run things at scale, you will need to get them to your remote cluster.

The following simple example explains how to get your local files on the cluster.

.. code-block:: python

  # /path/to/files is a directory on the local machine.
  # /path/to/files/hello.txt contains the string "Hello World!"

  import ray

  # Specify a runtime environment for the entire Ray job
  ray.init(runtime_env={"working_dir": "/path/to/files"})

  # Create a Ray task, which inherits the above runtime env.
  @ray.remote
  def f():
      # The function will have its working directory changed to its node's
      # local copy of /path/to/files.
      return open("hello.txt").read()

  print(ray.get(f.remote())) # Hello World!

.. note::
  The example above is written to run on a local machine, but as for all of these examples, it also works when specifying a Ray cluster to connect to
  (e.g., using ``ray.init("ray://123.456.7.89:10001", runtime_env=...)`` or ``ray.init(address="auto", runtime_env=...)``).

The specified local directory will automatically be pushed to the cluster nodes when ``ray.init()`` is called.

You can also specify files via a remote cloud storage URI; see :ref:`remote-uris` for details.

Using ``conda`` or ``pip`` packages
"""""""""""""""""""""""""""""""""""

Your Ray application might depend on Python packages (for example, ``pendulum`` or ``requests``) via ``import`` statements.

Ray ordinarily expects all imported packages to be preinstalled on every node of the cluster; in particular, these packages are not automatically shipped from your local machine to the cluster or downloaded from any repository.

However, using runtime environments you can dynamically specify packages to be automatically downloaded and installed in an isolated virtual environment for your Ray job, or for specific Ray tasks or actors.

.. code-block:: python

  import ray
  import requests

  # This example runs on a local machine, but you can also do
  # ray.init(address=..., runtime_env=...) to connect to a cluster.
  ray.init(runtime_env={"pip": ["requests"]})

  @ray.remote
  def reqs():
      return requests.get("https://www.ray.io/")

  print(ray.get(reqs.remote())) # <Response [200]>


You may also specify your ``pip`` dependencies either via a Python list or a ``requirements.txt`` file.
Alternatively, you can specify a ``conda`` environment, either as a Python dictionary or via a ``environment.yml`` file.  This conda environment can include ``pip`` packages.
For details, head to the :ref:`API Reference<runtime-environments-api-ref>`.

.. note::

  The ``ray[default]`` package itself will automatically be installed in the isolated environment.  However, if you are using any Ray libraries (for example, Ray Serve), then you will need to specify the library in the runtime environment (e.g. ``runtime_env = {"pip": ["requests", "ray[serve]"}]}``.)

.. warning::

  Since the packages in the ``runtime_env`` are installed at runtime, be cautious when specifying ``conda`` or ``pip`` packages whose installations involve building from source, as this can be slow.

Library Development
"""""""""""""""""""

Suppose you are developing a library ``my_module`` on Ray.

A typical iteration cycle will involve

1. Making some changes to the source code of ``my_module``
2. Running a Ray script to test the changes, perhaps on a distributed cluster.

To ensure your local changes show up across all Ray workers and can be imported properly, use the ``py_modules`` field.

.. code-block:: python

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

The ``runtime_env`` is a Python dictionary including one or more of the following fields:

- ``working_dir`` (str): Specifies the working directory for the Ray workers. This must either be (1) an local existing directory with total size at most 100 MiB, (2) a local existing zipped file with total unzipped size at most 100 MiB (Note: ``excludes`` has no effect), or (3) a URI to a remotely-stored zip file containing the working directory for your job. See :ref:`remote-uris` for details.
  The specified directory will be downloaded to each node on the cluster, and Ray workers will be started in their node's copy of this directory.

  - Examples

    - ``"."  # cwd``

    - ``"/src/my_project"``

    - ``"/src/my_project.zip"``

    - ``"s3://path/to/my_dir.zip"``

  Note: Setting a local directory per-task or per-actor is currently unsupported; it can only be set per-job (i.e., in ``ray.init()``).

  Note: If your local directory contains a ``.gitignore`` file, the files and paths specified therein will not be uploaded to the cluster.

- ``py_modules`` (List[str|module]): Specifies Python modules to be available for import in the Ray workers.  (For more ways to specify packages, see also the ``pip`` and ``conda`` fields below.)
  Each entry must be either (1) a path to a local directory, (2) a URI to a remote zip file (see :ref:`remote-uris` for details), or (3) a Python module object.

  - Examples of entries in the list:

    - ``"."``

    - ``"/local_dependency/my_module"``

    - ``"s3://bucket/my_module.zip"``

    - ``my_module # Assumes my_module has already been imported, e.g. via 'import my_module'``

  The modules will be downloaded to each node on the cluster.

  Note: Setting options (1) and (3) per-task or per-actor is currently unsupported, it can only be set per-job (i.e., in ``ray.init()``).

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

  - Example: ``{"dependencies": ["pytorch", "torchvision", "pip", {"pip": ["pendulum"]}]}``

  - Example: ``"./environment.yml"``

  - Example: ``"pytorch_p36"``


- ``env_vars`` (Dict[str, str]): Environment variables to set.

  - Example: ``{"OMP_NUM_THREADS": "32", "TF_WARNINGS": "none"}``

- ``container`` (dict): Require a given (Docker) image, and the worker process will run in a container with this image.
  The `worker_path` is the default_worker.py path. It is required only if ray installation directory in the container is different from raylet host.
  The `run_options` list spec is here: https://docs.docker.com/engine/reference/run/.

  - Example: ``{"image": "anyscale/ray-ml:nightly-py38-cpu", "worker_path": "/root/python/ray/workers/default_worker.py", "run_options": ["--cap-drop SYS_ADMIN","--log-level=debug"]}``

  Note: ``container`` is experimental now. If you have some requirements or run into any problems, raise issues in `github <https://github.com/ray-project/ray/issues>`.

- ``eager_install`` (bool): Indicates whether to install the runtime environment on the cluster at ``ray.init()`` time, before the workers are leased. This flag is set to ``True`` by default.
  If set to ``False``, the runtime environment will be only installed when the first task is invoked or when the first actor is created.
  Currently, specifying this option per-actor or per-task is not supported.

  - Example: ``{"eager_install": False}``

**Garbage Collection**.  Runtime environment resources on each node (such as conda environments, pip packages, or downloaded ``working_dir`` or ``py_modules`` folders) will be removed when they are no longer
referenced by any actor, task or job.  To disable this (for example, for debugging purposes) set the environment variable ``RAY_runtime_env_skip_local_gc`` to ``1`` on each node in your cluster before starting Ray (e.g. with ``ray start``).

Inheritance
"""""""""""

The runtime environment is inheritable, so it will apply to all tasks/actors within a job and all child tasks/actors of a task or actor once set, unless it is overridden.

If an actor or task specifies a new ``runtime_env``, it will override the parent’s ``runtime_env`` (i.e., the parent actor/task's ``runtime_env``, or the job's ``runtime_env`` if there is no parent actor or task) as follows:

* The ``runtime_env["env_vars"]`` field will be merged with the ``runtime_env["env_vars"]`` field of the parent.
  This allows for environment variables set in the parent's runtime environment to be automatically propagated to the child, even if new environment variables are set in the child's runtime environment.
* Every other field in the ``runtime_env`` will be *overridden* by the child, not merged.  For example, if ``runtime_env["py_modules"]`` is specified, it will replace the ``runtime_env["py_modules"]`` field of the parent.

Example:

.. code-block:: python

  # Parent's `runtime_env`
  {"pip": ["requests", "chess"],
  "env_vars": {"A": "a", "B": "b"}}

  # Child's specified `runtime_env`
  {"pip": ["torch", "ray[serve]"],
  "env_vars": {"B": "new", "C", "c"}}

  # Child's actual `runtime_env` (merged with parent's)
  {"pip": ["torch", "ray[serve]"],
  "env_vars": {"A": "a", "B": "new", "C", "c"}}


.. _remote-uris:

Remote URIs
-----------

The ``working_dir`` and ``py_modules`` arguments in the ``runtime_env`` dictionary can specify either local path(s) or remote URI(s).

A local path must be a directory path. The directory's contents will be directly accessed as the ``working_dir`` or a ``py_module``.
A remote URI must be a link directly to a zip file. **The zip file must contain only a single top-level directory.**
The contents of this directory will be directly accessed as the ``working_dir`` or a ``py_module``.

For example, suppose you want to use the contents in your local ``/some_path/example_dir`` directory as your ``working_dir``.
If you want to specify this directory as a local path, your ``runtime_env`` dictionary should contain:

.. code-block:: python

    runtime_env = {..., "working_dir": "/some_path/example_dir", ...}

Suppose instead you want to host your files in your ``/some_path/example_dir`` directory remotely and provide a remote URI.
You would need to first compress the ``example_dir`` directory into a zip file.
You can use the following command in the Terminal to do so:

.. code-block:: bash

    zip -r example.zip /some_path/example_dir

In general, to compress a directory called ``directory_to_zip`` into a zip file called ``zip_file_name.zip``, the command is:

.. code-block:: bash

    # General command
    zip -r zip_file_name.zip directory_to_zip

There should be no other files or directories at the top level of the zip file, other than ``example_dir``.
In general, the zip file's name and the top-level directory's name can be anything.
The top-level directory's contents will be used as the ``working_dir`` (or ``py_module``).
Suppose you upload the compressed ``example_dir`` directory to AWS S3 at the S3 URI ``s3://example_bucket/example.zip``.
Your ``runtime_env`` dictionary should contain:

.. code-block:: python

    runtime_env = {..., "working_dir": "s3://example_bucket/example.zip", ...}

.. warning::

  Check for hidden files and metadata directories (e.g. ``__MACOSX/``) in zipped dependencies.
  You can inspect a zip file's contents by running the ``zipinfo -1 zip_file_name.zip`` command in the Terminal.
  Some zipping methods can cause hidden files or metadata directories to appear in the zip file at the top level.
  This will cause Ray to throw an error because the structure of the zip file is invalid since there is more than a single directory at the top level.
  You can avoid this by using the ``zip -r`` command directly on the directory you want to compress.

Currently, three types of remote URIs are supported for hosting ``working_dir`` and ``py_modules`` packages:

- ``HTTPS``: ``HTTPS`` refers to URLs that start with ``https``.
  These are particularly useful because remote Git providers (e.g. GitHub, Bitbucket, GitLab, etc.) use ``https`` URLs as download links for repository archives.
  This allows you to host your dependencies on remote Git providers, push updates to them, and specify which dependency versions (i.e. commits) your jobs should use.
  To use packages via ``HTTPS`` URIs, you must have the ``smart_open`` library (you can install it using ``pip install smart_open``).

  - Example:

    - ``runtime_env = {"working_dir": "https://github.com/example_username/example_respository/archive/HEAD.zip"}``

- ``S3``: ``S3`` refers to URIs starting with ``s3://`` that point to compressed packages stored in `AWS S3 <https://aws.amazon.com/s3/>`_.
  To use packages via ``S3`` URIs, you must have the ``smart_open`` and ``boto3`` libraries (you can install them using ``pip install smart_open`` and ``pip install boto3``).
  Ray does not explicitly pass in any credentials to ``boto3`` for authentication.
  ``boto3`` will use your environment variables, shared credentials file, and/or AWS config file to authenticate access.
  See the `AWS boto3 documentation <https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html>`_ to learn how to configure these.

  - Example:

    - ``runtime_env = {"working_dir": "s3://example_bucket/example_file.zip"}``

- ``GS``: ``GS`` refers to URIs starting with ``gs://`` that point to compressed packages stored in `Google Cloud Storage <https://cloud.google.com/storage>`_.
  To use packages via ``GS`` URIs, you must have the ``smart_open`` and ``google-cloud-storage`` libraries (you can install them using ``pip install smart_open`` and ``pip install google-cloud-storage``).
  Ray does not explicitly pass in any credentials to the ``google-cloud-storage``'s ``Client`` object.
  ``google-cloud-storage`` will use your local service account key(s) and environment variables by default.
  Follow the steps on Google Cloud Storage's `Getting started with authentication <https://cloud.google.com/docs/authentication/getting-started>`_ guide to set up your credentials, which allow Ray to access your remote package.

  - Example:

    - ``runtime_env = {"working_dir": "gs://example_bucket/example_file.zip"}``

Hosting a Dependency on a Remote Git Provider: Step-by-Step Guide
-----------------------------------------------------------------

You can store your dependencies in repositories on a remote Git provider (e.g. GitHub, Bitbucket, GitLab, etc.), and you can periodically push changes to keep them updated.
In this section, you will learn how to store a dependency on GitHub and use it in your runtime environment.

.. note::
  These steps will also be useful if you use another large, remote Git provider (e.g. BitBucket, GitLab, etc.).
  For simplicity, this section refers to GitHub alone, but you can follow along on your provider.

First, create a repository on GitHub to store your ``working_dir`` contents or your ``py_module`` dependency.
By default, when you download a zip file of your repository, the zip file will already contain a single top-level directory that holds the repository contents,
so you can directly upload your ``working_dir`` contents or your ``py_module`` dependency to the GitHub repository.

Once you have uploaded your ``working_dir`` contents or your ``py_module`` dependency, you need the HTTPS URL of the repository zip file, so you can specify it in your ``runtime_env`` dictionary.

You have two options to get the HTTPS URL.

Option 1: Download Zip (quicker to implement, but not recommended for production environments)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The first option is to use the remote Git provider's "Download Zip" feature, which provides an HTTPS link that zips and downloads your repository.
This is quick, but it is **not recommended** because it only allows you to download a zip file of a repository branch's latest commit.
To find a GitHub URL, navigate to your repository on `GitHub <https://github.com/>`_, choose a branch, and click on the green "Code" drop down button:

.. figure:: ray_repo.png
   :width: 500px

This will drop down a menu that provides three options: "Clone" which provides HTTPS/SSH links to clone the repository,
"Open with GitHub Desktop", and "Download ZIP."
Right-click on "Download Zip."
This will open a pop-up near your cursor. Select "Copy Link Address":

.. figure:: download_zip_url.png
   :width: 300px

Now your HTTPS link is copied to your clipboard. You can paste it into your ``runtime_env`` dictionary.

.. warning::

  Using the HTTPS URL from your Git provider's "Download as Zip" feature is not recommended if the URL always points to the latest commit.
  For instance, using this method on GitHub generates a link that always points to the latest commit on the chosen branch.

  By specifying this link in the ``runtime_env`` dictionary, your Ray Cluster always uses the chosen branch's latest commit.
  This creates a consistency risk: if you push an update to your remote Git repository while your cluster's nodes are pulling the repository's contents,
  some nodes may pull the version of your package just before you pushed, and some nodes may pull the version just after.
  For consistency, it is better to specify a particular commit, so all the nodes use the same package.
  See "Option 2: Manually Create URL" to create a URL pointing to a specific commit.

Option 2: Manually Create URL (slower to implement, but recommended for production environments)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The second option is to manually create this URL by pattern-matching your specific use case with one of the following examples.
**This is recommended** because it provides finer-grained control over which repository branch and commit to use when generating your dependency zip file.
These options prevent consistency issues on Ray Clusters (see the warning above for more info).
To create the URL, pick a URL template below that fits your use case, and fill in all parameters in brackets (e.g. [username], [repository], etc.) with the specific values from your repository.
For instance, suppose your GitHub username is ``example_user``, the repository's name is ``example_repository``, and the desired commit hash is ``abcdefg``.
If ``example_repository`` is public and you want to retrieve the ``abcdefg`` commit (which matches the first example use case), the URL would be:

.. code-block:: python

    runtime_env = {"working_dir": ("https://github.com"
                                   "/example_user/example_repository/archive/abcdefg.zip")}

Here is a list of different use cases and corresponding URLs:

- Example: Retrieve package from a specific commit hash on a public GitHub repository

.. code-block:: python

    runtime_env = {"working_dir": ("https://github.com"
                                   "/[username]/[repository]/archive/[commit hash].zip")}

- Example: Retrieve package from a private GitHub repository using a Personal Access Token

.. code-block:: python

    runtime_env = {"working_dir": ("https://[username]:[personal access token]@github.com"
                                   "/[username]/[private repository]/archive/[commit hash].zip")}

- Example: Retrieve package from a public GitHub repository's latest commit

.. code-block:: python

    runtime_env = {"working_dir": ("https://github.com"
                                   "/[username]/[repository]/archive/HEAD.zip")}

- Example: Retrieve package from a specific commit hash on a public Bitbucket repository

.. code-block:: python

    runtime_env = {"working_dir": ("https://bitbucket.org"
                                   "/[owner]/[repository]/get/[commit hash].tar.gz")}

.. tip::

  It is recommended to specify a particular commit instead of always using the latest commit.
  This prevents consistency issues on a multi-node Ray Cluster.
  See the warning below "Option 1: Download Zip" for more info.

Once you have specified the URL in your ``runtime_env`` dictionary, you can pass the dictionary
into a ``ray.init()`` or ``.options()`` call. Congratulations! You have now hosted a ``runtime_env`` dependency
remotely on GitHub!
