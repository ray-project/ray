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

- ``working_dir`` (str): Specifies the working directory for your job. This can be the path of an existing local directory with a total size of at most 100 MiB.
  Alternatively, it can be a URI to a remotely-stored zip file containing the working directory for your job. See the "Remote URIs" section below for more info.
  The directory will be cached on the cluster, so the next time you connect with Ray Client you will be able to skip uploading the directory contents.
  All Ray workers for your job will be started in their node's local copy of this working directory.

  - Examples

    - ``"."  # cwd``

    - ``"/code/my_project"``

    - ``"gs://bucket/file.zip" # Google Cloud Storage URI``

  Note: Setting this option per-task or per-actor is currently unsupported.

  Note: If your working directory contains a `.gitignore` file, the files and paths specified therein will not be uploaded to the cluster.

- ``py_modules`` (List[str]): Specifies a list of dependencies for your job.
  The list must contain paths to local directories, remote URIs to zip files, or a mix of both.
  See the "Remote URIs" section below for more info about using remote zip files.
  ``py_modules`` and ``working_dir`` can both be specified in the same ``runtime_env`` Python dictionary.

  - Example: ``[".", "/local_dependency/code", "s3://bucket/file.zip"]``

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
This allows for environment variables set in the parent's runtime environment to be automatically propagated to the child, even if new environment variables are set in the child's runtime environment.

Here are some examples of runtime environments combining multiple options:

..
  TODO(architkulkarni): run working_dir doc example in CI

.. code-block:: python

    runtime_env = {"working_dir": "/files/my_project", "pip": ["pendulum=2.1.2"]}

.. literalinclude:: ../examples/doc_code/runtime_env_example.py
   :language: python
   :start-after: __runtime_env_conda_def_start__
   :end-before: __runtime_env_conda_def_end__

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

Once you have uploaded your ``working_dir`` contents or your ``py_module`` dependency, you need the ``HTTPS`` URL of the repository zip file, so you can specify it in your ``runtime_env`` dictionary.

You have two options to get the HTTPS URL.
The first option is to use the remote Git provider's "Download Zip" feature, which redirects you to an HTTPS link that zips and downloads your repository.
This is quick, but it is **not recommended** because it only allows you to download a zip file of the repository's main branch at the latest commit.
To find a GitHub URL, navigate to your repository on `GitHub <github.com>`_, choose a branch, and click on the green "Code" drop down button:

.. figure:: ray_repo.png
   :width: 500px

This will drop down a menu that provides three options: "Clone" which provides HTTPS/SSH links to clone the repository, 
"Open with GitHub Desktop", and "Download ZIP."
Right click on "Download Zip."
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
  See the following recommended option (manually crafting the HTTPS URL) to create a URL pointing to a specific commit.

The second option is to craft this URL by pattern-matching your specific use case with one of the following examples.
**This is recommended** because it provides finer-grained control over which repository branch and commit to use when generating your dependency zip file.
These options prevent consistency issues on Ray Clusters (see the warning above for more info).
To create the URL, pick a URL template below that fits your use case, and fill in all parameters in brackets (e.g. [username], [repository], etc.) with the specific values from your repository.
For instance, suppose your GitHub username is ``example_user`` and the repository's name is ``example_repository``.
If ``example_repository`` is public and you want to retrieve the latest commit, the URL would be:

.. code-block:: python

    runtime_env = {"working_dir": ("https://github.com"
                                   "/example_user/example_repository/archive/HEAD.zip")}

Here is a list of different use cases and corresponding URLs:

- Example: Retrieve package from a GitHub public repository's latest commit

.. code-block:: python

    runtime_env = {"working_dir": ("https://github.com"
                                   "/[username]/[repository]/archive/HEAD.zip")}

- Example: Retrieve package from a specific commit hash on a GitHub public repository

.. code-block:: python

    runtime_env = {"working_dir": ("https://github.com"
                                   "/[username]/[repository]/archive/[commit hash].zip")}

- Example: Retrieve package from a GitHub private repository using username and password

.. code-block:: python

    runtime_env = {"working_dir": ("https://[username]:[password]@github.com"
                                   "/[username]/[private repository]/archive/[commit hash].zip")}

- Example: Retrieve package from a GitHub private repository using a Personal Access Token

.. code-block:: python

    runtime_env = {"working_dir": ("https://[username]:[personal access token]@github.com"
                                   "/[username]/[private repository]/archive/[commit hash].zip")}

- Example: Retrieve package from a specific commit hash on a Bitbucket public repository

.. code-block:: python

    runtime_env = {"working_dir": ("https://bitbucket.org"
                                   "/[owner]/[repository]/get/[commit hash].tar.gz")}

.. tip::

  It is recommended to specify a particular commit instead of always using the latest commit.
  This prevents consistency issues on a multi-node Ray Cluster.
  See the warning above for more info.

Once you have specified the URL in your ``runtime_env`` dictionary, you can pass the dictionary 
into a ``ray.init()`` or ``.options()`` call. Congratulations! You have now hosted a ``runtime_env`` dependency 
remotely on GitHub!