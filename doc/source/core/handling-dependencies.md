---
myst:
  html_meta:
    description: "Ship Python and system dependencies to a Ray cluster with runtime environments, per job or per task, using local files, conda, or pip."
---

(handling_dependencies)=

# Environment dependencies

Your Ray application might depend on things that exist outside of your Ray script. For example, your script might do any of the following:

* Import Python packages.
* Expect specific environment variables to be available.
* Import files from outside of the script.

A common problem when you run on a cluster is that Ray expects these dependencies to exist on each Ray node. If they aren't present, you might see errors such as `ModuleNotFoundError` and `FileNotFoundError`.

You can solve this problem in two ways. You can prepare your dependencies on the cluster in advance with the Ray {ref}`cluster launcher <vm-cluster-quick-start>`, for example by using a container image. Or you can use Ray's {ref}`runtime environments <runtime-environments>` to install them on the fly.

For production or for environments that don't change, install your dependencies into a container image and specify the image with the cluster launcher. For dynamic environments, such as during development and experimentation, use runtime environments.


## Concepts

- **Ray application**: A program that includes a Ray script that calls `ray.init()` and uses Ray tasks or actors.

- **Dependencies**, or **environment**: Anything outside of the Ray script that your application needs to run, including files, packages, and environment variables.

- **Files**: Code files, data files, or other files that your Ray application needs to run.

- **Packages**: External libraries or executables that your Ray application requires, often installed through `pip` or `conda`.

- **Local machine** and **cluster**: Usually, you might want to keep the Ray cluster's compute machines or pods separate from the machine or pod that handles and submits the application. You can submit a Ray job through {ref}`the Ray job submission mechanism <jobs-overview>`, or use `ray attach` to connect to a cluster interactively. The machine that submits the job is your *local machine*.

- **Job**: A {ref}`Ray job <cluster-clients-and-jobs>` is a single application. It's the collection of Ray tasks, objects, and actors that originate from the same script.

(using-the-cluster-launcher)=

## Preparing an environment using the Ray cluster launcher

The first way to set up dependencies is to prepare a single environment across the cluster before you start the Ray runtime. Use any of the following methods:

- Build all your files and dependencies into a container image, and specify the image in your {ref}`cluster YAML configuration <cluster-config>`.

- Install packages with `setup_commands` in the Ray cluster configuration file. These commands run as each node joins the cluster. For details, see the {ref}`setup commands reference <cluster-configuration-setup-commands>`. For production settings, build any necessary packages into a container image instead.

- Push local files to the cluster with `ray rsync_up`. For details, see the {ref}`rsync command reference <ray-rsync>`.

(runtime-environments)=

## Runtime environments

:::{note}
Runtime environments require a full installation of Ray with `pip install "ray[default]"`. They're available starting with Ray 1.4.0. Ray currently supports them on macOS and Linux, with beta support on Windows.
:::

The second way to set up dependencies is to install them dynamically while Ray is running.

A *runtime environment* describes the dependencies your Ray application needs to run, including {ref}`files, packages, environment variables, and more <runtime-environments-api-ref>`. Ray installs it dynamically on the cluster at runtime and caches it for future use. For details about the lifecycle, see {ref}`Caching and garbage collection <runtime-environments-caching>`.

If you prepared an environment with {ref}`the Ray cluster launcher <using-the-cluster-launcher>`, you can use runtime environments on top of it. For example, you can use the cluster launcher to install a base set of packages, then use runtime environments to install additional packages. Unlike the base cluster environment, a runtime environment is active only for Ray processes. For example, if your runtime environment specifies a `pip` package `my_pkg`, the statement `import my_pkg` fails if you call it outside of a Ray task, actor, or job.

On a long-running Ray cluster, you can also use runtime environments to set dependencies per task, per actor, and per job.

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray

runtime_env = {"pip": ["emoji"]}

ray.init(runtime_env=runtime_env)

@ray.remote
def f():
  import emoji
  return emoji.emojize('Python is :thumbs_up:')

print(ray.get(f.remote()))
```

```{testoutput}
Python is 👍
```

You can describe a runtime environment with a Python `dict`:

```{literalinclude} /core/doc_code/runtime_env_example.py
:language: python
:start-after: __runtime_env_pip_def_start__
:end-before: __runtime_env_pip_def_end__
```

Alternatively, you can use {class}`ray.runtime_env.RuntimeEnv <ray.runtime_env.RuntimeEnv>`:

```{literalinclude} /core/doc_code/runtime_env_example.py
:language: python
:start-after: __strong_typed_api_runtime_env_pip_def_start__
:end-before: __strong_typed_api_runtime_env_pip_def_end__
```

For more examples, see the {ref}`API reference <runtime-environments-api-ref>`.


You can specify a runtime environment at two primary scopes:

* {ref}`Per job <rte-per-job>`
* {ref}`Per task or actor, within a job <rte-per-task-actor>`

(rte-per-job)=

### Specifying a runtime environment per job

You can specify a runtime environment for your whole job, whether you run a script directly on the cluster, use the {ref}`Ray Jobs API <jobs-overview>`, or submit a {ref}`KubeRay RayJob <kuberay-rayjob-quickstart>`:

```{literalinclude} /core/doc_code/runtime_env_example.py
:language: python
:start-after: __ray_init_start__
:end-before: __ray_init_end__
```

```{testcode}
:skipif: True

# Option 2: Using Ray Jobs API (Python SDK)
from ray.job_submission import JobSubmissionClient

client = JobSubmissionClient("http://<head-node-ip>:8265")
job_id = client.submit_job(
    entrypoint="python my_ray_script.py",
    runtime_env=runtime_env,
)
```

```bash
# Option 3: Using Ray Jobs API (CLI). (Note: can use --runtime-env to pass a YAML file instead of an inline JSON string.)
$ ray job submit --address="http://<head-node-ip>:8265" --runtime-env-json='{"working_dir": "/data/my_files", "pip": ["emoji"]}' -- python my_ray_script.py
```

```yaml
# Option 4: Using KubeRay RayJob. You can specify the runtime environment in the RayJob YAML manifest.
# [...]
spec:
  runtimeEnvYAML: |
    pip:
      - requests==2.26.0
      - pendulum==2.1.2
    env_vars:
      KEY: "VALUE"
```

:::{warning}
If you specify the `runtime_env` argument in the `submit_job` or `ray job submit` call, Ray installs the runtime environment on the cluster before it runs the entrypoint script.

If you specify `runtime_env` in `ray.init(runtime_env=...)`, Ray applies the runtime environment only to child tasks and actors. It doesn't apply to the entrypoint script itself, which is the driver.

If you specify `runtime_env` in both `ray job submit` and `ray.init`, Ray merges the runtime environments. For details, see {ref}`Runtime environment specified by both job and driver <runtime-environments-job-conflict>`.
:::

:::{note}
Ray installs the runtime environment at one of two points:

1. As soon as the job starts, which is when you call `ray.init()`. Ray eagerly downloads and installs the dependencies.
1. Only when you invoke a task or create an actor.

The default is option 1. To switch to option 2, add `"eager_install": False` to the `config` of `runtime_env`.
:::

(rte-per-task-actor)=

### Specifying a runtime environment per task or per actor

To specify different runtime environments per actor or per task, use `.options()` or the `@ray.remote` decorator:

```{literalinclude} /core/doc_code/runtime_env_example.py
:language: python
:start-after: __per_task_per_actor_start__
:end-before: __per_task_per_actor_end__
```

With this approach, each actor and task runs in its own environment, independent of the surrounding environment. The surrounding environment can be the job's runtime environment or the system environment of the cluster.

:::{warning}
Ray doesn't guarantee compatibility between tasks and actors with conflicting runtime environments. For example, if an actor whose runtime environment contains a `pip` package tries to communicate with an actor that has a different version of that package, unexpected behavior such as unpickling errors can result.
:::

### Common workflows

This section describes common use cases for runtime environments. These use cases aren't mutually exclusive. You can combine all of the following options in a single runtime environment.

(workflow-local-files)=

#### Using local files

Your Ray application might depend on source files or data files. During development, these files might live on your local machine, but to run at scale, you need to get them to your remote cluster.

The following example shows how to get your local files onto the cluster.

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import os
import ray

os.makedirs("/tmp/runtime_env_working_dir", exist_ok=True)
with open("/tmp/runtime_env_working_dir/hello.txt", "w") as hello_file:
  hello_file.write("Hello World!")

# Specify a runtime environment for the entire Ray job
ray.init(runtime_env={"working_dir": "/tmp/runtime_env_working_dir"})

# Create a Ray task, which inherits the above runtime env.
@ray.remote
def f():
    # The function will have its working directory changed to its node's
    # local copy of /tmp/runtime_env_working_dir.
    return open("hello.txt").read()

print(ray.get(f.remote()))
```

```{testoutput}
Hello World!
```

:::{note}
The preceding example runs on a local machine. As with all of these examples, it also works when you specify a Ray cluster to connect to, for example with `ray.init("ray://123.456.7.89:10001", runtime_env=...)` or `ray.init(address="auto", runtime_env=...)`.
:::

When you call `ray.init()`, Ray automatically pushes the specified local directory to the cluster nodes.

You can also specify files through a remote cloud storage URI. For details, see {ref}`remote-uris`.

If you specify a `working_dir`, Ray always prepares it first and makes it available to the creation of other runtime environments through the `${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}` environment variable. Because of this sequencing, `pip` and `conda` can reference local files in the `working_dir`, such as `requirements.txt` or `environment.yml`. For details, see the `pip` and `conda` sections in {ref}`runtime-environments-api-ref`.

#### Using `conda` or `pip` packages

Your Ray application might import Python packages such as `pendulum` or `requests`.

Ray ordinarily expects all imported packages to be preinstalled on every node of the cluster. Ray doesn't automatically ship these packages from your local machine to the cluster or download them from any repository.

With runtime environments, however, you can dynamically specify packages for Ray to download and install automatically in a virtual environment for your Ray job, or for specific Ray tasks or actors.

```{testcode}
:hide:

import ray
ray.shutdown()
```

```{testcode}
import ray
import requests

# This example runs on a local machine, but you can also do
# ray.init(address=..., runtime_env=...) to connect to a cluster.
ray.init(runtime_env={"pip": ["requests"]})

@ray.remote
def reqs():
    return requests.get("https://www.ray.io/").status_code

print(ray.get(reqs.remote()))
```

```{testoutput}
200
```


You can specify your `pip` dependencies either as a Python list or as a local `requirements.txt` file. Use a `requirements.txt` file when your `pip install` command requires options such as `--extra-index-url` or `--find-links`. For details, see the [pip requirements file format](https://pip.pypa.io/en/stable/reference/requirements-file-format/#). Alternatively, you can specify a `conda` environment, either as a Python dictionary or as a local `environment.yml` file. This `conda` environment can include `pip` packages. For details, see the {ref}`API reference <runtime-environments-api-ref>`.

:::{warning}
Ray installs the packages in the `runtime_env` at runtime, so be cautious when you specify `conda` or `pip` packages whose installation involves building from source, because building from source can be slow.
:::

:::{note}
When you use the `"pip"` field, Ray installs the specified packages on top of the base environment with `virtualenv`, so existing packages on your cluster remain importable. By contrast, when you use the `conda` field, your Ray tasks and actors run in an isolated environment. You can't use both the `conda` and `pip` fields in a single `runtime_env`.
:::

:::{note}
Ray automatically installs the `ray[default]` package itself in the environment. For the `conda` field only, if you use any other Ray libraries, such as Ray Serve, specify the library in the runtime environment, as in `runtime_env = {"conda": {"dependencies": ["pytorch", "pip", {"pip": ["requests", "ray[serve]"]}]}}`.
:::

:::{note}
`conda` environments must have the same Python version as the Ray cluster. Don't list `ray` in the `conda` dependencies, because Ray installs it automatically.
:::

(use-uv-for-package-management)=

#### Using `uv` for package management

The recommended way to manage packages with `uv` in runtime environments is `uv run`.

`uv run` keeps dependencies synchronized between your driver and Ray workers, and it fully supports `pyproject.toml`, including editable packages. You can also lock package versions with `uv lock`. For more details, see the [uv scripts documentation](https://docs.astral.sh/uv/guides/scripts/) and the [Anyscale blog post on uv and Ray](https://www.anyscale.com/blog/uv-ray-pain-free-python-dependencies-in-clusters).

Create a `pyproject.toml` file in your working directory with contents such as the following:

```toml
[project]

name = "test"

version = "0.1"

dependencies = [
  "emoji",
  "ray",
]
```


Then create a `test.py` file such as the following:

```{testcode}
:skipif: True

import emoji
import ray

@ray.remote
def f():
    return emoji.emojize('Python is :thumbs_up:')

# Execute 1000 copies of f across a cluster.
print(ray.get([f.remote() for _ in range(1000)]))
```


Run the driver script with `uv run test.py`. This command runs 1000 copies of the `f` function across Python worker processes in a Ray cluster. The `emoji` dependency is available to the main script and to all worker processes. The source code in the current working directory is also available to all the workers.

This workflow also supports editable packages. For example, run `uv add --editable ./path/to/package`. The `./path/to/package` directory must be inside your current working directory so that it's available to all workers.

For an end-to-end example that uses `uv run` to run a batch inference workload with Ray Data, see the [end-to-end example in the uv and Ray blog post](https://www.anyscale.com/blog/uv-ray-pain-free-python-dependencies-in-clusters#end-to-end-example-for-using-uv).

To use uv in a Ray job, keep the same `pyproject.toml` and `test.py` files as in the preceding example, and submit a Ray job with the following command:

```sh
ray job submit --working-dir . -- uv run test.py
```

This command runs both the driver and the workers of the job in the uv environment that your `pyproject.toml` specifies.

To use uv with Ray Serve, create appropriate `pyproject.toml` and `app.py` files, then run the Ray Serve application with `uv run serve run app:main`.

Keep the following best practices and tips in mind when you use uv:

- If you run on a Ray cluster, the Ray and Python versions of your uv environment must match the Ray and Python versions of your cluster. Otherwise, you get a version mismatch exception. To solve this, do one of the following:

  1. If you use ephemeral Ray clusters, run the application on a cluster with the right versions.
  1. If you need to run on a cluster with different versions, change the versions of your uv environment. Update the `pyproject.toml` file, or use the `--active` flag with `uv run`, as in `uv run --active main.py`.

- Use `uv lock` to generate a lockfile that freezes all your dependencies, so they don't change in uncontrolled ways when a new version of a package comes out.

- If you have a `requirements.txt` file, run `uv add -r requirement.txt` to add its dependencies to your `pyproject.toml`, then use that file with `uv run`.

- If your `pyproject.toml` is in a subdirectory, use `uv run --project` to use it from there.

- To use `uv run` with a working directory other than the current one, use the `--directory` flag. The Ray uv integration sets your `working_dir` accordingly.

Ray implements `uv run` support with a low-level runtime environment plugin called `py_executable`. This plugin specifies the Python executable, including its arguments, that Ray starts workers in. For uv, Ray sets `py_executable` to `uv run` with the same parameters that you used to run the driver. Ray also uses the `working_dir` runtime environment to propagate the driver's working directory, including the `pyproject.toml`, to the workers. As a result, uv can set up the right dependencies and environment for the workers to run in. In some advanced use cases, you might want to use the `py_executable` mechanism directly in your programs:

- **Applications with heterogeneous dependencies**: Ray supports a different runtime environment for each task or actor. This is useful for deploying different inference engines, models, or microservices in different [Ray Serve deployments](https://docs.ray.io/en/latest/serve/production-guide/handling-dependencies.html#dependencies-per-deployment), and for heterogeneous data pipelines in Ray Data. To implement this, specify a different `py_executable` for each runtime environment, and use `uv run` with a different `--project` parameter for each. Alternatively, use a different `working_dir` for each environment.

- **Customizing the command the worker runs in**: You might want to pass uv special arguments on the workers that the driver doesn't use. Or you might want to run processes with `poetry run`, a build system such as Bazel, a profiler, or a debugger. In these cases, specify the executable the worker runs in through `py_executable`. The executable can even be a shell script stored in `working_dir`, if you want to wrap multiple processes in more complex ways.

:::{note}
All child tasks and actors inherit the uv environment. To mix environments, for example `pip` runtime environments with `uv run`, set the Python executable back to one that doesn't run in the isolated uv environment, as in the following example:

```toml
[project]

name = "test"

version = "0.1"

dependencies = [
  "emoji",
  "ray",
  "pip",
  "virtualenv",
]
```

```{testcode}
:skipif: True

import ray

@ray.remote(runtime_env={"pip": ["wikipedia"], "py_executable": "python"})
def f():
    import wikipedia
    return wikipedia.summary("Wikipedia")

@ray.remote
def g():
    import emoji
    return emoji.emojize('Python is :thumbs_up:')

print(ray.get(f.remote()))
print(ray.get(g.remote()))
```


The preceding pattern can help support legacy applications, but the Ray team recommends using uv to track nested environments as well. To do so, create a separate `pyproject.toml` that contains the dependencies of the nested environment.
:::


#### Library development

Suppose you're developing a library `my_module` on Ray.

A typical iteration cycle involves the following steps:

1. Make changes to the source code of `my_module`.
1. Run a Ray script to test the changes, perhaps on a distributed cluster.

To make sure your local changes show up across all Ray workers and import correctly, use the `py_modules` field.

```{testcode}
:skipif: True

import ray
import my_module

ray.init("ray://123.456.7.89:10001", runtime_env={"py_modules": [my_module]})

@ray.remote
def test_my_module():
    # No need to import my_module inside this function.
    my_module.test()

ray.get(test_my_module.remote())
```

(runtime-environments-api-ref)=

### API reference

The `runtime_env` is a Python dictionary or a {class}`ray.runtime_env.RuntimeEnv <ray.runtime_env.RuntimeEnv>` object that includes one or more of the following fields:

- `working_dir` (str): Specifies the working directory for the Ray workers. The value must be one of the following:

  1. A local existing directory with a total size of at most 500 MiB.
  1. A local existing archive file in `.zip`, `.tar.gz`, `.tgz`, or `.tar.xz` format, with a total uncompressed size of at most 500 MiB. For an archive, `excludes` has no effect.
  1. A URI to a remotely stored archive in `.zip`, `.tar.gz`, `.tgz`, or `.tar.xz` format that contains the working directory for your job. Ray enforces no file size limit for this case. For details, see {ref}`remote-uris`.
  1. A `local://` URI naming a directory that already exists on every node, such as one baked into your container image.

  In cases 1 through 3, Ray downloads the specified directory to each node on the cluster, and Ray workers start in their node's copy of this directory. In case 4, Ray uploads and downloads nothing, and the workers start directly in that directory. See {ref}`in-image-working-dir`.

  - Examples:

    - `"."  # cwd`

    - `"/src/my_project"`

    - `"local:///app"`

    - `"/src/my_project.zip"`

    - `"s3://path/to/my_dir.zip"`

    - `"s3://path/to/my_dir.tar.gz"`

  :::{note}
  Setting a local directory per task or per actor is currently unsupported. You can set it only per job, in `ray.init()`.

  By default, if the local directory contains a `.gitignore` file, a `.rayignore` file, or both, Ray doesn't upload the files they specify to the cluster. To stop Ray from considering the `.gitignore` file, set `RAY_RUNTIME_ENV_IGNORE_GITIGNORE=1` on the machine that does the uploading.

  By default, Ray automatically excludes the common directories `.git`, `.venv`, `venv`, and `__pycache__` from the `working_dir` upload. To override these defaults, set the `RAY_OVERRIDE_RUNTIME_ENV_DEFAULT_EXCLUDES` environment variable to a comma-separated list of patterns. To disable default excludes entirely, set it to an empty string.

  If the local directory contains symbolic links, Ray follows the links and uploads the files they point to.
  :::

- `py_modules` (List[str|module]): Specifies Python modules to make available for import in the Ray workers. For more ways to specify packages, see the `pip` and `conda` fields that follow. Each entry must be one of the following:

  1. A path to a local file or directory.
  1. A URI to a remote archive in `.zip`, `.tar.gz`, `.tgz`, or `.tar.xz` format, or to a remote wheel file in `.whl` format. For details, see {ref}`remote-uris`.
  1. A Python module object.
  1. A path to a local `.whl` file.
  1. A `local://` URI naming a directory that already exists on every node. See {ref}`in-image-working-dir`.

  - Examples of entries in the list:

    - `"."`

    - `"/local_dependency/my_dir_module"`

    - `"/local_dependency/my_file_module.py"`

    - `"s3://bucket/my_module.zip"`

    - `my_module # Assumes my_module has already been imported, e.g. via 'import my_module'`

    - `my_module.whl`

    - `"s3://bucket/my_module.whl"`

  Ray downloads the modules to each node on the cluster.

  :::{note}
  Setting options 1, 3, and 4 per task or per actor is currently unsupported. You can set them only per job, in `ray.init()`.

  For option 1, by default, if the local directory contains a `.gitignore` file, a `.rayignore` file, or both, Ray doesn't upload the files they specify to the cluster. To stop Ray from considering the `.gitignore` file, set `RAY_RUNTIME_ENV_IGNORE_GITIGNORE=1` on the machine that does the uploading.
  :::

- `py_executable` (str): Specifies the executable that runs the Ray workers. It can include arguments. The executable can be in the `working_dir`. Use this field to run workers in a custom debugger or profiler, or in an environment that a package manager such as `uv` sets up. See {ref}`Using uv for package management <use-uv-for-package-management>`.

  :::{note}
  `py_executable` is currently experimental. If you have requirements or run into problems, open an issue on [GitHub](https://github.com/ray-project/ray/issues).
  :::

- `excludes` (List[str]): When used with `working_dir` or `py_modules`, specifies a list of files or paths to exclude from the upload to the cluster. This field uses the pattern-matching syntax of `.gitignore` files. For details, see the [gitignore documentation](https://git-scm.com/docs/gitignore). As in `.gitignore` syntax, if a pattern has a `/` separator at its beginning, its middle, or both, Ray interprets the pattern relative to the level of the `working_dir`. In particular, don't use absolute paths such as `/Users/my_working_dir/subdir/` with `excludes`. Use the relative path `/subdir/` instead. The leading `/` matches only the top-level `subdir` directory, rather than all directories named `subdir` at all levels.

  - Example: `{"working_dir": "/Users/my_working_dir/", "excludes": ["my_file.txt", "/subdir/", "path/to/dir", "*.log"]}`

- `pip` (dict | List[str] | str): The value is one of the following:

  1. A list of pip [requirements specifiers](https://pip.pypa.io/en/stable/cli/pip_install/#requirement-specifiers).
  1. A string containing the path to a local pip ["requirements.txt"](https://pip.pypa.io/en/stable/user_guide/#requirements-files) file.
  1. A Python dictionary with the following fields:
     - `packages` (required, List[str]): A list of pip packages.
     - `pip_check` (optional, bool): Whether to enable [pip check](https://pip.pypa.io/en/stable/cli/pip_check/) at the end of pip install. Defaults to `False`.
     - `pip_version` (optional, str): The version of pip. Ray adds the package name "pip" in front of the `pip_version` to form the final requirement string.
     - `pip_install_options` (optional, List[str]): Options that you provide for the `pip install` command. Defaults to `["--disable-pip-version-check", "--no-cache-dir"]`.

  [Python Enhancement Proposal (PEP) 508](https://www.python.org/dev/peps/pep-0508/) defines the full syntax of a requirement specifier. Ray installs these packages in the Ray workers at runtime. Packages in the preinstalled cluster environment remain available. To use a library such as Ray Serve or Ray Tune, include `"ray[serve]"` or `"ray[tune]"` here. The Ray version must match the cluster's Ray version.

  - Example: `["requests==1.0.0", "aiohttp", "ray[serve]"]`

  - Example: `"./requirements.txt"`

  - Example: `{"packages":["tensorflow", "requests"], "pip_check": False, "pip_version": "==22.0.2;python_version=='3.8.11'"}`

  When you specify a path to a `requirements.txt` file, the file must be present on your local machine. The path must be a valid absolute path, or a path relative to your local current working directory, *not* relative to the `working_dir` specified in the `runtime_env`. Ray doesn't directly support referencing local files *within* a `requirements.txt` file, such as `-r ./my-laptop/more-requirements.txt` or `./my-pkg.whl`. Instead, use the `${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}` environment variable in the creation process. For example, to reference local files, use `-r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/my-laptop/more-requirements.txt` or `${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/my-pkg.whl`, and make sure the files are in the `working_dir`.

- `uv` (dict | List[str] | str): An alpha feature. This plugin is the `uv pip` version of the preceding `pip` plugin. For `uv run` support with `pyproject.toml` and `uv.lock`, use {ref}`the uv run runtime environment plugin <use-uv-for-package-management>` instead.

  The value is one of the following:

  1. A list of uv [requirements specifiers](https://pip.pypa.io/en/stable/cli/pip_install/#requirement-specifiers).
  1. A string containing the path to a local uv ["requirements.txt"](https://pip.pypa.io/en/stable/user_guide/#requirements-files) file.
  1. A Python dictionary with the following fields:
     - `packages` (required, List[str]): A list of uv packages.
     - `uv_version` (optional, str): The version of uv. Ray adds the package name "uv" in front of the `uv_version` to form the final requirement string.
     - `uv_check` (optional, bool): Whether to enable pip check at the end of uv install. Defaults to `False`.
     - `uv_pip_install_options` (optional, List[str]): Options that you provide for the `uv pip install` command. Defaults to `["--no-cache"]`. To override the default options and install without any options, use an empty list `[]` as the install option value.

  The syntax of a requirement specifier is the same as for `pip` requirements. Ray installs these packages in the Ray workers at runtime. Packages in the preinstalled cluster environment remain available. To use a library such as Ray Serve or Ray Tune, include `"ray[serve]"` or `"ray[tune]"` here. The Ray version must match the cluster's Ray version.

  - Example: `["requests==1.0.0", "aiohttp", "ray[serve]"]`

  - Example: `"./requirements.txt"`

  - Example: `{"packages":["tensorflow", "requests"], "uv_version": "==0.4.0;python_version=='3.8.11'"}`

  When you specify a path to a `requirements.txt` file, the file must be present on your local machine. The path must be a valid absolute path, or a path relative to your local current working directory, *not* relative to the `working_dir` specified in the `runtime_env`. Ray doesn't directly support referencing local files *within* a `requirements.txt` file, such as `-r ./my-laptop/more-requirements.txt` or `./my-pkg.whl`. Instead, use the `${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}` environment variable in the creation process. For example, to reference local files, use `-r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/my-laptop/more-requirements.txt` or `${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/my-pkg.whl`, and make sure the files are in the `working_dir`.

- `conda` (dict | str): The value is one of the following:

  1. A dict representing the conda environment YAML.
  1. A string containing the path to a local [conda "environment.yml"](https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-environments.html#create-env-file-manually) file.
  1. The name of a local conda environment already installed on each node in your cluster, such as `"pytorch_p36"`, or its absolute path, such as `"/home/youruser/anaconda3/envs/pytorch_p36"`.

  In the first two cases, Ray automatically injects the Ray and Python dependencies into the environment to ensure compatibility, so you don't need to include them manually. The Python and Ray versions must match the cluster's versions, so you likely shouldn't specify them manually. You can't specify both the `conda` and `pip` keys of `runtime_env` at the same time. To use them together, use `conda` and add your pip dependencies in the `"pip"` field in your conda `environment.yaml`.

  - Example: `{"dependencies": ["pytorch", "torchvision", "pip", {"pip": ["pendulum"]}]}`

  - Example: `"./environment.yml"`

  - Example: `"pytorch_p36"`

  - Example: `"/home/youruser/anaconda3/envs/pytorch_p36"`

  When you specify a path to an `environment.yml` file, the file must be present on your local machine. The path must be a valid absolute path, or a path relative to your local current working directory, *not* relative to the `working_dir` specified in the `runtime_env`. Ray doesn't directly support referencing local files *within* an `environment.yml` file, such as `-r ./my-laptop/more-requirements.txt` or `./my-pkg.whl`. Instead, use the `${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}` environment variable in the creation process. For example, to reference local files, use `-r ${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/my-laptop/more-requirements.txt` or `${RAY_RUNTIME_ENV_CREATE_WORKING_DIR}/my-pkg.whl`, and make sure the files are in the `working_dir`.

- `env_vars` (Dict[str, str]): Environment variables to set. Environment variables already set on the cluster remain visible to the Ray workers, so you don't need to include `os.environ` or similar in the `env_vars` field. By default, these environment variables override environment variables of the same name on the cluster. To append to an existing environment variable, reference it with `${ENV_VAR}`. If the referenced environment variable doesn't exist, it becomes an empty string `""`.

  - Example: `{"OMP_NUM_THREADS": "32", "TF_WARNINGS": "none"}`

  - Example: `{"LD_LIBRARY_PATH": "${LD_LIBRARY_PATH}:/home/admin/my_lib"}`

  - Example with a variable that doesn't exist: `{"ENV_VAR_NOT_EXIST": "${ENV_VAR_NOT_EXIST}:/home/admin/my_lib"}` results in `ENV_VAR_NOT_EXIST=":/home/admin/my_lib"`.

  `env_vars` is a common way to pass credentials to a job, so Ray redacts these values out of the runtime environments it serves to browsers, including the Ray dashboard. Non-browser clients such as `ray list runtime-envs` and the Python SDK still receive the plaintext values. See {ref}`Runtime environment redaction <runtime-env-redaction>` to change this behavior.

- `nsight` (Union[str, Dict[str, str]]): Specifies the configuration for the Nsight System Profiler. The value is either `"default"`, which refers to the [default configuration](https://github.com/ray-project/ray/blob/master/python/ray/_private/runtime_env/nsight.py#L20), or a dict of Nsight System Profiler options and their values. For details on setup and usage, see {ref}`Nsight System Profiler <profiling-nsight-profiler>`.

  - Example: `"default"`

  - Example: `{"stop-on-exit": "true", "t": "cuda,cublas,cudnn", "ftrace": ""}`

- `image_uri` (str): Requires a given Docker image. The worker process runs in a container with this image.

  - Example: `{"image_uri": "anyscale/ray:2.53.0-py310-cpu"}`

  :::{note}
  `image_uri` is experimental. If you have requirements or run into problems, open an issue on [GitHub](https://github.com/ray-project/ray/issues).
  :::

  **Podman prerequisites on VM workers**

  Ray invokes `podman` to start the worker container. On every VM worker, do the following:

  - Install Podman and make sure the `podman` command is on the `PATH` of the Ray process, including the raylet and runtime environment subprocesses.
  - Configure rootful Podman. Ray requires this mode because it uses the host network, PID, and IPC namespaces for worker containers.
  - If Ray runs as a non-root user, configure a `podman` wrapper that invokes rootful Podman with narrowly scoped `sudo` permission and removes the rootless-only `--userns=keep-id` argument. The wrapper must preserve all other arguments exactly.
  - Allow outbound network access from the worker so Podman can pull the requested image.

- `config` (dict | {class}`ray.runtime_env.RuntimeEnvConfig <ray.runtime_env.RuntimeEnvConfig>`): Configuration for the runtime environment, as either a dict or a `RuntimeEnvConfig`. It supports the following fields:

  1. `setup_timeout_seconds`: The timeout for runtime environment creation, in seconds.

     - Example: `{"setup_timeout_seconds": 10}`

     - Example: `RuntimeEnvConfig(setup_timeout_seconds=10)`

  1. `eager_install` (bool): Whether to install the runtime environment on the cluster at `ray.init()` time, before the workers are leased. Defaults to `True`. If `False`, Ray installs the runtime environment only when you invoke the first task or create the first actor. Currently, you can't specify this option per actor or per task.

     - Example: `{"eager_install": False}`

     - Example: `RuntimeEnvConfig(eager_install=False)`

(runtime-environments-caching)=

#### Caching and garbage collection
Ray caches runtime environment resources on each node, such as conda environments, pip packages, and downloaded `working_dir` or `py_modules` directories, so that different runtime environments within a job can reuse them quickly. Each field, such as `working_dir` or `py_modules`, has its own cache with a default size of 10 GB. To change this default, set the environment variable `RAY_RUNTIME_ENV_<field>_CACHE_SIZE_GB` on each node in your cluster before you start Ray. For example, `export RAY_RUNTIME_ENV_WORKING_DIR_CACHE_SIZE_GB=1.5`.

When a cache exceeds its size limit, Ray deletes resources that no actor, task, or job is using.

(runtime-environments-job-conflict)=

#### Runtime environment specified by both job and driver

When you run an entrypoint script, which is the driver, you can specify the runtime environment through `ray.init(runtime_env=...)` or `ray job submit --runtime-env`. For details, see {ref}`Specifying a runtime environment per job <rte-per-job>`.

- If you specify the runtime environment with `ray job submit --runtime-env=...`, Ray applies it to the driver and to all the tasks and actors created from it.
- If you specify the runtime environment with `ray.init(runtime_env=...)`, Ray applies it to all the tasks and actors, but not to the driver itself.

Because `ray job submit` submits a driver that calls `ray.init`, sometimes both of them specify a runtime environment. When both the Ray job and the driver specify runtime environments, Ray merges them if they don't conflict. The driver script uses the runtime environment that `ray job submit` specifies, and all the tasks and actors use the merged runtime environment. If the runtime environments conflict, Ray raises an exception.

* Ray merges the `runtime_env["env_vars"]` of `ray job submit --runtime-env=...` with the `runtime_env["env_vars"]` of `ray.init(runtime_env=...)`. Ray merges each individual environment variable key. If the environment variables conflict, Ray raises an exception.
* Ray merges every other field in the `runtime_env`. If any key conflicts, Ray raises an exception.

The following example shows a merge without conflicts:

```{testcode}
# `ray job submit --runtime_env=...`
{"pip": ["requests", "chess"],
"env_vars": {"A": "a", "B": "b"}}

# ray.init(runtime_env=...)
{"env_vars": {"C": "c"}}

# Driver's actual `runtime_env` (merged with Job's)
{"pip": ["requests", "chess"],
"env_vars": {"A": "a", "B": "b", "C": "c"}}
```

The following examples show conflicts:

```{testcode}
# Example 1, env_vars conflicts
# `ray job submit --runtime_env=...`
{"pip": ["requests", "chess"],
"env_vars": {"C": "a", "B": "b"}}

# ray.init(runtime_env=...)
{"env_vars": {"C": "c"}}

# Ray raises an exception because the "C" env var conflicts.

# Example 2, other field (e.g., pip) conflicts
# `ray job submit --runtime_env=...`
{"pip": ["requests", "chess"]}

# ray.init(runtime_env=...)
{"pip": ["torch"]}

# Ray raises an exception because "pip" conflicts.
```

To avoid an exception on a conflict, set the environment variable `RAY_OVERRIDE_JOB_RUNTIME_ENV=1`. In this case, the runtime environments follow the same {ref}`inheritance rules <runtime-environments-inheritance>` as a parent and a child, where `ray job submit` is the parent and `ray.init` is the child.

(runtime-environments-inheritance)=

#### Inheritance

(runtime-env-driver-to-task-inheritance)=

The runtime environment is inheritable. Once set, it applies to all tasks and actors within a job, and to all child tasks and actors of a task or actor, unless overridden.

The parent's `runtime_env` is the `runtime_env` of the parent actor or task, or the job's `runtime_env` if the actor or task doesn't have a parent. If an actor or task specifies a new `runtime_env`, it overrides the parent's `runtime_env` as follows:

* Ray merges the `runtime_env["env_vars"]` field with the `runtime_env["env_vars"]` field of the parent. As a result, environment variables set in the parent's runtime environment propagate automatically to the child, even if the child's runtime environment sets new environment variables.
* The child *overrides* every other field in the `runtime_env`, rather than merging it. For example, if the child specifies `runtime_env["py_modules"]`, it replaces the `runtime_env["py_modules"]` field of the parent.

The following example shows a child's actual `runtime_env` after inheritance:

```{testcode}
# Parent's `runtime_env`
{"pip": ["requests", "chess"],
"env_vars": {"A": "a", "B": "b"}}

# Child's specified `runtime_env`
{"pip": ["torch", "ray[serve]"],
"env_vars": {"B": "new", "C": "c"}}

# Child's actual `runtime_env` (merged with parent's)
{"pip": ["torch", "ray[serve]"],
"env_vars": {"A": "a", "B": "new", "C": "c"}}
```

(runtime-env-faq)=

### Frequently asked questions

#### Are environments installed on every node?

If you specify a runtime environment in `ray.init(runtime_env=...)`, Ray installs the environment on every node. For details, see {ref}`Per job <rte-per-job>`. By default, Ray installs the runtime environment eagerly on every node in the cluster. To install the runtime environment lazily on demand, set the `eager_install` option to `False`, as in `ray.init(runtime_env={..., "config": {"eager_install": False}}`.

(when-is-the-environment-installed)=

#### When does Ray install the environment?

For a per-job environment, Ray installs the environment when you call `ray.init()`, unless you set `"eager_install": False`. For a per-task or per-actor environment, Ray installs the environment when you invoke the task or instantiate the actor, which happens when you call `my_task.remote()` or `my_actor.remote()`. For details, see {ref}`Per job <rte-per-job>` and {ref}`Per task or actor, within a job <rte-per-task-actor>`.

#### Where are the environments cached?

Ray caches any local files that the environments download at `/tmp/ray/session_latest/runtime_resources`.

#### How long does it take to install or to load from cache?

Install time usually consists mostly of the time it takes to run `pip install`, `conda create`, or `conda activate`, or to upload or download a `working_dir`, depending on which `runtime_env` options you use. Installation can take seconds or minutes.

Loading a runtime environment from the cache should be nearly as fast as ordinary Ray worker startup, which takes on the order of a few seconds. Ray starts a new worker for every Ray actor or task that requires a new runtime environment. Loading a cached `conda` environment might still be slow, because the `conda activate` command sometimes takes a few seconds.

To keep the installation from hanging for a long time, set the `setup_timeout_seconds` configuration. If the installation doesn't finish within this time, your tasks or actors fail to start.

(what-is-the-relationship-between-runtime-environments-and-docker)=

#### What's the relationship between runtime environments and Docker?

You can use them independently or together. For large or static dependencies, specify a container image in the {ref}`cluster launcher <vm-cluster-quick-start>`. For more dynamic use cases, specify runtime environments per job or per task or actor. The runtime environment inherits packages, files, and environment variables from the container image.

(my-runtime-env-was-installed-but-when-i-log-into-the-node-i-can-t-import-the-packages)=

#### Why can't you import `runtime_env` packages when you log in to a node?

The runtime environment is active only for the Ray worker processes. It doesn't install any packages globally on the node.

(in-image-working-dir)=

## Local URIs

Your code might already be on every node. It might be baked into a container image, laid down by a node setup script, or stored on a shared filesystem. In that case, point `working_dir` or `py_modules` at it in place with a `local://` URI instead of uploading a copy:

```python
runtime_env = {"working_dir": "local:///app"}
runtime_env = {"py_modules": ["local:///app/lib"]}
```

The path must be absolute, as in `local:///app`, not `local://app`. On Windows, put the drive letter in that same position, as in `local://C:/app`.

Ray uses the directory in place. It doesn't package, upload, download, or unpack anything. The directory never counts against Ray's URI cache, and Ray never evicts or deletes it.

For `working_dir`, workers start in the directory and it's first on their `PYTHONPATH`, exactly as with a downloaded `working_dir`.

For `py_modules`, a `local://` entry adds that directory to `PYTHONPATH`, so the modules inside it are importable at the top level. For example, given the file `/app/lib/foo.py`, the following entry makes `foo` importable:

```python
runtime_env = {"py_modules": ["local:///app/lib"]}  # import foo
```

This differs from passing a local directory path such as `"/app/lib"`, which uploads the directory and makes the directory itself importable as a package with `import lib`.

:::{warning}
Ray can't tell when the contents of a `local://` directory change. When Ray uploads a `working_dir`, it hashes the contents, so editing a file produces a different URI. A `local://` URI names a path rather than a snapshot, so it resolves to whatever is on disk when each worker starts. Two nodes can therefore run different code under the same URI, as can one node over time. Keep the contents identical on every node, and treat any change to them as a new deployment.
:::

Keep three more things in mind when you use a `local://` URI:

- The directory must exist on every node that runs your tasks or actors. If it doesn't, runtime environment setup fails with an error naming the missing path.
- `excludes` has no effect, because nothing is packaged.
- A `local://` URI must name a directory. Ray rejects archives such as `.zip`, `.whl`, `.tar.gz`, `.tgz`, or `.tar.xz` when it validates the runtime environment, because it never unpacks them.

(remote-uris)=

## Remote URIs

The `working_dir` and `py_modules` arguments in the `runtime_env` dictionary can specify either local paths or remote URIs.

A local path can be a directory, or a file where the API reference allows one, such as a local archive for `working_dir` or a `.whl` file for `py_modules`. Ray accesses a directory's contents directly as the `working_dir` or a `py_module`. A remote URI must link directly to a `.zip`, `.tar.gz`, `.tgz`, or `.tar.xz` archive or, for `py_module` only, a wheel file. The archive must contain only a single top-level directory. Ray accesses the contents of this directory directly as the `working_dir` or a `py_module`.

For example, suppose you want to use the contents of your local `/some_path/example_dir` directory as your `working_dir`. To specify this directory as a local path, include the following in your `runtime_env` dictionary:

```{testcode}
:skipif: True

runtime_env = {..., "working_dir": "/some_path/example_dir", ...}
```

Suppose instead that you want to host the files in your `/some_path/example_dir` directory remotely and provide a remote URI. First, compress the `example_dir` directory into a `.zip`, `.tar.gz`, `.tgz`, or `.tar.xz` archive.

The archive shouldn't contain any files or directories at the top level other than `example_dir`. Use one of the following commands in a terminal:

```bash
cd /some_path
# Using zip:
zip -r archive.zip example_dir
# Using tar.gz:
tar -czf archive.tar.gz example_dir
# Using tar.xz:
tar -cJf archive.tar.xz example_dir
```

Run the command from the *parent directory* of the desired `working_dir` so that the resulting archive contains a single top-level directory. In general, the archive's name and the top-level directory's name can be anything. Ray uses the top-level directory's contents as the `working_dir` or `py_module`.

To check that the archive contains a single top-level directory, run one of the following commands in a terminal:

```bash
# For zip:
zipinfo -1 archive.zip
# For tar.gz:
tar -tzf archive.tar.gz
# For tar.xz:
tar -tJf archive.tar.xz
# example_dir/
# example_dir/my_file_1.txt
# example_dir/subdir/my_file_2.txt
```

Suppose you upload the compressed `example_dir` directory to AWS S3 at the S3 URI `s3://example_bucket/example.zip`. Include the following in your `runtime_env` dictionary:

```{testcode}
:skipif: True

runtime_env = {..., "working_dir": "s3://example_bucket/example.zip", ...}
```

You can also use `.tar.gz`, `.tgz`, or `.tar.xz` archives:

```{testcode}
:skipif: True

runtime_env = {..., "working_dir": "s3://example_bucket/example.tar.gz", ...}
```

For example, specify an XZ-compressed archive the same way:

```{testcode}
:skipif: True

runtime_env = {..., "working_dir": "s3://example_bucket/example.tar.xz", ...}
```

:::{warning}
Check for hidden files and metadata directories in archived dependencies. You can inspect an archive's contents by running `zipinfo -1 archive.zip` or `tar -tzf archive.tar.gz` in a terminal. Some archiving methods can cause hidden files or metadata directories to appear at the top level. To avoid this, run `zip -r` or `tar -czf` directly on the directory you want to compress, from its parent directory. For example, if you have a directory structure such as `a/b` and you want to compress `b`, run the command from directory `a`. If Ray detects more than a single directory at the top level, it uses the entire archive instead of the top-level directory, which might lead to unexpected behavior.
:::

Remote URIs support `.zip`, `.tar.gz`, `.tgz`, and `.tar.xz` archive formats. Supported schemes are `http`, `https`, `s3`, `gs`, `azure`, `abfss`, and `file`. The following list describes the most common remote storage types:

- `HTTPS`: URLs that start with `https`. These URLs are particularly useful because remote Git providers such as GitHub, Bitbucket, and GitLab use `https` URLs as download links for repository archives. As a result, you can host your dependencies on remote Git providers, push updates to them, and specify which dependency versions, meaning commits, your jobs should use. To use packages through `HTTPS` URIs, you must have the `smart_open` library. Install it with `pip install smart_open`.

  - Example:

    - `runtime_env = {"working_dir": "https://github.com/example_username/example_repository/archive/HEAD.zip"}`

- `S3`: URIs starting with `s3://` that point to compressed packages stored in [AWS S3](https://aws.amazon.com/s3/). To use packages through `S3` URIs, you must have the `smart_open` and `boto3` libraries. Install them with `pip install smart_open` and `pip install boto3`. Ray doesn't explicitly pass any credentials to `boto3` for authentication. `boto3` uses your environment variables, shared credentials file, AWS config file, or any combination of them to authenticate access. To configure these, see the [AWS boto3 documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

  - Example:

    - `runtime_env = {"working_dir": "s3://example_bucket/example_file.zip"}`

- `GS`: URIs starting with `gs://` that point to compressed packages stored in [Google Cloud Storage](https://cloud.google.com/storage). To use packages through `GS` URIs, you must have the `smart_open` and `google-cloud-storage` libraries. Install them with `pip install smart_open` and `pip install google-cloud-storage`. Ray doesn't explicitly pass any credentials to the `google-cloud-storage` `Client` object. By default, `google-cloud-storage` uses your local service account keys and environment variables. To set up the credentials that Ray needs to access your remote package, follow the steps in Google Cloud Storage's [Getting started with authentication](https://cloud.google.com/docs/authentication/getting-started) guide.

  - Example:

    - `runtime_env = {"working_dir": "gs://example_bucket/example_file.zip"}`

- `Azure`: URIs starting with `azure://` that point to compressed packages stored in [Azure Blob Storage](https://azure.microsoft.com/en-us/products/storage/blobs). To use packages through `Azure` URIs, you must have the `smart_open`, `azure-storage-blob`, and `azure-identity` libraries. Install them with `pip install smart_open[azure] azure-storage-blob azure-identity`. Ray supports two authentication methods for Azure Blob Storage:

  1. Connection string: Set the environment variable `AZURE_STORAGE_CONNECTION_STRING` to your Azure storage connection string.
  1. Managed Identity: Set the environment variable `AZURE_STORAGE_ACCOUNT` to your Azure storage account name. This method uses Azure's Managed Identity for authentication.

  - Example:

    - `runtime_env = {"working_dir": "azure://container-name/example_file.zip"}`

:::{caution}
The `smart_open`, `boto3`, `google-cloud-storage`, `azure-storage-blob`, and `azure-identity` packages aren't installed by default, and specifying them in the `pip` section of your `runtime_env` isn't sufficient. The relevant packages must already be installed on all nodes of the cluster when Ray starts.
:::

## Hosting a dependency on a remote Git provider: Step-by-step guide

You can store your dependencies in repositories on a remote Git provider, such as GitHub, Bitbucket, or GitLab, and periodically push changes to keep them updated. This section describes how to store a dependency on GitHub and use it in your runtime environment.

:::{note}
These steps are also useful if you use another large remote Git provider, such as Bitbucket or GitLab. For simplicity, this section refers only to GitHub, but you can follow along on your provider.
:::

First, create a repository on GitHub to store your `working_dir` contents or your `py_module` dependency. By default, when you download a zip file of your repository, the zip file already contains a single top-level directory that holds the repository contents. So you can upload your `working_dir` contents or your `py_module` dependency directly to the GitHub repository.

After you upload your `working_dir` contents or your `py_module` dependency, you need the HTTPS URL of the repository zip file to specify in your `runtime_env` dictionary.

You have two options to get the HTTPS URL.

(option-1-download-zip-quicker-to-implement-but-not-recommended-for-production-environments)=

### Option 1: Download ZIP

The first option is to use the remote Git provider's "Download Zip" feature, which provides an HTTPS link that zips and downloads your repository. This option is quicker to implement, but it isn't recommended for production environments, because the link only downloads a zip file of a repository branch's latest commit. To find a GitHub URL, go to your repository on [GitHub](https://github.com/), choose a branch, and click the green **Code** drop-down button.

```{figure} images/ray_repo.png
:width: 500px
```

A menu opens with three options. **Clone** provides HTTPS and SSH links to clone the repository. The other two are **Open with GitHub Desktop** and **Download ZIP**. Right-click **Download ZIP** to open a pop-up menu near your cursor, then select **Copy Link Address**.

```{figure} images/download_zip_url.png
:width: 300px
```

Your HTTPS link is now on your clipboard. Paste it into your `runtime_env` dictionary.

:::{warning}
Using the HTTPS URL from your Git provider's "Download as Zip" feature isn't recommended if the URL always points to the latest commit. For instance, using this method on GitHub generates a link that always points to the latest commit on the chosen branch.

If you specify this link in the `runtime_env` dictionary, your Ray cluster always uses the chosen branch's latest commit. This creates a consistency risk. If you push an update to your remote Git repository while your cluster's nodes are pulling the repository's contents, some nodes might pull the version of your package from immediately before you pushed, and others might pull the version from immediately after. For consistency, specify a particular commit so that all the nodes use the same package. To create a URL that points to a specific commit, see the "Option 2: Manually create a URL" section.
:::

(option-2-manually-create-url-slower-to-implement-but-recommended-for-production-environments)=

### Option 2: Manually create a URL

The second option is slower to implement, but it's recommended for production environments. Create the URL manually by matching your use case to one of the following examples. This approach gives you finer-grained control over which repository branch and commit to use when you generate your dependency zip file, which prevents the consistency issues that the preceding warning describes. To create the URL, pick the URL template that fits your use case from the following examples, and fill in all parameters in brackets, such as `[username]` and `[repository]`, with the values from your repository. For instance, suppose your GitHub username is `example_user`, the repository's name is `example_repository`, and the desired commit hash is `abcdefg`. If `example_repository` is public and you want to retrieve the `abcdefg` commit, which matches the first example use case, the URL is the following:

```{testcode}
runtime_env = {"working_dir": ("https://github.com"
                               "/example_user/example_repository/archive/abcdefg.zip")}
```

The following examples show use cases and their corresponding URLs.

To retrieve a package from a specific commit hash on a public GitHub repository, use the following URL:

```{testcode}
runtime_env = {"working_dir": ("https://github.com"
                               "/[username]/[repository]/archive/[commit hash].zip")}
```

To retrieve a package from a private GitHub repository with a personal access token during development, use the following URL:

```{testcode}
runtime_env = {"working_dir": ("https://[username]:[personal access token]@github.com"
                               "/[username]/[private repository]/archive/[commit hash].zip")}
```

For production, learn how to {ref}`authenticate private dependencies safely <runtime-env-auth>` instead.

To retrieve a package from a public GitHub repository's latest commit, use the following URL:

```{testcode}
runtime_env = {"working_dir": ("https://github.com"
                               "/[username]/[repository]/archive/HEAD.zip")}
```

To retrieve a package from a specific commit hash on a public Bitbucket repository, use the following URL:

```{testcode}
runtime_env = {"working_dir": ("https://bitbucket.org"
                               "/[owner]/[repository]/get/[commit hash].tar.gz")}
```

:::{tip}
Specify a particular commit instead of always using the latest commit. This prevents consistency issues on a multi-node Ray cluster. For more information, see the warning in the "Option 1: Download ZIP" section.
:::

After you specify the URL in your `runtime_env` dictionary, pass the dictionary into a `ray.init()` or `.options()` call to use your remotely hosted dependency.


## Debugging
If Ray can't set up the `runtime_env`, for example because of network issues or download failures, Ray fails to schedule the tasks and actors that require it. If you call `ray.get`, it raises `RuntimeEnvSetupError` with a detailed error message.

```{testcode}
import ray
import time

@ray.remote
def f():
    pass

@ray.remote
class A:
    def f(self):
        pass

start = time.time()
bad_env = {"conda": {"dependencies": ["this_doesnt_exist"]}}

# [Tasks] will raise `RuntimeEnvSetupError`.
try:
  ray.get(f.options(runtime_env=bad_env).remote())
except ray.exceptions.RuntimeEnvSetupError:
  print("Task fails with RuntimeEnvSetupError")

# [Actors] will raise `RuntimeEnvSetupError`.
a = A.options(runtime_env=bad_env).remote()
try:
  ray.get(a.f.remote())
except ray.exceptions.RuntimeEnvSetupError:
  print("Actor fails with RuntimeEnvSetupError")
```

```{testoutput}
Task fails with RuntimeEnvSetupError
Actor fails with RuntimeEnvSetupError
```


You can always find the full logs in the file `runtime_env_setup-[job_id].log` for per-actor, per-task, and per-job environments, or in `runtime_env_setup-ray_client_server_[port].log` for per-job environments when you use Ray Client.

To stream `runtime_env` debugging logs, set the environment variable `RAY_RUNTIME_ENV_LOG_TO_DRIVER_ENABLED=1` on each node before you start Ray, for example with {ref}`setup commands <cluster-configuration-setup-commands>` in the Ray cluster configuration file. Ray then prints the full `runtime_env` setup log messages to the driver, which is the script that calls `ray.init()`.

The following example shows the log output:

```{testcode}
:hide:

ray.shutdown()
```

```{testcode}
ray.init(runtime_env={"pip": ["requests"]})
```

```{testoutput}
:options: +MOCK

(pid=runtime_env) 2022-02-28 14:12:33,653       INFO pip.py:188 -- Creating virtualenv at /tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/virtualenv, current python dir /Users/user/anaconda3/envs/ray-py38
(pid=runtime_env) 2022-02-28 14:12:33,653       INFO utils.py:76 -- Run cmd[1] ['/Users/user/anaconda3/envs/ray-py38/bin/python', '-m', 'virtualenv', '--app-data', '/tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/virtualenv_app_data', '--reset-app-data', '--no-periodic-update', '--system-site-packages', '--no-download', '/tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/virtualenv']
(pid=runtime_env) 2022-02-28 14:12:34,267       INFO utils.py:97 -- Output of cmd[1]: created virtual environment CPython3.8.11.final.0-64 in 473ms
(pid=runtime_env)   creator CPython3Posix(dest=/private/tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/virtualenv, clear=False, no_vcs_ignore=False, global=True)
(pid=runtime_env)   seeder FromAppData(download=False, pip=bundle, setuptools=bundle, wheel=bundle, via=copy, app_data_dir=/private/tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/virtualenv_app_data)
(pid=runtime_env)     added seed packages: pip==22.0.3, setuptools==60.6.0, wheel==0.37.1
(pid=runtime_env)   activators BashActivator,CShellActivator,FishActivator,NushellActivator,PowerShellActivator,PythonActivator
(pid=runtime_env)
(pid=runtime_env) 2022-02-28 14:12:34,268       INFO utils.py:76 -- Run cmd[2] ['/tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/virtualenv/bin/python', '-c', 'import ray; print(ray.__version__, ray.__path__[0])']
(pid=runtime_env) 2022-02-28 14:12:35,118       INFO utils.py:97 -- Output of cmd[2]: 3.0.0.dev0 /Users/user/ray/python/ray
(pid=runtime_env)
(pid=runtime_env) 2022-02-28 14:12:35,120       INFO pip.py:236 -- Installing python requirements to /tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/virtualenv
(pid=runtime_env) 2022-02-28 14:12:35,122       INFO utils.py:76 -- Run cmd[3] ['/tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/virtualenv/bin/python', '-m', 'pip', 'install', '--disable-pip-version-check', '--no-cache-dir', '-r', '/tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/requirements.txt']
(pid=runtime_env) 2022-02-28 14:12:38,000       INFO utils.py:97 -- Output of cmd[3]: Requirement already satisfied: requests in /Users/user/anaconda3/envs/ray-py38/lib/python3.8/site-packages (from -r /tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/requirements.txt (line 1)) (2.26.0)
(pid=runtime_env) Requirement already satisfied: idna<4,>=2.5 in /Users/user/anaconda3/envs/ray-py38/lib/python3.8/site-packages (from requests->-r /tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/requirements.txt (line 1)) (3.2)
(pid=runtime_env) Requirement already satisfied: certifi>=2017.4.17 in /Users/user/anaconda3/envs/ray-py38/lib/python3.8/site-packages (from requests->-r /tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/requirements.txt (line 1)) (2021.10.8)
(pid=runtime_env) Requirement already satisfied: urllib3<1.27,>=1.21.1 in /Users/user/anaconda3/envs/ray-py38/lib/python3.8/site-packages (from requests->-r /tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/requirements.txt (line 1)) (1.26.7)
(pid=runtime_env) Requirement already satisfied: charset-normalizer~=2.0.0 in /Users/user/anaconda3/envs/ray-py38/lib/python3.8/site-packages (from requests->-r /tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/requirements.txt (line 1)) (2.0.6)
(pid=runtime_env)
(pid=runtime_env) 2022-02-28 14:12:38,001       INFO utils.py:76 -- Run cmd[4] ['/tmp/ray/session_2022-02-28_14-12-29_909064_87908/runtime_resources/pip/0cc818a054853c3841171109300436cad4dcf594/virtualenv/bin/python', '-c', 'import ray; print(ray.__version__, ray.__path__[0])']
(pid=runtime_env) 2022-02-28 14:12:38,804       INFO utils.py:97 -- Output of cmd[4]: 3.0.0.dev0 /Users/user/ray/python/ray
```

For more details, see {ref}`Log files in logging directory <logging-directory-structure>`.
