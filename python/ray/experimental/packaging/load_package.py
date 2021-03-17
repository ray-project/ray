"""Support for loading code packages into Ray at runtime.

Ray packages allow developers to define self-contained code modules that can
be imported reproducibly into any Ray cluster. Each package can define its own
runtime environment, which can include:
 - Different versions of code (e.g., from different git commits).
 - Different Python libraries (e.g., conda environments, pip dependencies).
 - Different Docker container images.

You can run this file for an example of loading a "hello world" package.
"""

import importlib.util
import os
import re
import hashlib
import subprocess
import tempfile
from typing import Union, Dict
import yaml

import ray
import ray._private.runtime_env as runtime_support


def load_package(package_def: Union[str, Dict[str, str]]) -> "_RuntimePackage":
    """Load the code package given a config.

    The config can be a Python dictionary, a local path to a configuration
    YAML file, or a remote URI pointing to a config file (currently this only
    supports GitHub).

    Args:
        package_def (Union[str, Dict[str, str]]): A Python dictionary or path
            to the configuration YAML that defines the package. For
            documentation on the packaging format, see the example YAML in
            ``example_pkg/ray_pkg.yaml``.

    Examples:
        >>> # Load from Python dict.
        >>> my_pkg = load_package({
            "name": example_package,
            "description": "This is my example package!",
            "stub_file": "/path/to/stub/file.py",
            "runtime_env": {"docker": "anyscale-ml/ray-ml:nightly-py38-cpu"}
        })

        >>> # Load from local.
        >>> my_pkg = load_package("~/path/to/my_pkg.yaml")

        >>> # Load from GitHub.
        >>> my_pkg = ray.util.load_package(
        ...   "https://raw.githubusercontent.com/user/repo/refspec"
        ...   "/path/to/package/my_pkg.yaml")

        >>> # Inspect the package runtime env.
        >>> print(my_pkg._runtime_env)
        ... {"conda": {...},
        ...  "docker": "anyscale-ml/ray-ml:nightly-py38-cpu",
        ...  "working_dir": "https://github.com/demo/foo/blob/v3.0/project/"}

        >>> # Run remote functions from the package.
        >>> my_pkg.my_func.remote(1, 2)

        >>> # Create actors from the package.
        >>> actor = my_pkg.MyActor.remote(3, 4)

        >>> # Create new remote funcs in the same env as a package.
        >>> @ray.remote(runtime_env=my_pkg._runtime_env)
        >>> def f(): ...
    """

    if isinstance(package_def, dict):
        # TODO(edoakes): when loading the package natively in Python, we should
        # support passing in the stub module directly instead of a file.
        config = package_def
        base_dir = os.path.abspath(os.path.dirname(__file__))
    elif isinstance(package_def, str):
        config_path = _download_from_github_if_needed(package_def)

        if not os.path.exists(config_path):
            raise ValueError(
                "Config file does not exist: {}".format(config_path))

        # TODO(ekl) validate schema?
        with open(config_path) as f:
            config = yaml.safe_load(f.read())
        base_dir = os.path.abspath(os.path.dirname(config_path))
    else:
        raise TypeError("package_def must be str or dict.")

    runtime_env = config["runtime_env"]

    # Autofill working directory by uploading to GCS storage.
    if "working_dir" not in runtime_env:
        pkg_name = runtime_support.get_project_package_name(
            working_dir=base_dir, modules=[])
        pkg_uri = runtime_support.Protocol.GCS.value + "://" + pkg_name

        def do_register_package():
            if not runtime_support.package_exists(pkg_uri):
                tmp_path = os.path.join(_pkg_tmp(), "_tmp{}".format(pkg_name))
                runtime_support.create_project_package(
                    working_dir=base_dir, modules=[], output_path=tmp_path)
                # TODO(ekl) does this get garbage collected correctly with the
                # current job id?
                runtime_support.push_package(pkg_uri, tmp_path)
                if not runtime_support.package_exists(pkg_uri):
                    raise RuntimeError(
                        "Failed to upload package {}".format(pkg_uri))

        if ray.is_initialized():
            do_register_package()
        else:
            ray.worker._post_init_hooks.append(do_register_package)
        runtime_env["working_dir"] = pkg_uri

    # Autofill conda config.
    conda_yaml = os.path.join(base_dir, "conda.yaml")
    if os.path.exists(conda_yaml):
        if "conda" in runtime_env:
            raise ValueError(
                "Both conda.yaml and conda: section found in package")
        runtime_env["conda"] = yaml.safe_load(open(conda_yaml).read())

    pkg = _RuntimePackage(
        name=config["name"],
        desc=config["description"],
        stub_file=os.path.join(base_dir, config["stub_file"]),
        runtime_env=runtime_env)
    return pkg


def _download_from_github_if_needed(config_path: str) -> str:
    """Resolve a GitHub raw link to the config file to a local path.

    If the user specifies a GitHub raw URL, download the repo specified at
    that particular URL locally. This lets us treat YAMLs linked from GitHub
    the same as local files.
    """
    if config_path.startswith("http"):
        if "github" not in config_path:
            raise ValueError(
                "Only GitHub URLs are supported by load_package().")
        if "raw.githubusercontent.com" not in config_path:
            raise ValueError(
                "GitHub URL must start with raw.githubusercontent.com")
        URL_FORMAT = ".*raw.githubusercontent.com/([^/]*)/([^/]*)/([^/]*)/(.*)"
        match = re.match(URL_FORMAT, config_path)
        if not match:
            raise ValueError(
                "GitHub URL must be of format {}".format(URL_FORMAT))
        gh_user = match.group(1)
        gh_repo = match.group(2)
        gh_branch = match.group(3)
        gh_subdir = match.group(4)

        # Compute the cache key based on the URL.
        hasher = hashlib.sha1()
        hasher.update(config_path.encode("utf-8"))
        config_key = hasher.hexdigest()
        final_path = os.path.join(_pkg_tmp(),
                                  "github_snapshot_{}".format(config_key))

        # Only download the repo if needed.
        if not os.path.exists(final_path):
            tmp = tempfile.mkdtemp(
                prefix="github_{}".format(gh_repo), dir=_pkg_tmp())
            subprocess.check_call([
                "curl", "--fail", "-L",
                "https://github.com/{}/{}/tarball/{}".format(
                    gh_user, gh_repo, gh_branch), "--output", tmp + ".tar.gz"
            ])
            subprocess.check_call([
                "tar", "xzf", tmp + ".tar.gz", "-C", tmp,
                "--strip-components=1"
            ])
            os.rename(tmp, final_path)
        return os.path.join(final_path, gh_subdir)

    return config_path


class _RuntimePackage:
    """Represents a Ray package loaded via ``load_package()``.

    This class provides access to the symbols defined by the stub file of the
    package (e.g., remote functions and actor definitions). You can also
    access the raw runtime env defined by the package via ``pkg._runtime_env``.
    """

    def __init__(self, name: str, desc: str, stub_file: str,
                 runtime_env: dict):
        self._name = name
        self._description = desc
        self._stub_file = stub_file
        self._runtime_env = runtime_env
        _validate_stub_file(self._stub_file)

        spec = importlib.util.spec_from_file_location(self._name,
                                                      self._stub_file)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        self._module = module

        for symbol in dir(self._module):
            if not symbol.startswith("_"):
                value = getattr(self._module, symbol)
                if (isinstance(value, ray.remote_function.RemoteFunction)
                        or isinstance(value, ray.actor.ActorClass)):
                    setattr(
                        self, symbol, value.options(runtime_env=runtime_env))

    def __repr__(self):
        return "ray._RuntimePackage(module={}, runtime_env={})".format(
            self._module, self._runtime_env)


def _validate_stub_file(stub_file: str):
    if not os.path.exists(stub_file):
        raise ValueError("Stub file does not exist: {}".format(stub_file))
    for line in open(stub_file):
        line = line.replace("\n", "")
        if line.startswith("import ") or line.startswith("from "):
            if line != "import ray" and "noqa" not in line:
                raise ValueError(
                    "Stub files are only allowed to import `ray` "
                    "at top-level, found `{}`. Please either remove or "
                    "change this into a lazy import. To unsafely allow "
                    "this import, add `# noqa` to the line "
                    "in question.".format(line))


def _pkg_tmp():
    tmp = "/tmp/ray/packaging"
    os.makedirs(tmp, exist_ok=True)
    return tmp


if __name__ == "__main__":
    ray.init()

    print("-> Testing load local")
    pkg = load_package("./example_pkg/ray_pkg.yaml")
    print("-> Loaded package", pkg)
    print("-> Package symbols", [x for x in dir(pkg) if not x.startswith("_")])
    print("-> Testing actor call")
    a = pkg.MyActor.remote()
    print(ray.get(a.f.remote()))
    print("-> Testing method call")
    print(ray.get(pkg.my_func.remote()))

    print("-> Testing load from github")
    pkg2 = load_package(
        "http://raw.githubusercontent.com/ray-project/ray/master/"
        "python/ray/experimental/packaging/example_pkg/ray_pkg.yaml")
    print("-> Loaded package", pkg2)
    print("-> Testing method call")
    print(ray.get(pkg2.my_func.remote()))

    print("-> Testing load from Python dict")
    os.chdir("example_pkg")
    pkg3 = load_package({
        "name": "example_package",
        "description": "This is the example config for my package.",
        "stub_file": "my_pkg/stubs.py",
        "runtime_env": {
            "docker": "anyscale-ml/ray-ml:nightly-py38-cpu"
        }
    })
    print("-> Loaded package", pkg3)
    print("-> Testing method call")
    print(ray.get(pkg3.my_func.remote()))
