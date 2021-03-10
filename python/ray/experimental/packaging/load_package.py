import importlib.util
import os
import yaml

import ray
import ray._private.runtime_env as runtime_support


class _RuntimePackage:
    def __init__(self, name: str, desc: str, stub_file: str,
                 runtime_env: dict):
        self._name = name
        self._description = desc
        self._stub_file = stub_file
        self._runtime_env = runtime_env

        if not os.path.exists(stub_file):
            raise ValueError("Stub file does not exist: {}".format(stub_file))

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
                    # TODO(ekl) use the runtime_env option here instead of
                    # the override env vars. Currently this doesn't work since
                    # there is no way to define per-task job config.
                    setattr(
                        self,
                        symbol,
                        value.options(override_environment_variables={
                            "RAY_RUNTIME_ENV_FILES": runtime_env["files"]
                        }))
                else:
                    setattr(self, symbol, value)

    def __repr__(self):
        return "ray._RuntimePackage(module={}, runtime_env={})".format(
            self._module, self._runtime_env)


def _download_from_github_if_needed(config_path: str) -> str:
    # TODO(ekl) support github URIs for the config.
    return config_path


def load_package(config_path: str) -> _RuntimePackage:
    if not os.path.exists(config_path):
        raise ValueError("Config file does not exist: {}".format(config_path))

    if not ray.is_initialized():
        # TODO(ekl) lift this requirement?
        raise RuntimeError("Ray must be initialized first to load packages.")

    config_path = _download_from_github_if_needed(config_path)
    # TODO(ekl) validate schema?
    config = yaml.safe_load(open(config_path).read())
    base_dir = os.path.abspath(os.path.dirname(config_path))
    runtime_env = config["runtime_env"]

    # Autofill working directory by uploading to GCS storage.
    if "files" not in runtime_env:
        pkg_name = runtime_support.get_project_package_name(
            working_dir=base_dir, modules=[])
        pkg_uri = runtime_support.Protocol.GCS.value + "://" + pkg_name
        if not runtime_support.package_exists(pkg_uri):
            tmp_path = "/tmp/ray/_tmp{}".format(pkg_name)
            runtime_support.create_project_package(
                working_dir=base_dir, modules=[], output_path=tmp_path)
            # TODO(ekl) does this get garbage collected correctly with the
            # current job id?
            runtime_support.push_package(pkg_uri, tmp_path)
            if not runtime_support.package_exists(pkg_uri):
                raise RuntimeError(
                    "Failed to upload package {}".format(pkg_uri))
            runtime_env["files"] = pkg_uri

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


if __name__ == "__main__":
    ray.init()
    pkg = load_package("./example_pkg/ray_pkg.yaml")
    print("-> Loaded package", pkg)
    print("-> Package symbols", [x for x in dir(pkg) if not x.startswith("_")])
    print("-> Testing actor call")
    a = pkg.MyActor.remote()
    print(ray.get(a.f.remote()))
    print("-> Testing method call")
    for _ in range(5):
        print(ray.get(pkg.my_func.remote()))
