import importlib.util
import os
import yaml

import ray


class RuntimePackage:
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
                setattr(self, symbol, getattr(self._module, symbol))

    def __repr__(self):
        return "ray.RuntimePackage(module={}, runtime_env={})".format(
            self._module, self._runtime_env)


def load_package(config_path: str) -> RuntimePackage:
    config = yaml.load(open(config_path).read())
    base_dir = os.path.abspath(os.path.dirname(config_path))
    pkg = RuntimePackage(
        name=config["name"],
        desc=config["description"],
        stub_file=os.path.join(base_dir, config["stub_file"]),
        runtime_env=config["runtime_env"])
    return pkg


if __name__ == "__main__":
    pkg = load_package("./example_pkg/ray_pkg.yaml")
    print("-> Loaded package", pkg)
    print("-> Package symbols", dir(pkg))

    ray.init()
    print("-> Testing method call")
    print(ray.get(pkg.my_func.remote()))
    print("-> Testing actor call")
    a = pkg.MyActor.remote()
    print(ray.get(a.f.remote()))
