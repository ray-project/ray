import json
import logging
import os
from pathlib import Path
import sys
from typing import Any, Dict, Optional
from urllib.parse import urlparse
import yaml

import ray

# We need to setup this variable before
# using this module
PKG_DIR = None

logger = logging.getLogger(__name__)

FILE_SIZE_WARNING = 10 * 1024 * 1024  # 10MiB
# NOTE(edoakes): we should be able to support up to 512 MiB based on the GCS'
# limit, but for some reason that causes failures when downloading.
GCS_STORAGE_MAX_SIZE = 100 * 1024 * 1024  # 100MiB


class RuntimeEnvDict:
    """Parses and validates the runtime env dictionary from the user.

    Attributes:
        working_dir (Path | str): Specifies the working directory of the
            worker. This can be
                - A local directory
                - A zip file
                - A s3 bucket url that contains zipped working_dir files
            Examples:
                "."  # cwd
                "local_project.zip"  # archive is unpacked into directory
                "s3://bucket/local_project.zip" # downloaded then unpacked
                                                # into directory
        py_modules (List[Path]): Similar to working_dir, but specifies python
            modules to add to the `sys.path`.
            Examples:
                ["/path/to/other_module", "/other_path/local_project.zip"]
        pip (List[str] | str): Either a list of pip packages, or a string
            containing the path to a pip requirements.txt file.
        conda (dict | str): Either the conda YAML config, the name of a
            local conda env (e.g., "pytorch_p36"), or the path to a conda
            environment.yaml file.
            The Ray dependency will be automatically injected into the conda
            env to ensure compatibility with the cluster Ray. The conda name
            may be mangled automatically to avoid conflicts between runtime
            envs.
            This field cannot be specified at the same time as the 'pip' field.
            To use pip with conda, please specify your pip dependencies within
            the conda YAML config:
            https://conda.io/projects/conda/en/latest/user-guide/tasks/manage-e
            nvironments.html#create-env-file-manually
            Examples:
                {"channels": ["defaults"], "dependencies": ["codecov"]}
                "pytorch_p36"   # Found on DLAMIs
        container (dict): Require a given (Docker) container image,
            The Ray worker process will run in a container with this image.
            The `worker_path` is the default_worker.py path.
            The `run_options` list spec is here:
            https://docs.docker.com/engine/reference/run/
            Examples:
                {"image": "anyscale/ray-ml:nightly-py38-cpu",
                 "worker_path": "/root/python/ray/workers/default_worker.py",
                 "run_options": ["--cap-drop SYS_ADMIN","--log-level=debug"]}
        env_vars (dict): Environment variables to set.
            Examples:
                {"OMP_NUM_THREADS": "32", "TF_WARNINGS": "none"}
    """

    def __init__(self,
                 runtime_env_json: dict,
                 working_dir: Optional[str] = None):
        # Simple dictionary with all options validated. This will always
        # contain all supported keys; values will be set to None if
        # unspecified. However, if all values are None this is set to {}.
        self._dict = dict()

        working_dir = self._handle_working_dir(runtime_env_json, working_dir)
        self._handle_conda(runtime_env_json, working_dir)
        self._handle_pip(runtime_env_json, working_dir)

        if "uris" in runtime_env_json:
            self._dict["uris"] = runtime_env_json["uris"]

        if "container" in runtime_env_json:
            self._dict["container"] = runtime_env_json["container"]

        self._handle_env_vars(runtime_env_json)

        if "_ray_release" in runtime_env_json:
            self._dict["_ray_release"] = runtime_env_json["_ray_release"]

        if "_ray_commit" in runtime_env_json:
            self._dict["_ray_commit"] = runtime_env_json["_ray_commit"]
        else:
            if self._dict.get("pip") or self._dict.get("conda"):
                self._dict["_ray_commit"] = ray.__commit__

        # Used for testing wheels that have not yet been merged into master.
        # If this is set to True, then we do not inject Ray into the conda
        # or pip dependencies.
        if os.environ.get("RAY_RUNTIME_ENV_LOCAL_DEV_MODE"):
            runtime_env_json["_inject_current_ray"] = True
        if "_inject_current_ray" in runtime_env_json:
            self._dict["_inject_current_ray"] = runtime_env_json[
                "_inject_current_ray"]

        # TODO(ekl) we should have better schema validation here.
        # TODO(ekl) support py_modules
        # TODO(architkulkarni) support docker

        # TODO(architkulkarni) This is to make it easy for the worker caching
        # code in C++ to check if the env is empty without deserializing and
        # parsing it.  We should use a less confusing approach here.
        if all(val is None for val in self._dict.values()):
            self._dict = {}

    def _handle_working_dir(self, runtime_env_json: dict,
                            working_dir: Optional[str]):
        if "working_dir" in runtime_env_json:
            self._dict["working_dir"] = runtime_env_json["working_dir"]
            if not isinstance(self._dict["working_dir"], str):
                raise TypeError("`working_dir` must be a string. Type "
                                f"{type(self._dict['working_dir'])} received.")
            parsed = urlparse(self._dict["working_dir"])
            # parsed.

            working_dir = Path(self._dict["working_dir"]).absolute()
        else:
            self._dict["working_dir"] = None
            working_dir = Path(working_dir).absolute() if working_dir else None

        return working_dir

    def _handle_conda(self, runtime_env_json: dict, working_dir: str):
        self._dict["conda"] = None
        if "conda" in runtime_env_json:
            if sys.platform == "win32":
                raise NotImplementedError("The 'conda' field in runtime_env "
                                          "is not currently supported on "
                                          "Windows.")
            conda = runtime_env_json["conda"]
            if isinstance(conda, str):
                yaml_file = Path(conda)
                if yaml_file.suffix in (".yaml", ".yml"):
                    if working_dir and not yaml_file.is_absolute():
                        yaml_file = working_dir / yaml_file
                    if not yaml_file.is_file():
                        raise ValueError(
                            f"Can't find conda YAML file {yaml_file}")
                    try:
                        self._dict["conda"] = yaml.safe_load(
                            yaml_file.read_text())
                    except Exception as e:
                        raise ValueError(
                            f"Invalid conda file {yaml_file} with error {e}")
                else:
                    logger.info(
                        f"Using preinstalled conda environment: {conda}")
                    self._dict["conda"] = conda
            elif isinstance(conda, dict):
                self._dict["conda"] = conda
            elif conda is not None:
                raise TypeError("runtime_env['conda'] must be of type str or "
                                "dict")

    def _handle_pip(self, runtime_env_json: dict, working_dir: str):
        self._dict["pip"] = None
        pip = runtime_env_json.get("pip")
        if pip is not None:
            if sys.platform == "win32":
                raise NotImplementedError("The 'pip' field in runtime_env "
                                          "is not currently supported on "
                                          "Windows.")
            conda = runtime_env_json.get("conda")
            if runtime_env_json.get("conda") is not None:
                raise ValueError(
                    "The 'pip' field and 'conda' field of "
                    "runtime_env cannot both be specified.\n"
                    f"specified pip field: {runtime_env_json['pip']}\n"
                    f"specified conda field: {runtime_env_json['conda']}\n"
                    "To use pip with conda, please only set the 'conda' "
                    "field, and specify your pip dependencies "
                    "within the conda YAML config dict: see "
                    "https://conda.io/projects/conda/en/latest/"
                    "user-guide/tasks/manage-environments.html"
                    "#create-env-file-manually")
            if isinstance(pip, str):
                # We have been given a path to a requirements.txt file.
                pip_file = Path(pip)
                if working_dir and not pip_file.is_absolute():
                    pip_file = working_dir / pip_file
                if not pip_file.is_file():
                    raise ValueError(f"{pip_file} is not a valid file")
                self._dict["pip"] = pip_file.read_text()
            elif isinstance(pip, list) and all(
                    isinstance(dep, str) for dep in pip):
                # Construct valid pip requirements.txt from list of packages.
                self._dict["pip"] = "\n".join(pip) + "\n"
            else:
                raise TypeError("runtime_env['pip'] must be of type str or "
                                "List[str]")

    def _handle_env_vars(self, runtime_env_json: dict):
        self._dict["env_vars"] = None
        env_vars = runtime_env_json.get("env_vars")
        if env_vars is not None:
            self._dict["env_vars"] = env_vars
            if not (isinstance(env_vars, dict) and all(
                    isinstance(k, str) and isinstance(v, str)
                    for (k, v) in env_vars.items())):
                raise TypeError("runtime_env['env_vars'] must be of type"
                                "Dict[str, str]")

    def get_parsed_dict(self) -> dict:
        return self._dict

    def serialize(self) -> str:
        # Use sort_keys=True because we will use the output as a key to cache
        # workers by, so we need the serialization to be independent of the
        # dict order.
        return json.dumps(self._dict, sort_keys=True)

    def set_uris(self, uris):
        self._dict["uris"] = uris

    def get(self, key: str, default=None):
        return self._dict.get(key, default)

    def __getitem__(self, key: str):
        return self._dict.get(key)

    def __repr__(self):
        return f"RuntimeEnvDict({self._dict})"


def override_task_or_actor_runtime_env(
        runtime_env: Optional[Dict[str, Any]],
        parent_runtime_env: Dict[str, Any]) -> Dict[str, Any]:
    if runtime_env:
        if runtime_env.get("working_dir"):
            raise NotImplementedError(
                "Overriding working_dir for actors is not supported. "
                "Please use ray.init(runtime_env={'working_dir': ...}) "
                "to configure per-job environment instead.")
        # NOTE(edoakes): this is sort of hacky, but we pass in the parent
        # working_dir here so the relative path to a requirements.txt file
        # works. The right solution would be to merge the runtime_env with the
        # parent runtime env before validation.
        runtime_env_dict = RuntimeEnvDict(
            runtime_env, working_dir=parent_runtime_env.get(
                "working_dir")).get_parsed_dict()
    else:
        runtime_env_dict = {}

    # If per-actor URIs aren't specified, override them with those in the
    # job config.
    if "uris" not in runtime_env_dict and "uris" in parent_runtime_env:
        runtime_env_dict["uris"] = parent_runtime_env.get("uris")

    return runtime_env_dict
