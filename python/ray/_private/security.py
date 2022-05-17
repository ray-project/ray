import os
import logging
import yaml
import re
import io
import ray.cloudpickle as pickle
from ray import ray_constants

logger = logging.getLogger(__name__)

ENV_VAR_NAME = "RAY_SECURERITY_CONFIG_PATH"

RAY_SECURERITY_CONFIG_PATH = os.environ.get(ENV_VAR_NAME, None)


class RemoteFunctionWhitelist:
    module_whitelist = None

    @classmethod
    def whitelist_init(cls):
        if RAY_SECURERITY_CONFIG_PATH:
            cls.module_whitelist = yaml.safe_load(
                open(RAY_SECURERITY_CONFIG_PATH, "rt")
            ).get("remote_function_whitelist", None)
            if cls.module_whitelist:
                logger.info(
                    "The remote function module whitelist have been set. It means that"
                    " if your remote functions or actor classes are not included in "
                    "the whitelist, your remote tasks or actors will fail. You can "
                    "modify the environment variable %s to change the whitelist or "
                    'disable this functionality by "unset %s" directly. The whitelist'
                    " is %s.",
                    ENV_VAR_NAME,
                    ENV_VAR_NAME,
                    cls.module_whitelist,
                )

    @classmethod
    def whitelist_check(cls, function_descriptor):
        if not cls.module_whitelist:
            return
        else:
            for module in cls.module_whitelist:
                if re.match(module, function_descriptor.module_name):
                    return
            raise KeyError(
                "Remote function module whitelist check failed "
                f"for {function_descriptor}"
            )


RemoteFunctionWhitelist.whitelist_init()

_pickle_whitelist = None


class RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        module_whitelist = (
            None if _pickle_whitelist is None else _pickle_whitelist.get("module", {})
        )
        package_whitelist = (
            None if _pickle_whitelist is None else _pickle_whitelist.get("package", [])
        )

        package = module.split(".")[0]
        if package == "ray":
            return super().find_class(module, name)

        if package_whitelist is None or package in package_whitelist:
            return super().find_class(module, name)

        if module_whitelist is None or (
            module in module_whitelist
            and (module_whitelist[module] is None or name in module_whitelist[module])
        ):
            return super().find_class(module, name)

        # Forbid everything else.
        raise pickle.UnpicklingError("global '%s.%s' is forbidden" % (module, name))


def restricted_loads(
    serialized_data,
    *,
    fix_imports=True,
    encoding="ASCII",
    errors="strict",
    buffers=None,
):
    if isinstance(serialized_data, str):
        raise TypeError("Can't load pickle from unicode string")
    file = io.BytesIO(serialized_data)
    return RestrictedUnpickler(
        file, fix_imports=fix_imports, buffers=buffers, encoding=encoding, errors=errors
    ).load()


def patch_pickle_for_security():
    global _pickle_whitelist
    whitelist_path = ray_constants.RAY_PICKLE_WHITELIST_CONFIG_PATH
    if whitelist_path is None:
        return

    _pickle_whitelist = yaml.safe_load(open(whitelist_path, "rt")).get(
        "pickle_whitelist", None
    )
    if _pickle_whitelist is None:
        return

    _pickle_whitelist["module"] = _pickle_whitelist.get("module", {})
    _pickle_whitelist["package"] = _pickle_whitelist.get("package", [])

    if "*" in _pickle_whitelist["module"] or "*" in _pickle_whitelist["package"]:
        _pickle_whitelist = None
    for module_name, attr_list in _pickle_whitelist["module"].items():
        if "*" in attr_list:
            _pickle_whitelist["module"][module_name] = None
    pickle.loads = restricted_loads


patch_pickle_for_security()
