import os
import logging
import yaml
import re

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
