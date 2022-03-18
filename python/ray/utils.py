from typing import Dict, Union, Optional
from google.protobuf import json_format

import ray._private.utils as private_utils
from ray.runtime_env import RuntimeEnv, RuntimeEnvConfig
from ray.core.generated.runtime_env_common_pb2 import (
    RuntimeEnvInfo as ProtoRuntimeEnvInfo,
)

deprecated = private_utils.deprecated(
    "If you need to use this function, open a feature request issue on GitHub.",
    removal_release="1.4",
    warn_once=True,
)

get_system_memory = deprecated(private_utils.get_system_memory)


def get_runtime_env_info(
    runtime_env: RuntimeEnv,
    *,
    is_job_runtime_env: bool = False,
    serialize: bool = False,
):
    """Create runtime env info from runtime env.

    In the user interface, the argument `runtime_env` contains some fields
    which not contained in `ProtoRuntimeEnv` but in `ProtoRuntimeEnvInfo`,
    such as `eager_install`. This function will extract those fields from
    `RuntimeEnv` and create a new `ProtoRuntimeEnvInfo`, and serialize it.
    """
    proto_runtime_env_info = ProtoRuntimeEnvInfo()

    proto_runtime_env_info.uris[:] = runtime_env.get_uris()

    # Normally, `RuntimeEnv` should guarantee the accuracy of field eager_install,
    # but so far, the internal code has not completely prohibited direct
    # modification of fields in RuntimeEnv, so we should check it for insurance.
    # TODO(Catch-Bull): overload `__setitem__` for `RuntimeEnv`, change the
    # runtime_env of all internal code from dict to RuntimeEnv.

    eager_install = runtime_env.get("eager_install")
    if is_job_runtime_env or eager_install is not None:
        if eager_install is None:
            eager_install = True
        elif not isinstance(eager_install, bool):
            raise TypeError(
                f"eager_install must be a boolean. got {type(eager_install)}"
            )
        proto_runtime_env_info.runtime_env_eager_install = eager_install

    runtime_env_config = runtime_env.get("config")
    if runtime_env_config is None:
        runtime_env_config = RuntimeEnvConfig.default_config()
    else:
        runtime_env_config = RuntimeEnvConfig.parse_and_validate_runtime_env_config(
            runtime_env_config
        )

    proto_runtime_env_info.runtime_env_config.CopyFrom(
        runtime_env_config.build_proto_runtime_env_config()
    )

    proto_runtime_env_info.serialized_runtime_env = runtime_env.serialize()

    if not serialize:
        return proto_runtime_env_info

    return json_format.MessageToJson(proto_runtime_env_info)


def parse_runtime_env(runtime_env: Optional[Union[Dict, RuntimeEnv]]):
    # Parse local pip/conda config files here. If we instead did it in
    # .remote(), it would get run in the Ray Client server, which runs on
    # a remote node where the files aren't available.
    if runtime_env:
        if isinstance(runtime_env, dict):
            return RuntimeEnv(**(runtime_env or {}))
        raise TypeError(
            "runtime_env must be dict or RuntimeEnv, ",
            f"but got: {type(runtime_env)}",
        )
    else:
        # Keep the new_runtime_env as None.  In .remote(), we need to know
        # if runtime_env is None to know whether or not to fall back to the
        # runtime_env specified in the @ray.remote decorator.
        return None
