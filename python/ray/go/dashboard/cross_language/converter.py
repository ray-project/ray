"""Recursive conversion of Python objects into msgpack-native types.

pydantic v1 ``.dict()`` recursively converts nested BaseModel/list/dict but
leaves str-Enum members as Enum objects (e.g.
``TrainRun.dict()["status"]`` is ``<RunStatus.RUNNING: 'RUNNING'>``), and the
serve instance details dict likewise embeds ``ApplicationStatus`` /
``APIType`` enums. The msgpack ``strict_types``
serializer rejects Enum instances, which forces the Python serializer fallback
that the Go cross-language deserializer cannot read ("msgpack: unknown ext
id=101"). This module recursively reduces any value to msgpack-native types
(dict/list/str/int/float/bool/bytes/None) by converting every Enum to its
value and every pydantic BaseModel to its ``.dict()``.
"""

import enum

from ray._common.pydantic_compat import BaseModel


def parse_serve_deploy_schema(config: dict):
    """Validate and construct a ServeDeploySchema from the Go-passed dict.

    Pure function (no actor state, no ray.* dependency) so the controller can
    call it locally and tests can exercise it directly. The pydantic parse_obj
    performs the full validation the Python ServeHead applies (applications
    required, non-empty names, unique routes, etc.); a ValidationError propagates
    to the caller, mirroring the Python dashboard's 400 behavior.

    The serve imports are deliberately local: this module must stay importable
    (and to_msgpack_native usable by the train state actors) on clusters
    without the ray[serve] extra installed, where starlette is absent.
    """
    from ray.serve.schema import ServeDeploySchema

    return ServeDeploySchema.parse_obj(config)


def parse_deployment_id(deployment_id: dict):
    """Construct a DeploymentID from the Go-passed dict.

    Accepts ``{"name": ..., "app_name": ...}``; app_name defaults to
    SERVE_DEFAULT_APP_NAME when absent, matching the wrapper's previous
    behavior and DeploymentID's own default.

    The serve imports are deliberately local; see parse_serve_deploy_schema.
    """
    from ray.serve._private.common import DeploymentID
    from ray.serve._private.constants import SERVE_DEFAULT_APP_NAME

    return DeploymentID(
        name=deployment_id["name"],
        app_name=deployment_id.get("app_name", SERVE_DEFAULT_APP_NAME),
    )


def to_msgpack_native(obj):
    """Recursively convert *obj* to msgpack-native types (dict/list/str/int/
    float/bool/bytes/None). Anything else (Enum/BaseModel/custom object) is
    reduced: Enum -> its value, BaseModel -> its ``.dict()``, tuple/set ->
    list.
    """
    if isinstance(obj, enum.Enum):
        value = obj.value
        # Prefer the enum value when it is already msgpack-native; otherwise
        # (e.g. an enum whose value is itself a complex object) fall back to
        # str() so the field still serializes as a string.
        if value is None or isinstance(value, (bool, int, float, str, bytes)):
            return value
        return str(obj)
    if BaseModel is not None and isinstance(obj, BaseModel):
        return to_msgpack_native(obj.dict())
    if isinstance(obj, dict):
        return {k: to_msgpack_native(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple, set)):
        return [to_msgpack_native(v) for v in obj]
    return obj
