# NOTE: We provide these as aliases to maintain compatibility with older version
#       of Arrow `PyExtensionType` that relies on picked class references that
#       reference `ray.air.util.tensor_extensions.arrow.*` classes

from ray.data._internal.tensor_extensions.arrow import (
    ArrowTensorType,  # noqa: F401
    ArrowTensorTypeV2,  # noqa: F401
    ArrowVariableShapedTensorType,  # noqa: F401
)
