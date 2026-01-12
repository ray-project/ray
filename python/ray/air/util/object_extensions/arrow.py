# NOTE: We provide these as aliases to maintain compatibility with older version
#       of Arrow `PyExtensionType` that relies on picked class references that
#       reference `ray.air.util.{tensor|object}_extensions.arrow.*` classes

from ray.data._internal.object_extensions.arrow import (
    ArrowPythonObjectType,  # noqa: F401
)
