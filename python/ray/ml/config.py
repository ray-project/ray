from typing import Any, Dict


# Right now, RunConfig is just an arbitrary dict that specifies tune.run
# kwargs.
# TODO(xwjiang): After Tuner is implemented, make this into an actual dataclass
RunConfig = Dict[str, Any]

ScalingConfig = Dict[str, Any]
