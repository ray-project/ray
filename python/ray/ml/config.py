from typing import Dict, Any

# Right now, RunConfig is just an arbitrary dict that specifies tune.run
# kwargs.
# TODO(xwjiang): After Tuner is implemented, define the schema
RunConfig = Dict[str, Any]

ScalingConfig = Dict[str, Any]
