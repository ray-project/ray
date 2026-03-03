import sys
from dataclasses import dataclass, field


@dataclass
class TorchCompileConfig:
    """Configuration options for RLlib's usage of torch.compile in RLModules.

    # On `torch.compile` in Torch RLModules
    `torch.compile` invokes torch's dynamo JIT compiler that can potentially bring
    speedups to RL Module's forward methods.
    This is a performance optimization that should be disabled for debugging.

    General usage:
    - Usually, you only want to `RLModule._forward_train` to be compiled on
      instances of RLModule used for learning. (e.g. the learner)
    - In some cases, it can bring speedups to also compile
      `RLModule._forward_exploration` on instances used for exploration. (e.g.
      RolloutWorker)
    - In some cases, it can bring speedups to also compile
      `RLModule._forward_inference` on instances used for inference. (e.g.
      RolloutWorker)

    Note that different backends are available on different platforms.
    Also note that the default backend for torch dynamo is "aot_eager" on macOS.
    This is a debugging backend that is expected not to improve performance because
    the inductor backend is not supported on OSX so far.

    Args:
        torch_dynamo_backend: The torch.dynamo backend to use.
        torch_dynamo_mode: The torch.dynamo mode to use.
        kwargs: Additional keyword arguments to pass to `torch.compile()`
    """

    torch_dynamo_backend: str = (
        "aot_eager" if sys.platform == "darwin" else "cudagraphs"
    )
    torch_dynamo_mode: str = None
    kwargs: dict = field(default_factory=lambda: dict())
