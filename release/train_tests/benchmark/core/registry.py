"""Maps an experiment's ``model.adapter`` string to a FrameworkAdapter class.

Lazy imports keep heavy framework deps (deepspeed, torch, transformers) out of
the driver process until the worker actually needs them, so the dispatcher and
config validation stay importable anywhere.
"""

from typing import Type

from frameworks.base_adapter import FrameworkAdapter


def get_adapter_cls(adapter_name: str) -> Type[FrameworkAdapter]:
    if adapter_name == "deepspeed":
        from frameworks.deepspeed.adapter import DeepSpeedAdapter

        return DeepSpeedAdapter
    # Future adapters slot in here:
    #   "torchtitan" -> frameworks.torchtitan.adapter.TorchTitanAdapter
    #   "maxtext"    -> frameworks.maxtext.adapter.MaxTextAdapter
    #   "image_classification" / "recsys" -> legacy BenchmarkFactory wrappers
    raise ValueError(
        f"Unknown adapter '{adapter_name}'. Registered: ['deepspeed']."
    )
