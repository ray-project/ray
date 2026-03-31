from typing import List, Literal, Optional

from pydantic import ConfigDict, Field, model_validator

from ray.llm._internal.common.base_pydantic import BaseModelExtended


class BundleConfig(BaseModelExtended):
    """Configuration for a single placement group bundle.

    Resource counts are floats to align with Ray's internal resource
    representation, which supports fractional values (e.g. GPU=0.5).
    Both CPU and GPU default to 0.0 — the schema does not inject hidden
    resource requests. Extra fields are allowed for custom Ray resources (e.g. TPU,
    accelerator_type:L4).
    """

    model_config = ConfigDict(extra="allow")

    CPU: float = Field(
        default=0.0,
        ge=0.0,
        description="The number of CPUs per bundle.",
    )
    GPU: float = Field(
        default=0.0,
        ge=0.0,
        description="The number of GPUs per bundle.",
    )

    @model_validator(mode="after")
    def validate_extra_resources(self):
        """Ensure custom resources (TPU, accelerator_type, etc.) are non-negative."""
        extra_resources = self.__pydantic_extra__
        if not extra_resources:
            return self
        for key, value in extra_resources.items():
            if not isinstance(value, (int, float)):
                raise ValueError(
                    f"Resource '{key}' must be a number, got {type(value).__name__}"
                )
            if value < 0:
                raise ValueError(f"Resource '{key}' must be non-negative, got {value}")
        return self


class PlacementGroupConfig(BaseModelExtended):
    """Configuration for placement group."""

    bundle_per_worker: Optional[BundleConfig] = Field(
        default=None,
        description=(
            "Resource bundle specification for each worker. "
            "Auto-replicated based on tensor_parallel_size * pipeline_parallel_size. "
            "Cannot be used together with 'bundles'."
        ),
    )
    bundles: Optional[List[BundleConfig]] = Field(
        default=None, description="List of resource bundles"
    )
    strategy: Literal["PACK", "SPREAD", "STRICT_PACK", "STRICT_SPREAD"] = Field(
        default="PACK", description="Placement group strategy"
    )

    @model_validator(mode="after")
    def validate_bundle_options(self):
        if self.bundle_per_worker is not None and self.bundles is not None:
            raise ValueError(
                "Cannot specify both 'bundle_per_worker' and 'bundles' in "
                "placement_group_config. Use 'bundle_per_worker' for simple "
                "per-worker resource specification (auto-replicated by tp*pp), "
                "or 'bundles' for full control."
            )
        if self.bundle_per_worker is None and self.bundles is None:
            raise ValueError(
                "placement_group_config must specify either 'bundle_per_worker' "
                "or 'bundles'."
            )
        return self
