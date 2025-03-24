from collections import OrderedDict
from typing import Optional, List, Type, Callable, Dict

from pydantic import Field

from ray.data.block import UserDefinedFunction
from ray.data import Dataset
from ray.util.annotations import PublicAPI, DeveloperAPI

from ray.llm._internal.batch.stages import (
    StatefulStage,
    wrap_preprocess,
    wrap_postprocess,
)
from ray.llm._internal.common.base_pydantic import BaseModelExtended


class ProcessorConfig(BaseModelExtended):
    """The processor configuration."""

    batch_size: int = Field(
        default=64,
        description="Large batch sizes are likely to saturate the compute resources "
        "and could achieve higher throughput. On the other hand, small batch sizes "
        "are more fault-tolerant and could reduce bubbles in the data pipeline. "
        "You can tune the batch size to balance the throughput and fault-tolerance "
        "based on your use case. Defaults to 64.",
    )
    accelerator_type: Optional[str] = Field(
        default=None,
        description="The accelerator type used by the LLM stage in a processor. "
        "Default to None, meaning that only the CPU will be used.",
    )
    concurrency: int = Field(
        default=1,
        description="The number of workers for data parallelism. Default to 1.",
    )

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


@PublicAPI(stability="alpha")
class Processor:
    """A processor is composed of a preprocess stage, followed by one or more
    processing stages, and finally a postprocess stage. We use processor as a
    paradigm for processing data using LLMs.

    Args:
        config: The processor config.
        preprocess: An optional lambda function that takes a row (dict) as input
            and returns a preprocessed row (dict). The output row must contain the
            required fields for the following processing stages.
        postprocess: An optional lambda function that takes a row (dict) as input
            and returns a postprocessed row (dict).
    """

    # The internal used data column name ("__data"). Your input
    # dataset should not contain this column. If you want to use this column
    # in your input dataset, you have to derive and customize Processor.
    DATA_COLUMN: str = "__data"

    def __init__(
        self,
        config: ProcessorConfig,
        stages: List[StatefulStage],
        preprocess: Optional[UserDefinedFunction] = None,
        postprocess: Optional[UserDefinedFunction] = None,
    ):
        self.config = config
        self.preprocess = None
        self.postprocess = None
        self.stages: OrderedDict[str, StatefulStage] = OrderedDict()

        # NOTE (Kourosh): If pre/postprocess is not provided, use the identity function.
        # Wrapping is required even if they are identity functions, b/c data_column
        # gets inserted/removed via wrap_preprocess/wrap_postprocess.
        preprocess = preprocess or (lambda row: row)
        postprocess = postprocess or (lambda row: row)

        self.preprocess = wrap_preprocess(
            preprocess,
            self.DATA_COLUMN,
        )

        self.postprocess = wrap_postprocess(
            postprocess,
            self.DATA_COLUMN,
        )

        for stage in stages:
            self._append_stage(stage)

    def __call__(self, dataset: Dataset) -> Dataset:
        """Execute the processor:
        preprocess -> stages -> postprocess.
        Note that the dataset won't be materialized during the execution.

        Args:
            dataset: The input dataset.

        Returns:
            The output dataset.
        """
        if self.preprocess is not None:
            dataset = dataset.map(self.preprocess)

        # Apply stages.
        for stage in self.stages.values():
            kwargs = stage.get_dataset_map_batches_kwargs(
                batch_size=self.config.batch_size,
                data_column=self.DATA_COLUMN,
            )
            dataset = dataset.map_batches(stage.fn, **kwargs)

        if self.postprocess is not None:
            dataset = dataset.map(self.postprocess)
        return dataset

    def _append_stage(self, stage: StatefulStage) -> None:
        """Append a stage before postprocess. The stage class name will be used as
        the stage name. If there are multiple stages with the same type, a suffix
        will be added to the stage name to avoid conflicts.

        Args:
            stage: The stage to append.
        """
        stage_name = type(stage).__name__

        # When a processor has multiple stages with the same type,
        # append a index suffix to the stage name to avoid conflicts.
        if stage_name in self.stages:
            num_same_type_stage = len([s for s in self.stages.values() if s is stage])
            stage_name = f"{stage_name}_{num_same_type_stage + 1}"
        self.stages[stage_name] = stage

    def list_stage_names(self) -> List[str]:
        """List the stage names of this processor in order. Preprocess and postprocess
        are not included.

        Returns:
            A list of stage names.
        """
        return list(self.stages.keys())

    def get_stage_by_name(self, name: str) -> StatefulStage:
        """Get a particular stage by its name. If the stage is not found,
        a ValueError will be raised.

        Args:
            name: The stage name.

        Returns:
            The pipeline stage.
        """
        if name in self.stages:
            return self.stages[name]
        raise ValueError(f"Stage {name} not found")


@DeveloperAPI
class ProcessorBuilder:
    """Build a processor based on the configuration."""

    _registry: Dict[str, Callable] = {}

    @classmethod
    def register(cls, config_type: Type[ProcessorConfig], builder: Callable) -> None:
        """A decorator to assoicate a particular pipeline config
        with its build function.
        """
        type_name = config_type.__name__
        if type_name in cls._registry:
            raise ValueError(f"Processor config type {type_name} already registered.")
        cls._registry[type_name] = builder

    @classmethod
    def build(
        cls,
        config: ProcessorConfig,
        override_stage_config_fn: Optional[Callable] = None,
        **kwargs,
    ) -> Processor:
        """Build a processor.

        Args:
            config: The processor config.
            override_stage_config_fn: Custom stages configurations.

        Returns:
            The built processor.
        """
        type_name = type(config).__name__
        if type_name not in cls._registry:
            raise ValueError(
                f"Processor config type {type_name} not registered. "
                f"Available types: {cls._registry.keys()}"
            )
        processor = cls._registry[type_name](config, **kwargs)
        if override_stage_config_fn is not None:
            for name, stage in processor.stages.items():
                override_stage_config_fn(name, stage)
        return processor
