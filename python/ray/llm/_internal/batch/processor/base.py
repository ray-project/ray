from collections import OrderedDict
from typing import Optional, List, Type, Callable, Dict

from pydantic import BaseModel

from ray.data.block import UserDefinedFunction
from ray.data import Dataset
from ray.util.annotations import DeveloperAPI

from ray.llm._internal.batch.stages import (
    StatefulStage,
    wrap_preprocess,
    wrap_postprocess,
)


@DeveloperAPI
class ProcessorConfig(BaseModel):
    """The processor configuration."""

    # Whether to carry over input columns.
    carry_over: bool = True
    # Control the fault tolerance granularity.
    batch_size: int = 64

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True


@DeveloperAPI
class Processor:
    """The processor.

    Args:
        config: The processor config.
        preprocess_fn: Preprocess inputs to fit the processor inputs.
        postprocess_fn: Postprocess outputs from the processor.
        accelerator_type: The accelerator type.
        concurrency: The number of concurrent requests.
    """

    # The reserved input/output column names. Usually we don't need to
    # change this them, but if your dataset really needs to use these
    # names and results in conflicts, you should inherit the processor
    # to customize them.
    input_column: str = "__inputs"
    output_column: str = "__outputs"

    def __init__(
        self,
        config: ProcessorConfig,
        preprocess: Optional[UserDefinedFunction] = None,
        postprocess: Optional[UserDefinedFunction] = None,
        accelerator_type: Optional[str] = None,
        concurrency: int = 1,
    ):
        self.config = config
        self.preprocess = None
        self.postprocess = None
        self.accelerator_type = accelerator_type
        self.concurrency = concurrency
        self.stages: OrderedDict[str, StatefulStage] = OrderedDict()

        if preprocess is not None:
            self.preprocess = wrap_preprocess(
                preprocess,
                self.input_column,
                self.config.carry_over,
            )

        if postprocess is not None:
            self.postprocess = wrap_postprocess(
                postprocess,
                self.input_column,
                self.config.carry_over,
            )

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

        for idx, (stage_name, stage) in enumerate(self.stages.items()):
            # Prepare .map_batches() arguments.
            kwargs = stage.map_batches_kwargs.copy()
            kwargs["batch_size"] = self.config.batch_size
            kwargs.update({"fn_constructor_kwargs": stage.fn_constructor_kwargs})
            kwargs["fn_constructor_kwargs"].update(
                dict(
                    input_column=self.input_column,
                    output_column=self.output_column,
                    carry_over=self.config.carry_over,
                )
            )

            # Apply the stage.
            dataset = dataset.map_batches(stage.fn, **kwargs)

            # Rename the output column to the input column to chain the stages.
            dataset = dataset.rename_columns({self.output_column: self.input_column})

        if self.postprocess is not None:
            dataset = dataset.map(self.postprocess)
        return dataset

    def append_stage(self, stage: StatefulStage):
        """Append a stage before postprocess.

        Args:
            stage: The stage to append.
        """
        stage_name = type(stage).__name__

        # When a processor has multiple stages with the same type,
        # append a index suffix to the stage name to avoid conflicts.
        if stage_name in self.stages:
            num_same_type_stage = len(
                [s for s in self.stages.values() if type(s) == type(stage)]
            )
            stage_name = f"{stage_name}_{num_same_type_stage + 1}"
        self.stages[stage_name] = stage

    def list_stage_names(self) -> List[str]:
        """List the stage names of this processor in order.

        Returns:
            A list of stage names.
        """
        return list(self.stages.keys())

    def get_stage_by_name(self, name: str) -> StatefulStage:
        """Get a particular stage by its name.

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

    _registry: Dict[Type[ProcessorConfig], Callable] = {}

    @classmethod
    def register(cls, config_type: Type[ProcessorConfig], builder: Callable):
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
