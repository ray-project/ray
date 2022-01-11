from ray.rllib.agents.marwil.marwil import MARWILTrainer, \
    DEFAULT_CONFIG as MARWIL_CONFIG
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict

# yapf: disable
# __sphinx_doc_begin__
BC_DEFAULT_CONFIG = MARWILTrainer.merge_trainer_configs(
    MARWIL_CONFIG, {
        # No need to calculate advantages (or do anything else with the
        # rewards).
        "beta": 0.0,
        # Advantages (calculated during postprocessing) not important for
        # behavioral cloning.
        "postprocess_inputs": False,
        # No reward estimation.
        "input_evaluation": [],
    })
# __sphinx_doc_end__
# yapf: enable


class BCTrainer(MARWILTrainer):
    """Behavioral Cloning (derived from MARWIL).

    Simply uses the MARWIL agent with beta force-set to 0.0.
    """

    @classmethod
    @override(MARWILTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return BC_DEFAULT_CONFIG

    @override(MARWILTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["beta"] != 0.0:
            raise ValueError(
                "For behavioral cloning, `beta` parameter must be 0.0!")
