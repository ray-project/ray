"""
[1] Mastering Diverse Domains through World Models - 2023
D. Hafner, J. Pasukonis, J. Ba, T. Lillicrap
https://arxiv.org/pdf/2301.04104v1.pdf

[2] Mastering Atari with Discrete World Models - 2021
D. Hafner, T. Lillicrap, M. Norouzi, J. Ba
https://arxiv.org/pdf/2010.02193.pdf
"""
from dataclasses import dataclass

from ray.rllib.core.learner.learner import LearnerHPs


@dataclass
class DreamerV3Hyperparmameters(LearnerHPs):
    """Hyper-parameters for a DreamerV3Learner.

    Attributes:
        model_dimension: The main switch (given as a string such as "S", "M", or "L")
            for adjusting the overall model size. See [1] (table B) for more
            information. Individual model settings, such as the sizes of individual
            layers can still be overwritten by the user.
        training_ratio: The ratio of replayed steps (used for learning/updating the
            model) over env steps (from the actual environment, not the dreamed one).
    """

    # Main config settings to use for fine-tuning.
    model_dimension: str = "XS"
    training_ratio: float = 1024
