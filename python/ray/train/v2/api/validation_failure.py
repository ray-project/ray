from dataclasses import dataclass

import ray
from ray.train.v2.api.exceptions import ValidationFailedError


@dataclass
class ValidationFailure:
    checkpoint: "ray.train.Checkpoint"
    validation_failed_error: ValidationFailedError
