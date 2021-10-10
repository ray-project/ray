from pydantic import BaseModel, PositiveInt


class StepConfig(BaseModel):
    num_replicas: PositiveInt
    inline: bool
