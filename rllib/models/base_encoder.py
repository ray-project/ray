import abc


class Encoder:
    """The base class for all encoders.

    Required Attributes:
        hidden_size: int: Returns the hidden size of this model
    """

    @property
    @abc.abtractmethod
    def hidden_size(self) -> int:
        """Return the hidden size of this model"""
