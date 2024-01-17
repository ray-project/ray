"""
Data types of the HTTP interface.
"""


class ResultOrException:
    """
    Returned by client `ray.get`.
    Both result and exc is serialized in bytes.

    Invariant: If result is not None, exc can't be None.
    """

    def __init__(self) -> None:
        self.result = None
        self.exc = None
