class AnytensorError(Exception):
    """Base class for Anytensor-specific exceptions."""

    pass


class NotFoundError(AnytensorError):
    pass
