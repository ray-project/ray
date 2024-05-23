class MockClass:
    """
    This class is used for testing purpose only. It should not be used in production.
    """

    _annotated = None
    _annotated_type = "PublicAPI"


def mock_function():
    """
    This function is used for testing purpose only. It should not be used in production.
    """
    pass


mock_function._annotated = None
mock_function._annotated_type = "Deprecated"
