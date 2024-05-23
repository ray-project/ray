class MockClass:
    """
    This class is used for testing purpose only. It should not be used in production.
    """

    _attribute = None


def mock_function():
    """
    This function is used for testing purpose only. It should not be used in production.
    """
    pass


mock_function._attribute = None
