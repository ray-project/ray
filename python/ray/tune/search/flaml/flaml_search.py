try:
    from flaml import BlendSearch, CFO
except ImportError:

    class _DummyErrorRaiser:
        def __init__(self, *args, **kwargs) -> None:
            raise ImportError(
                "FLAML must be installed! "
                "You can install FLAML with the command: "
                "`pip install flaml`."
            )

    BlendSearch = CFO = _DummyErrorRaiser
