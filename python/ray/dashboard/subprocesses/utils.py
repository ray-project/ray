import asyncio


def assert_not_in_asyncio_loop():
    try:
        asyncio.get_running_loop()
        raise AssertionError(
            "This function should not be called from within an asyncio loop"
        )
    except RuntimeError:
        pass
