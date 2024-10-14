import asyncio
import sys

def get_or_create_event_loop() -> asyncio.BaseEventLoop:

    vers_info = sys.version_info
    if vers_info.major >= 3 and vers_info.minor >= 10:
        # This follows the implementation of the deprecating `get_event_loop`
        # in python3.10's asyncio. See python3.10/asyncio/events.py
        # _get_event_loop()
        loop = None
        try:
            loop = asyncio.get_running_loop()
            assert loop is not None
            return loop
        except RuntimeError as e:
            # No running loop, relying on the error message as for now to
            # differentiate runtime errors.
            assert "no running event loop" in str(e)
            return asyncio.get_event_loop_policy().get_event_loop()

    return asyncio.get_event_loop()
