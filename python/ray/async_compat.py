"""
This file should only be imported from Python 3.
It will raise SyntaxError when importing from Python 2.
"""


def sync_to_async(func):
    """Convert a blocking function to async function"""

    async def wrapper(*args, **kwargs):
        return func(*args, **kwargs)

    return wrapper
