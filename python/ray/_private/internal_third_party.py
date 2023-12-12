# Anyone importing this file addes the internal third party files vendored in the Ray
# pip package. Should only be used by the Dashboard and the Runtime Env Agent.

def check_is_internal_process():
    """
    raises ImportError if this import is illegal. The only leagal importers are the Ray internal ones: the Dashboard and the Runtime Env Agent.
    """
    import inspect
    print(inspect.stack())
    # TODO: inspect the right layer (or the bottom layer?) of the stack, and see if the module is legal.
    # Maybe bottom layer is better -> how you started the process at all

def _configure_path():
    import sys
    import os

    sys.path.insert(
        0,
        os.path.join(
            os.path.abspath(os.path.dirname(__file__)), "internal_thirdparty_files"
        ),
    )


_configure_path()
del _configure_path

import aiosignal  # noqa: E402 F401
import aiohttp_cors  # noqa: E402 F401
import aiohttp  # noqa: E402 F401
