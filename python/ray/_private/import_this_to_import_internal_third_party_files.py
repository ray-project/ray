# Anyone importing this file addes the internal third party files vendored in the Ray pip package.
# Should only be used by the Dashboard and the Runtime Env Agent.
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
