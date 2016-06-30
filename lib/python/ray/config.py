import os.path
import sys
import tempfile
LOG_DIRECTORY = os.path.join("/tmp" if sys.platform.startswith("darwin") else tempfile.gettempdir(), "raylogs")
LOG_TIMESTAMP = "{:%Y-%m-%d=%H:%M:%S}"
