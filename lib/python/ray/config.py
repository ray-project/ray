import datetime
import os.path
import sys
import tempfile

def get_log_file_path(name):
  return os.path.join(
    os.path.join("/tmp" if sys.platform.startswith("darwin") else tempfile.gettempdir(), "raylogs"),
    ("{:%Y-%m-%d-%H-%M-%S}-{}").format(datetime.datetime.now(), name.replace(":", "-")))
