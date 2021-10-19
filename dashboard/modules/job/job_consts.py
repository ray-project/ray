import os
from ray.core.generated import common_pb2

# Job agent consts
# TODO(fyrestone): We should use job id instead of unique_id.
JOB_DIR = "{temp_dir}/job/{unique_id}/"
JOB_UNPACK_DIR = os.path.join(JOB_DIR, "package")
JOB_DRIVER_ENTRY_FILE = os.path.join(JOB_DIR, "driver-{uuid}.py")
# Downloader constants
DOWNLOAD_BUFFER_SIZE = 10 * 1024 * 1024  # 10MB
DOWNLOAD_PACKAGE_FILE = os.path.join(JOB_DIR, "package.zip")
# Redis key
JOB_CHANNEL = "JOB"
RETRY_GET_ALL_JOB_INFO_INTERVAL_SECONDS = 2
# Languages
PYTHON = common_pb2.Language.Name(common_pb2.Language.PYTHON)
JAVA = common_pb2.Language.Name(common_pb2.Language.JAVA)
CPP = common_pb2.Language.Name(common_pb2.Language.CPP)
