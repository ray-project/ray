import os

# Default Timeout
INITIALIZE_ENV_TIMEOUT_SECONDS = 60 * 10  # 10 minutes
INITIALIZE_ENV_TIMEOUT_SECONDS_LIMIT = 60 * 60  # 1 hour
# Job agent consts
JOB_RETRY_INTERVAL_SECONDS = 5
JOB_RETRY_TIMES = 3
JOB_DIR = "{temp_dir}/job/{job_id}/"
JOB_DRIVER_ENTRY_FILE = os.path.join(JOB_DIR, "driver-{uuid}.py")
JOB_MARK_ENVIRON_READY_FILE = os.path.join(JOB_DIR, ".environ")
# Downloader constants
DOWNLOAD_RESOURCE_BUFFER_SIZE = 10 * 1024 * 1024  # 10MB
DOWNLOAD_PACKAGE = os.path.join(JOB_DIR, "package.zip")
DOWNLOAD_PACKAGE_UNZIP_DIR = os.path.join(JOB_DIR, "package")
# Python package constants
PYTHON_PIP_CACHE = "{temp_dir}/pipcache"
PYTHON_PACKAGE_INDEX = ""
PYTHON_VIRTUAL_ENV_DIR = os.path.join(JOB_DIR, "pyenv")
PYTHON_VIRTUAL_ENV_CACHE_DIR = os.path.join(JOB_DIR, "virtualenv_cache")
PYTHON_REQUIREMENTS_FILE = os.path.join(JOB_DIR, "requirements.txt")
# Java package constants
JAVA_SHARED_LIBRARY_DIR = "{temp_dir}/shared_java_lib"
# Redis key
REDIS_KEY_JOB_COUNTER = "JobCounter"
JOB_CHANNEL = "JOB"
RETRY_GET_ALL_JOB_INFO_INTERVAL_SECONDS = 2
