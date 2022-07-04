# This file defines the C++ symbols that need to be exported (aka ABI, application binary interface).
# These symbols will be used by other libraries.
# Note: This file is used for macOS only, and should be kept in sync with `ray_api_version_script.lds`.
# Ray ABI is not finalized, the exact set of exported (C/C++) APIs is subject to change.
# common
*[0-9]ray[0-9]*;
TaskExecutionHandler;
GetFunctionManager;
GetRemoteFunctions;
InitRayRuntime;
