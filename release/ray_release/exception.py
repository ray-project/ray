import enum


class ExitCode(enum.Enum):
    SUCCESS = 0  # Do not set/return this manually
    UNCAUGHT = 1  # Do not set/return this manually

    UNSPECIFIED = 2
    UNKNOWN = 3

    # Hard infra errors (non-retryable)
    CLI_ERROR = 10
    CONFIG_ERROR = 11
    SETUP_ERROR = 12
    CLUSTER_RESOURCE_ERROR = 13
    CLUSTER_ENV_BUILD_ERROR = 14
    CLUSTER_STARTUP_ERROR = 15
    # LOCAL_ENV_SETUP_ERROR = 16   # not used anymore
    # REMOTE_ENV_SETUP_ERROR = 17  # not used anymore
    FETCH_RESULT_ERROR = 18
    ANYSCALE_ERROR = 19

    # Infra timeouts (retryable)
    RAY_WHEELS_TIMEOUT = 30
    CLUSTER_ENV_BUILD_TIMEOUT = 31
    CLUSTER_STARTUP_TIMEOUT = 32
    CLUSTER_WAIT_TIMEOUT = 33

    # Command errors - these are considered application errors
    COMMAND_ERROR = 40
    COMMAND_ALERT = 41
    COMMAND_TIMEOUT = 42
    PREPARE_ERROR = 43


class ReleaseTestError(RuntimeError):
    exit_code = ExitCode.UNSPECIFIED


class ReleaseTestPackageError(ReleaseTestError):
    pass


class ReleaseTestConfigError(ReleaseTestPackageError):
    exit_code = ExitCode.CONFIG_ERROR


class ReleaseTestCLIError(ReleaseTestPackageError):
    exit_code = ExitCode.CLI_ERROR


class ReleaseTestSetupError(ReleaseTestPackageError):
    exit_code = ExitCode.SETUP_ERROR


class ClusterManagerError(ReleaseTestError):
    exit_code = ExitCode.CLUSTER_RESOURCE_ERROR


class ClusterEnvCreateError(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_RESOURCE_ERROR


class ClusterEnvBuildError(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_ENV_BUILD_ERROR


class ClusterEnvBuildTimeout(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_ENV_BUILD_TIMEOUT


class ClusterComputeCreateError(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_RESOURCE_ERROR


class CloudInfoError(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_RESOURCE_ERROR


class ClusterStartupTimeout(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_STARTUP_TIMEOUT


class ClusterStartupFailed(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_STARTUP_ERROR


class FileManagerError(ReleaseTestError):
    pass


class FileUploadError(FileManagerError):
    pass


class CommandTimeout(ReleaseTestError):
    exit_code = ExitCode.COMMAND_TIMEOUT


class PrepareCommandTimeout(CommandTimeout):
    exit_code = ExitCode.CLUSTER_WAIT_TIMEOUT


class TestCommandTimeout(CommandTimeout):
    exit_code = ExitCode.COMMAND_TIMEOUT


class CommandError(ReleaseTestError):
    exit_code = ExitCode.COMMAND_ERROR


class PrepareCommandError(CommandError):
    exit_code = ExitCode.PREPARE_ERROR


class TestCommandError(CommandError):
    exit_code = ExitCode.COMMAND_ERROR


class FetchResultError(FileManagerError):
    exit_code = ExitCode.FETCH_RESULT_ERROR


class LogsError(CommandError):
    pass


class ResultsAlert(CommandError):
    exit_code = ExitCode.COMMAND_ALERT


class JobBrokenError(ReleaseTestError):
    exit_code = ExitCode.ANYSCALE_ERROR


class JobTerminatedBeforeStartError(ReleaseTestError):
    exit_code = ExitCode.CLUSTER_STARTUP_TIMEOUT


class JobTerminatedError(ReleaseTestError):
    exit_code = ExitCode.ANYSCALE_ERROR


class JobOutOfRetriesError(ReleaseTestError):
    exit_code = ExitCode.ANYSCALE_ERROR


class JobStartupFailed(ClusterStartupFailed):
    pass


class JobStartupTimeout(ClusterStartupTimeout):
    pass


class JobNoLogsError(ReleaseTestError):
    exit_code = ExitCode.ANYSCALE_ERROR
