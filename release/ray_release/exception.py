from ray_release.result import ExitCode


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


class RayWheelsError(ReleaseTestError):
    exit_code = ExitCode.CLI_ERROR


class RayWheelsUnspecifiedError(RayWheelsError):
    exit_code = ExitCode.CLI_ERROR


class RayWheelsNotFoundError(RayWheelsError):
    exit_code = ExitCode.CLI_ERROR


class RayWheelsTimeoutError(RayWheelsError):
    exit_code = ExitCode.RAY_WHEELS_TIMEOUT


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


class ClusterCreationError(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_RESOURCE_ERROR


class ClusterStartupError(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_STARTUP_ERROR


class CloudInfoError(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_RESOURCE_ERROR


class ClusterStartupTimeout(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_STARTUP_TIMEOUT


class ClusterStartupFailed(ClusterManagerError):
    exit_code = ExitCode.CLUSTER_STARTUP_ERROR


class EnvironmentSetupError(ReleaseTestError):
    exit_code = ExitCode.CLUSTER_STARTUP_ERROR


class LocalEnvSetupError(EnvironmentSetupError):
    exit_code = ExitCode.LOCAL_ENV_SETUP_ERROR


class RemoteEnvSetupError(EnvironmentSetupError):
    exit_code = ExitCode.REMOTE_ENV_SETUP_ERROR


class FileManagerError(ReleaseTestError):
    pass


class FileUploadError(FileManagerError):
    pass


class FileDownloadError(FileManagerError):
    pass


class ClusterNodesWaitTimeout(ReleaseTestError):
    exit_code = ExitCode.CLUSTER_WAIT_TIMEOUT


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


class ResultsError(CommandError):
    pass


class LogsError(CommandError):
    pass


class ResultsAlert(CommandError):
    exit_code = ExitCode.COMMAND_ALERT


class JobBrokenError(ReleaseTestError):
    exit_code = ExitCode.JOB_BROKEN_ERROR


class JobTerminatedError(ReleaseTestError):
    exit_code = ExitCode.CLUSTER_WAIT_TIMEOUT
