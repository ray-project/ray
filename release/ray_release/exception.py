class ReleaseTestPackageError(RuntimeError):
    pass


class ReleaseTestConfigError(ReleaseTestPackageError):
    pass


class ReleaseTestCLIError(ReleaseTestPackageError):
    pass


class RayWheelsError(RuntimeError):
    pass


class RayWheelsUnspecifiedError(RayWheelsError):
    pass


class RayWheelsNotFoundError(RayWheelsError):
    pass


class RayWheelsTimeoutError(RayWheelsError):
    pass


class ClusterManagerError(RuntimeError):
    pass


class ClusterEnvBuildError(ClusterManagerError):
    pass


class ClusterEnvBuildTimeout(ClusterManagerError):
    pass


class ClusterComputeBuildError(ClusterManagerError):
    pass


class ClusterCreationError(ClusterManagerError):
    pass


class ClusterStartupError(ClusterManagerError):
    pass


class ClusterStartupTimeout(ClusterManagerError):
    pass


class ClusterStartupFailed(ClusterManagerError):
    pass


class CommandTimeout(RuntimeError):
    pass


class PrepareCommandTimeout(CommandTimeout):
    pass


class TestCommandTimeout(CommandTimeout):
    pass


class CommandError(RuntimeError):
    pass


class PrepareCommandError(CommandError):
    pass


class TestCommandError(CommandError):
    pass


class LogsError(CommandError):
    pass


class ResultsError(CommandError):
    pass


class ResultsAlert(CommandError):
    pass


# ---


class PrepareCommandRuntimeError(RuntimeError):
    pass


class ReleaseTestRuntimeError(RuntimeError):
    pass


class ReleaseTestInfraError(ReleaseTestRuntimeError):
    pass


class ReleaseTestTimeoutError(ReleaseTestRuntimeError):
    pass


class SessionTimeoutError(ReleaseTestTimeoutError):
    pass


class FileSyncTimeoutError(ReleaseTestTimeoutError):
    pass


class CommandTimeoutError(ReleaseTestTimeoutError):
    pass


class PrepareCommandTimeoutError(ReleaseTestTimeoutError):
    pass


# e.g., App config failure.
class AppConfigBuildFailure(RuntimeError):
    pass
