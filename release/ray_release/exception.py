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
