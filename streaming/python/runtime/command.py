class BaseWorkerCmd:
    """
    base worker cmd
    """

    def __init__(self, actor_id):
        self.from_actor_id = actor_id


class WorkerCommitReport(BaseWorkerCmd):
    """
    worker commit report
    """

    def __init__(self, actor_id, commit_checkpoint_id):
        super().__init__(actor_id)
        self.commit_checkpoint_id = commit_checkpoint_id


class WorkerRollbackRequest(BaseWorkerCmd):
    """
    worker rollback request
    """

    def __init__(self, actor_id, exception_msg):
        super().__init__(actor_id)
        self.__exception_msg = exception_msg

    def exception_msg(self):
        return self.__exception_msg


class WorkerReceivePartialBarrierReport(BaseWorkerCmd):
    """
    worker receive partial barrier report
    """

    def __init__(self, actor_id, global_checkpoint_id, partial_checkpoint_id):
        super().__init__(actor_id)
        self.global_checkpoint_id = global_checkpoint_id
        self.partial_checkpoint_id = partial_checkpoint_id
