import logging
import os
import ray
import time
from enum import Enum

from ray.actor import ActorHandle
from ray.streaming.generated import remote_call_pb2
from ray.streaming.runtime.command\
    import WorkerCommitReport, WorkerRollbackRequest

logger = logging.getLogger(__name__)


class CallResult:
    """
    Call Result
    """

    def __init__(self, success, result_code, result_msg, result_obj):
        self.success = success
        self.result_code = result_code
        self.result_msg = result_msg
        self.result_obj = result_obj

    @staticmethod
    def success(payload=None):
        return CallResult(True, CallResultEnum.SUCCESS, None, payload)

    @staticmethod
    def fail(payload=None):
        return CallResult(False, CallResultEnum.FAILED, None, payload)

    @staticmethod
    def skipped(msg=None):
        return CallResult(True, CallResultEnum.SKIPPED, msg, None)

    def is_success(self):
        if self.result_code is CallResultEnum.SUCCESS:
            return True

        return False


class CallResultEnum(Enum):
    """
    call result enum
    """

    SUCCESS = 0
    FAILED = 1
    SKIPPED = 2


class RemoteCallMst:
    """
    remote call job master
    """

    @staticmethod
    def request_job_worker_rollback(master: ActorHandle,
                                    request: WorkerRollbackRequest):
        logger.info("Remote call mst: request job worker rollback start.")
        request_pb = remote_call_pb2.BaseWorkerCmd()
        request_pb.actor_id = request.from_actor_id
        request_pb.timestamp = int(time.time() * 1000.0)
        rollback_request_pb = remote_call_pb2.WorkerRollbackRequest()
        rollback_request_pb.exception_msg = request.exception_msg()
        rollback_request_pb.worker_hostname = os.uname()[1]
        rollback_request_pb.worker_pid = str(os.getpid())
        request_pb.detail.Pack(rollback_request_pb)
        return_ids = master.requestJobWorkerRollback\
            .remote(request_pb.SerializeToString())
        result = remote_call_pb2.BoolResult()
        result.ParseFromString(ray.get(return_ids))
        logger.info("Remote call mst: request job worker rollback finish.")
        return result.boolRes

    @staticmethod
    def report_job_worker_commit(master: ActorHandle,
                                 report: WorkerCommitReport):
        logger.info("Remote call mst: report job worker commit start.")
        report_pb = remote_call_pb2.BaseWorkerCmd()

        report_pb.actor_id = report.from_actor_id
        report_pb.timestamp = int(time.time() * 1000.0)
        wk_commit = remote_call_pb2.WorkerCommitReport()
        wk_commit.commit_checkpoint_id = report.commit_checkpoint_id
        report_pb.detail.Pack(wk_commit)
        return_id = master.reportJobWorkerCommit\
            .remote(report_pb.SerializeToString())
        result = remote_call_pb2.BoolResult()
        result.ParseFromString(ray.get(return_id))
        logger.info("Remote call mst: report job worker commit finish.")
        return result.boolRes
