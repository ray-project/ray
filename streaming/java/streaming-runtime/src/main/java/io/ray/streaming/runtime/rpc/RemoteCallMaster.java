package io.ray.streaming.runtime.rpc;

import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.master.coordinator.command.WorkerCommitReport;
import io.ray.streaming.runtime.master.coordinator.command.WorkerRollbackRequest;

public class RemoteCallMaster {

  public static ObjectRef<byte[]> reportJobWorkerCommitAsync(
      ActorHandle<JobMaster> actor, WorkerCommitReport commitReport) {
    RemoteCall.WorkerCommitReport commit =
        RemoteCall.WorkerCommitReport.newBuilder()
            .setCommitCheckpointId(commitReport.commitCheckpointId)
            .build();
    Any detail = Any.pack(commit);
    RemoteCall.BaseWorkerCmd cmd =
        RemoteCall.BaseWorkerCmd.newBuilder()
            .setActorId(ByteString.copyFrom(commitReport.fromActorId.getBytes()))
            .setTimestamp(System.currentTimeMillis())
            .setDetail(detail)
            .build();

    return actor.task(JobMaster::reportJobWorkerCommit, cmd.toByteArray()).remote();
  }

  public static Boolean requestJobWorkerRollback(
      ActorHandle<JobMaster> actor, WorkerRollbackRequest rollbackRequest) {
    RemoteCall.WorkerRollbackRequest request =
        RemoteCall.WorkerRollbackRequest.newBuilder()
            .setExceptionMsg(rollbackRequest.getRollbackExceptionMsg())
            .setWorkerHostname(rollbackRequest.getHostname())
            .setWorkerPid(rollbackRequest.getPid())
            .build();
    Any detail = Any.pack(request);
    RemoteCall.BaseWorkerCmd cmd =
        RemoteCall.BaseWorkerCmd.newBuilder()
            .setActorId(ByteString.copyFrom(rollbackRequest.fromActorId.getBytes()))
            .setTimestamp(System.currentTimeMillis())
            .setDetail(detail)
            .build();
    ObjectRef<byte[]> ret =
        actor.task(JobMaster::requestJobWorkerRollback, cmd.toByteArray()).remote();
    byte[] res = ret.get();
    return PbResultParser.parseBoolResult(res);
  }
}
