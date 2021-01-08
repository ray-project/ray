package io.ray.streaming.runtime.rpc.async;

import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.function.PyActorMethod;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.message.CallResult;
import io.ray.streaming.runtime.rpc.PbResultParser;
import io.ray.streaming.runtime.rpc.async.RemoteCallPool.Callback;
import io.ray.streaming.runtime.rpc.async.RemoteCallPool.ExceptionHandler;
import io.ray.streaming.runtime.transfer.channel.ChannelRecoverInfo;
import io.ray.streaming.runtime.worker.JobWorker;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("unchecked")
public class AsyncRemoteCaller {

  private static final Logger LOG = LoggerFactory.getLogger(AsyncRemoteCaller.class);
  private RemoteCallPool remoteCallPool = new RemoteCallPool();

  /**
   * Call JobWorker::checkIfNeedRollback async
   *
   * @param actor JobWorker actor
   * @param callback callback function on success
   * @param onException callback function on exception
   */
  public void checkIfNeedRollbackAsync(
      BaseActorHandle actor, Callback<Boolean> callback, ExceptionHandler<Throwable> onException) {
    if (actor instanceof PyActorHandle) {
      // python
      remoteCallPool.bindCallback(
          ((PyActorHandle) actor).task(PyActorMethod.of("check_if_need_rollback")).remote(),
          (obj) -> {
            byte[] res = (byte[]) obj;
            callback.handle(PbResultParser.parseBoolResult(res));
          },
          onException);
    } else {
      // java
      remoteCallPool.bindCallback(
          ((ActorHandle<JobWorker>) actor)
              .task(JobWorker::checkIfNeedRollback, System.currentTimeMillis())
              .remote(),
          callback,
          onException);
    }
  }

  /**
   * Call JobWorker::rollback async
   *
   * @param actor JobWorker actor
   * @param callback callback function on success
   * @param onException callback function on exception
   */
  public void rollback(
      BaseActorHandle actor,
      final Long checkpointId,
      Callback<CallResult<ChannelRecoverInfo>> callback,
      ExceptionHandler<Throwable> onException) {
    // python
    if (actor instanceof PyActorHandle) {
      RemoteCall.CheckpointId checkpointIdPb =
          RemoteCall.CheckpointId.newBuilder().setCheckpointId(checkpointId).build();
      ObjectRef call =
          ((PyActorHandle) actor)
              .task(PyActorMethod.of("rollback"), checkpointIdPb.toByteArray())
              .remote();
      remoteCallPool.bindCallback(
          call,
          obj -> callback.handle(PbResultParser.parseRollbackResult((byte[]) obj)),
          onException);
    } else {
      // java
      ObjectRef call =
          ((ActorHandle<JobWorker>) actor)
              .task(JobWorker::rollback, checkpointId, System.currentTimeMillis())
              .remote();
      remoteCallPool.bindCallback(
          call,
          obj -> {
            CallResult<ChannelRecoverInfo> res = (CallResult<ChannelRecoverInfo>) obj;
            callback.handle(res);
          },
          onException);
    }
  }

  /**
   * Call JobWorker::rollback async in batch
   *
   * @param actors JobWorker actor list
   * @param callback callback function on success
   * @param onException callback function on exception
   */
  public void batchRollback(
      List<BaseActorHandle> actors,
      final Long checkpointId,
      Collection<String> abnormalQueues,
      Callback<List<CallResult<ChannelRecoverInfo>>> callback,
      ExceptionHandler<Throwable> onException) {
    List<ObjectRef<Object>> rayCallList = new ArrayList<>();
    Map<Integer, Boolean> isPyActor = new HashMap<>();
    for (int i = 0; i < actors.size(); ++i) {
      BaseActorHandle actor = actors.get(i);
      ObjectRef call;
      if (actor instanceof PyActorHandle) {
        isPyActor.put(i, true);
        RemoteCall.CheckpointId checkpointIdPb =
            RemoteCall.CheckpointId.newBuilder().setCheckpointId(checkpointId).build();
        call =
            ((PyActorHandle) actor)
                .task(PyActorMethod.of("rollback"), checkpointIdPb.toByteArray())
                .remote();
      } else {
        // java
        call =
            ((ActorHandle<JobWorker>) actor)
                .task(JobWorker::rollback, checkpointId, System.currentTimeMillis())
                .remote();
      }
      rayCallList.add(call);
    }
    remoteCallPool.bindCallback(
        rayCallList,
        objList -> {
          List<CallResult<ChannelRecoverInfo>> results = new ArrayList<>();
          for (int i = 0; i < objList.size(); ++i) {
            Object obj = objList.get(i);
            if (isPyActor.getOrDefault(i, false)) {
              results.add(PbResultParser.parseRollbackResult((byte[]) obj));
            } else {
              results.add((CallResult<ChannelRecoverInfo>) obj);
            }
          }
          callback.handle(results);
        },
        onException);
  }
}
