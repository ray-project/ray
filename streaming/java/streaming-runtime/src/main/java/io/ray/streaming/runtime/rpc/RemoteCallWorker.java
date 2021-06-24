package io.ray.streaming.runtime.rpc;

import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorMethod;
import io.ray.api.function.RayFunc3;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Ray call worker. It takes the communication job from {@link JobMaster} to {@link JobWorker}. */
public class RemoteCallWorker {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallWorker.class);

  /**
   * Call JobWorker actor to init.
   *
   * @param actor target JobWorker actor
   * @param context JobWorker's context
   * @return init result
   */
  public static ObjectRef<Boolean> initWorker(BaseActorHandle actor, JobWorkerContext context) {
    LOG.info("Call worker to initiate, actor: {}, context: {}.", actor.getId(), context);
    ObjectRef<Boolean> result;

    // python
    if (actor instanceof PyActorHandle) {
      result =
          ((PyActorHandle) actor)
              .task(PyActorMethod.of("init", Boolean.class), context.getPythonWorkerContextBytes())
              .remote();
    } else {
      // java
      result = ((ActorHandle<JobWorker>) actor).task(JobWorker::init, context).remote();
    }

    LOG.info("Finished calling worker to initiate.");
    return result;
  }

  /**
   * Call JobWorker actor to start.
   *
   * @param actor target JobWorker actor
   * @param checkpointId checkpoint ID to be rollback
   * @return start result
   */
  public static ObjectRef rollback(BaseActorHandle actor, final Long checkpointId) {
    LOG.info("Call worker to start, actor: {}.", actor.getId());
    ObjectRef result;

    // python
    if (actor instanceof PyActorHandle) {
      RemoteCall.CheckpointId checkpointIdPb =
          RemoteCall.CheckpointId.newBuilder().setCheckpointId(checkpointId).build();
      result =
          ((PyActorHandle) actor)
              .task(PyActorMethod.of("rollback"), checkpointIdPb.toByteArray())
              .remote();
    } else {
      // java
      result =
          ((ActorHandle<JobWorker>) actor)
              .task(JobWorker::rollback, checkpointId, System.currentTimeMillis())
              .remote();
    }

    LOG.info("Finished calling worker to start.");
    return result;
  }

  /**
   * Call JobWorker actor to destroy without reconstruction.
   *
   * @param actor target JobWorker actor
   * @return destroy result
   */
  public static Boolean shutdownWithoutReconstruction(BaseActorHandle actor) {
    LOG.info("Call worker to shutdown without reconstruction, actor is {}.", actor.getId());
    Boolean result = false;

    // TODO (datayjz): ray call worker to destroy

    LOG.info("Finished calling wk shutdownWithoutReconstruction, result is {}.", result);
    return result;
  }

  public static ObjectRef triggerCheckpoint(BaseActorHandle actor, Long barrierId) {
    // python
    if (actor instanceof PyActorHandle) {
      RemoteCall.Barrier barrierPb = RemoteCall.Barrier.newBuilder().setId(barrierId).build();
      return ((PyActorHandle) actor)
          .task(PyActorMethod.of("commit"), barrierPb.toByteArray())
          .remote();
    } else {
      // java
      return ((ActorHandle<JobWorker>) actor)
          .task(JobWorker::triggerCheckpoint, barrierId)
          .remote();
    }
  }

  public static void clearExpiredCheckpointParallel(
      List<BaseActorHandle> actors, Long stateCheckpointId, Long queueCheckpointId) {
    if (LOG.isInfoEnabled()) {
      LOG.info(
          "Call worker clearExpiredCheckpoint, state checkpoint id is {},"
              + " queue checkpoint id is {}.",
          stateCheckpointId,
          queueCheckpointId);
    }

    List<Object> result =
        checkpointCompleteCommonCallTwoWay(
            actors,
            stateCheckpointId,
            queueCheckpointId,
            "clear_expired_cp",
            JobWorker::clearExpiredCheckpoint);

    if (LOG.isInfoEnabled()) {
      result.forEach(
          obj -> LOG.info("Finish call worker clearExpiredCheckpointParallel, ret is {}.", obj));
    }
  }

  public static void notifyCheckpointTimeoutParallel(
      List<BaseActorHandle> actors, Long checkpointId) {
    LOG.info("Call worker notifyCheckpointTimeoutParallel, checkpoint id is {}", checkpointId);

    actors.forEach(
        actor -> {
          if (actor instanceof PyActorHandle) {
            RemoteCall.CheckpointId checkpointIdPb =
                RemoteCall.CheckpointId.newBuilder().setCheckpointId(checkpointId).build();
            ((PyActorHandle) actor)
                .task(PyActorMethod.of("notify_checkpoint_timeout"), checkpointIdPb.toByteArray())
                .remote();
          } else {
            ((ActorHandle<JobWorker>) actor)
                .task(JobWorker::notifyCheckpointTimeout, checkpointId)
                .remote();
          }
        });

    LOG.info("Finish call worker notifyCheckpointTimeoutParallel.");
  }

  private static List<Object> checkpointCompleteCommonCallTwoWay(
      List<BaseActorHandle> actors,
      Long stateCheckpointId,
      Long queueCheckpointId,
      String pyFuncName,
      RayFunc3<JobWorker, Long, Long, Boolean> rayFunc) {
    List<ObjectRef<Object>> waitFor =
        checkpointCompleteCommonCall(
            actors, stateCheckpointId, queueCheckpointId, pyFuncName, rayFunc);
    return Ray.get(waitFor);
  }

  private static List<ObjectRef<Object>> checkpointCompleteCommonCall(
      List<BaseActorHandle> actors,
      Long stateCheckpointId,
      Long queueCheckpointId,
      String pyFuncName,
      RayFunc3<JobWorker, Long, Long, Boolean> rayFunc) {
    List<ObjectRef<Object>> waitFor = new ArrayList<>();
    actors.forEach(
        actor -> {
          // python
          if (actor instanceof PyActorHandle) {
            RemoteCall.CheckpointId stateCheckpointIdPb =
                RemoteCall.CheckpointId.newBuilder().setCheckpointId(stateCheckpointId).build();

            RemoteCall.CheckpointId queueCheckpointIdPb =
                RemoteCall.CheckpointId.newBuilder().setCheckpointId(queueCheckpointId).build();
            waitFor.add(
                ((PyActorHandle) actor)
                    .task(
                        PyActorMethod.of(pyFuncName),
                        stateCheckpointIdPb.toByteArray(),
                        queueCheckpointIdPb.toByteArray())
                    .remote());
          } else {
            // java
            waitFor.add(
                ((ActorHandle) actor).task(rayFunc, stateCheckpointId, queueCheckpointId).remote());
          }
        });
    return waitFor;
  }
}
