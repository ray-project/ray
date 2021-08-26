package io.ray.streaming.runtime.master.coordinator;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.id.ActorId;
import io.ray.runtime.exception.RayException;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.master.coordinator.command.BaseWorkerCmd;
import io.ray.streaming.runtime.master.coordinator.command.WorkerCommitReport;
import io.ray.streaming.runtime.rpc.RemoteCallWorker;
import io.ray.streaming.runtime.worker.JobWorker;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CheckpointCoordinator is the controller of checkpoint, responsible for triggering checkpoint,
 * collecting {@link JobWorker}'s reports and calling {@link JobWorker} to clear expired checkpoints
 * when new checkpoint finished.
 */
public class CheckpointCoordinator extends BaseCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);
  private final Set<ActorId> pendingCheckpointActors = new HashSet<>();
  private final Set<Long> interruptedCheckpointSet = new HashSet<>();
  private final int cpIntervalSecs;
  private final int cpTimeoutSecs;

  public CheckpointCoordinator(JobMaster jobMaster) {
    super(jobMaster);

    // get checkpoint interval from conf
    this.cpIntervalSecs = runtimeContext.getConf().masterConfig.checkpointConfig.cpIntervalSecs();
    this.cpTimeoutSecs = runtimeContext.getConf().masterConfig.checkpointConfig.cpTimeoutSecs();

    // Trigger next checkpoint in interval by reset last checkpoint timestamp.
    runtimeContext.lastCpTimestamp = System.currentTimeMillis();
  }

  @Override
  public void run() {
    while (!closed) {
      try {
        final BaseWorkerCmd command = runtimeContext.cpCmds.poll(1, TimeUnit.SECONDS);
        if (command != null) {
          if (command instanceof WorkerCommitReport) {
            processCommitReport((WorkerCommitReport) command);
          } else {
            interruptCheckpoint();
          }
        }

        if (!pendingCheckpointActors.isEmpty()) {
          // if wait commit report timeout, this cp fail, and restart next cp
          if (timeoutOnWaitCheckpoint()) {
            LOG.warn(
                "Waiting for checkpoint {} timeout, pending cp actors is {}.",
                runtimeContext.lastCheckpointId,
                graphManager.getExecutionGraph().getActorName(pendingCheckpointActors));

            interruptCheckpoint();
          }
        } else {
          maybeTriggerCheckpoint();
        }
      } catch (Throwable e) {
        LOG.error("Checkpoint coordinator occur err.", e);
        try {
          interruptCheckpoint();
        } catch (Throwable interruptE) {
          LOG.error("Ignore interrupt checkpoint exception in catch block.");
        }
      }
    }
    LOG.warn("Checkpoint coordinator thread exit.");
  }

  public Boolean reportJobWorkerCommit(WorkerCommitReport report) {
    LOG.info("Report job worker commit {}.", report);

    Boolean ret = runtimeContext.cpCmds.offer(report);
    if (!ret) {
      LOG.warn("Report job worker commit failed, because command queue is full.");
    }
    return ret;
  }

  private void processCommitReport(WorkerCommitReport commitReport) {
    LOG.info(
        "Start process commit report {}, from actor name={}.",
        commitReport,
        graphManager.getExecutionGraph().getActorName(commitReport.fromActorId));

    try {
      Preconditions.checkArgument(
          commitReport.commitCheckpointId == runtimeContext.lastCheckpointId,
          "expect checkpointId %s, but got %s",
          runtimeContext.lastCheckpointId,
          commitReport);

      if (!pendingCheckpointActors.contains(commitReport.fromActorId)) {
        LOG.warn("Invalid commit report, skipped.");
        return;
      }

      pendingCheckpointActors.remove(commitReport.fromActorId);
      LOG.info(
          "Pending actors after this commit: {}.",
          graphManager.getExecutionGraph().getActorName(pendingCheckpointActors));

      // checkpoint finish
      if (pendingCheckpointActors.isEmpty()) {
        // actor finish
        runtimeContext.checkpointIds.add(runtimeContext.lastCheckpointId);

        if (clearExpiredCpStateAndQueueMsg()) {
          // save master context
          jobMaster.saveContext();

          LOG.info("Finish checkpoint: {}.", runtimeContext.lastCheckpointId);
        } else {
          LOG.warn("Fail to do checkpoint: {}.", runtimeContext.lastCheckpointId);
        }
      }

      LOG.info("Process commit report {} success.", commitReport);
    } catch (Throwable e) {
      LOG.warn("Process commit report has exception.", e);
    }
  }

  private void triggerCheckpoint() {
    interruptedCheckpointSet.clear();
    if (LOG.isInfoEnabled()) {
      LOG.info("Start trigger checkpoint {}.", runtimeContext.lastCheckpointId + 1);
    }

    List<ActorId> allIds = graphManager.getExecutionGraph().getAllActorsId();
    // do the checkpoint
    pendingCheckpointActors.addAll(allIds);

    // inc last checkpoint id
    ++runtimeContext.lastCheckpointId;

    final List<ObjectRef> sourcesRet = new ArrayList<>();

    graphManager
        .getExecutionGraph()
        .getSourceActors()
        .forEach(
            actor -> {
              sourcesRet.add(
                  RemoteCallWorker.triggerCheckpoint(actor, runtimeContext.lastCheckpointId));
            });

    for (ObjectRef rayObject : sourcesRet) {
      if (rayObject.get() instanceof RayException) {
        LOG.warn("Trigger checkpoint has exception.", (RayException) rayObject.get());
        throw (RayException) rayObject.get();
      }
    }
    runtimeContext.lastCpTimestamp = System.currentTimeMillis();
    LOG.info("Trigger checkpoint success.");
  }

  private void interruptCheckpoint() {
    // notify checkpoint timeout is time-consuming while many workers crash or
    // container failover.
    if (interruptedCheckpointSet.contains(runtimeContext.lastCheckpointId)) {
      LOG.warn("Skip interrupt duplicated checkpoint id : {}.", runtimeContext.lastCheckpointId);
      return;
    }
    interruptedCheckpointSet.add(runtimeContext.lastCheckpointId);
    LOG.warn("Interrupt checkpoint, checkpoint id : {}.", runtimeContext.lastCheckpointId);

    List<BaseActorHandle> allActor = graphManager.getExecutionGraph().getAllActors();
    if (runtimeContext.lastCheckpointId > runtimeContext.getLastValidCheckpointId()) {
      RemoteCallWorker.notifyCheckpointTimeoutParallel(allActor, runtimeContext.lastCheckpointId);
    }

    if (!pendingCheckpointActors.isEmpty()) {
      pendingCheckpointActors.clear();
    }
    maybeTriggerCheckpoint();
  }

  private void maybeTriggerCheckpoint() {
    if (readyToTrigger()) {
      triggerCheckpoint();
    }
  }

  private boolean clearExpiredCpStateAndQueueMsg() {
    // queue msg must clear when first checkpoint finish
    List<BaseActorHandle> allActor = graphManager.getExecutionGraph().getAllActors();
    if (1 == runtimeContext.checkpointIds.size()) {
      Long msgExpiredCheckpointId = runtimeContext.checkpointIds.get(0);
      RemoteCallWorker.clearExpiredCheckpointParallel(allActor, 0L, msgExpiredCheckpointId);
    }

    if (runtimeContext.checkpointIds.size() > 1) {
      Long stateExpiredCpId = runtimeContext.checkpointIds.remove(0);
      Long msgExpiredCheckpointId = runtimeContext.checkpointIds.get(0);
      RemoteCallWorker.clearExpiredCheckpointParallel(
          allActor, stateExpiredCpId, msgExpiredCheckpointId);
    }
    return true;
  }

  private boolean readyToTrigger() {
    return (System.currentTimeMillis() - runtimeContext.lastCpTimestamp) >= cpIntervalSecs * 1000;
  }

  private boolean timeoutOnWaitCheckpoint() {
    return (System.currentTimeMillis() - runtimeContext.lastCpTimestamp) >= cpTimeoutSecs * 1000;
  }
}
