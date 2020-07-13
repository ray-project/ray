package io.ray.streaming.runtime.master.coordinator.checkpoint;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.exception.RayException;
import io.ray.api.id.ActorId;
import io.ray.streaming.runtime.barrier.Barrier;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.master.context.CheckpointContext;
import io.ray.streaming.runtime.master.coordinator.BaseCoordinator;
import io.ray.streaming.runtime.master.coordinator.command.BaseWorkerCmd;
import io.ray.streaming.runtime.master.coordinator.command.WorkerCommitReport;
import io.ray.streaming.runtime.rpc.RemoteCallWorker;
import io.ray.streaming.runtime.worker.JobWorker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * CheckpointCoordinator is the controller of checkpoint, responsible for triggering checkpoint, collecting
 * {@link JobWorker}'s reports and calling {@link JobWorker} to clear expired checkpoints when new checkpoint finished.
 */
public class CheckpointCoordinator extends BaseCoordinator {

 private static final Logger LOG = LoggerFactory.getLogger(CheckpointCoordinator.class);

  private int cpIntervalSecs;
  private int cpTimeoutSecs;
  private final Set<ActorId> pendingCheckpointActors = new HashSet<>();
  private final Set<Long> interruptedCheckpointSet = new HashSet<>();
  private CheckpointContext checkpointContext;

  public CheckpointCoordinator(JobMaster jobMaster) {
    super(jobMaster);

    this.checkpointContext = runtimeContext.getCheckpointContext();
    // get checkpoint interval from conf
    this.cpIntervalSecs = runtimeContext.getConf().masterConfig.checkpointConfig.cpIntervalSecs();
    this.cpTimeoutSecs = runtimeContext.getConf().masterConfig.checkpointConfig.cpTimeoutSecs();

    // Trigger next checkpoint in interval by reset last checkpoint timestamp.
    checkpointContext.lastCpTimestamp = System.currentTimeMillis();
  }

  @Override
  public void run() {
    while (!closed) {
      try {
        final BaseWorkerCmd command = checkpointContext.cpCmds.poll(1, TimeUnit.SECONDS);
        if (command != null) {
          if (command instanceof WorkerCommitReport) {
            dealWithCommitReport((WorkerCommitReport) command);
          } else {
            interruptCheckpoint();
          }
        }

        if (!pendingCheckpointActors.isEmpty()) {
          // if wait commit report timeout, this cp fail, and restart next cp
          if (isTimeoutOfWaitCp()) {
            LOG.warn("Waiting for checkpoint {} timeout, pending cp actors is {}.",
              checkpointContext.lastCheckpointId,
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

    Boolean ret = checkpointContext.cpCmds.offer(report);
    if (!ret) {
      LOG.warn("Report job worker commit failed, because command queue is full.");
    }
    return ret;
  }

  private void dealWithCommitReport(WorkerCommitReport commitReport) {
    LOG.info("Start deal with commit report {}, from actor name={}.", commitReport,
        graphManager.getExecutionGraph().getActorName(commitReport.fromActorId));

    try {
      Preconditions.checkArgument(
          commitReport.commitCheckpointId == checkpointContext.lastCheckpointId,
          "expect checkpointId %s, but got %s",
        checkpointContext.lastCheckpointId, commitReport);

      if (!pendingCheckpointActors.contains(commitReport.fromActorId)) {
        LOG.warn("Invalid commit report, skipped.");
        return;
      }

      pendingCheckpointActors.remove(commitReport.fromActorId);
      LOG.info("Pending actors after this commit: {}.",
          graphManager.getExecutionGraph().getActorName(pendingCheckpointActors));

      // checkpoint finish
      if (pendingCheckpointActors.isEmpty()) {
        // actor finish
        checkpointContext.checkpointIds.add(checkpointContext.lastCheckpointId);

        if (clearExpiredCpStateAndQueueMsg()) {
          // save master context
          jobMaster.saveContext();

          LOG.info("Finish checkpoint: {}.", checkpointContext.lastCheckpointId);
        } else {
          LOG.warn("Fail to do checkpoint: {}.", checkpointContext.lastCheckpointId);
        }
      }

      LOG.info("Deal with commit report {} success.", commitReport);
    } catch (Throwable e) {
      LOG.warn("Deal with commit report has exception.", e);
    }
  }

  private void triggerCheckpoint() {
    interruptedCheckpointSet.clear();
    if (LOG.isInfoEnabled()) {
      LOG.info("Start trigger checkpoint {}.", checkpointContext.lastCheckpointId + 1);
    }

    List<ActorId> allIds = graphManager.getExecutionGraph().getAllActorsId();
    // do the checkpoint
    pendingCheckpointActors.addAll(allIds);

    // inc last checkpoint id
    ++checkpointContext.lastCheckpointId;

    final List<ObjectRef> sourcesRet = new ArrayList<>();

    graphManager.getExecutionGraph().getSourceActors().forEach(actor -> {
        sourcesRet.add(RemoteCallWorker.triggerCheckpoint(
            actor, new Barrier(checkpointContext.lastCheckpointId)));
    });

    for (ObjectRef rayObject : sourcesRet) {
      if (rayObject.get() instanceof RayException) {
        LOG.warn("Trigger checkpoint has exception.", rayObject.get());
        throw (RayException) rayObject.get();
      }
    }
    checkpointContext.lastCpTimestamp = System.currentTimeMillis();
    LOG.info("Trigger checkpoint success.");
  }

  private void interruptCheckpoint() {
    // notify checkpoint timeout is time-consuming while many workers crash or
    // container failover.
    if (interruptedCheckpointSet.contains(checkpointContext.lastCheckpointId)) {
      LOG.warn("Skip interrupt duplicated checkpoint id : {}.", checkpointContext.lastCheckpointId);
      return;
    }
    interruptedCheckpointSet.add(checkpointContext.lastCheckpointId);
    LOG.warn("Interrupt checkpoint, checkpoint id : {}.", checkpointContext.lastCheckpointId);

    List<BaseActorHandle> allActor = graphManager.getExecutionGraph().getAllActors();
    if (checkpointContext.lastCheckpointId > checkpointContext.getLastValidCheckpointId()) {
      RemoteCallWorker.notifyCheckpointTimeoutParallel(allActor, checkpointContext.lastCheckpointId);
    }

    if (!pendingCheckpointActors.isEmpty()) {
      pendingCheckpointActors.clear();
    }
    maybeTriggerCheckpoint();
  }

  private void maybeTriggerCheckpoint() {
    if (isReadyToTrigger()) {
      triggerCheckpoint();
    }
  }

  private boolean clearExpiredCpStateAndQueueMsg() {
    // queue msg must clear when first checkpoint finish
    List<BaseActorHandle> allActor = graphManager.getExecutionGraph().getAllActors();
    if (1 == checkpointContext.checkpointIds.size()) {
      Long qMsgExpiredCheckpointId = checkpointContext.checkpointIds.get(0);
      RemoteCallWorker.clearExpiredCpParallel(allActor, 0L, qMsgExpiredCheckpointId);
    }

    if (checkpointContext.checkpointIds.size() > 1) {
      Long stateExpiredCpId = checkpointContext.checkpointIds.remove(0);
      Long qMsgExpiredCheckpointId = checkpointContext.checkpointIds.get(0);
      RemoteCallWorker.clearExpiredCpParallel(allActor, stateExpiredCpId, qMsgExpiredCheckpointId);
    }
    return true;
  }

  private boolean isReadyToTrigger() {
    return (System.currentTimeMillis() - checkpointContext.lastCpTimestamp) >= cpIntervalSecs * 1000;
  }

  private boolean isTimeoutOfWaitCp() {
    return (System.currentTimeMillis() - checkpointContext.lastCpTimestamp) >= cpTimeoutSecs * 1000;
  }
}
