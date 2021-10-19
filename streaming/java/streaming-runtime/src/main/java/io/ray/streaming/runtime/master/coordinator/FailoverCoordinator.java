package io.ray.streaming.runtime.master.coordinator;

import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.master.context.JobMasterRuntimeContext;
import io.ray.streaming.runtime.master.coordinator.command.BaseWorkerCmd;
import io.ray.streaming.runtime.master.coordinator.command.InterruptCheckpointRequest;
import io.ray.streaming.runtime.master.coordinator.command.WorkerRollbackRequest;
import io.ray.streaming.runtime.rpc.async.AsyncRemoteCaller;
import io.ray.streaming.runtime.transfer.channel.ChannelRecoverInfo;
import io.ray.streaming.runtime.util.ResourceUtil;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections.map.DefaultedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FailoverCoordinator extends BaseCoordinator {

  private static final Logger LOG = LoggerFactory.getLogger(FailoverCoordinator.class);

  private static final int ROLLBACK_RETRY_TIME_MS = 10 * 1000;
  private final Object cmdLock = new Object();
  private final AsyncRemoteCaller asyncRemoteCaller;
  private long currentCascadingGroupId = 0;
  private final Map<ExecutionVertex, Boolean> isRollbacking =
      DefaultedMap.decorate(new ConcurrentHashMap<ExecutionVertex, Boolean>(), false);

  public FailoverCoordinator(JobMaster jobMaster, boolean isRecover) {
    this(jobMaster, new AsyncRemoteCaller(), isRecover);
  }

  public FailoverCoordinator(
      JobMaster jobMaster, AsyncRemoteCaller asyncRemoteCaller, boolean isRecover) {
    super(jobMaster);

    this.asyncRemoteCaller = asyncRemoteCaller;
    // recover unfinished FO commands
    JobMasterRuntimeContext runtimeContext = jobMaster.getRuntimeContext();
    if (isRecover) {
      runtimeContext.foCmds.addAll(runtimeContext.unfinishedFoCmds);
    }
    runtimeContext.unfinishedFoCmds.clear();
  }

  @Override
  public void run() {
    while (!closed) {
      try {
        final BaseWorkerCmd command;
        // see rollback() for lock reason
        synchronized (cmdLock) {
          command = jobMaster.getRuntimeContext().foCmds.poll(1, TimeUnit.SECONDS);
        }
        if (null == command) {
          continue;
        }
        if (command instanceof WorkerRollbackRequest) {
          jobMaster.getRuntimeContext().unfinishedFoCmds.add(command);
          dealWithRollbackRequest((WorkerRollbackRequest) command);
        }
      } catch (Throwable e) {
        LOG.error("Fo coordinator occur err.", e);
      }
    }
    LOG.warn("Fo coordinator thread exit.");
  }

  private Boolean isDuplicateRequest(WorkerRollbackRequest request) {
    try {
      Object[] foCmdsArray = runtimeContext.foCmds.toArray();
      for (Object cmd : foCmdsArray) {
        if (request.fromActorId.equals(((BaseWorkerCmd) cmd).fromActorId)) {
          return true;
        }
      }
    } catch (Exception e) {
      LOG.warn("Check request is duplicated failed.", e);
    }
    return false;
  }

  public Boolean requestJobWorkerRollback(WorkerRollbackRequest request) {
    LOG.info("Request job worker rollback {}.", request);
    boolean ret;
    if (!isDuplicateRequest(request)) {
      ret = runtimeContext.foCmds.offer(request);
    } else {
      LOG.warn("Skip duplicated worker rollback request, {}.", request.toString());
      return true;
    }
    jobMaster.saveContext();
    if (!ret) {
      LOG.warn("Request job worker rollback failed, because command queue is full.");
    }
    return ret;
  }

  private void dealWithRollbackRequest(WorkerRollbackRequest rollbackRequest) {
    LOG.info("Start deal with rollback request {}.", rollbackRequest);

    ExecutionVertex exeVertex = getExeVertexFromRequest(rollbackRequest);

    // Reset pid for new-rollback actor.
    if (null != rollbackRequest.getPid()
        && !rollbackRequest.getPid().equals(WorkerRollbackRequest.DEFAULT_PID)) {
      exeVertex.setPid(rollbackRequest.getPid());
    }

    if (isRollbacking.get(exeVertex)) {
      LOG.info("Vertex {} is rollbacking, skip rollback again.", exeVertex);
      return;
    }

    String hostname = "";
    Optional<Container> container =
        ResourceUtil.getContainerById(
            jobMaster.getResourceManager().getRegisteredContainers(), exeVertex.getContainerId());
    if (container.isPresent()) {
      hostname = container.get().getHostname();
    }

    if (rollbackRequest.isForcedRollback) {
      interruptCheckpointAndRollback(rollbackRequest);
    } else {
      asyncRemoteCaller.checkIfNeedRollbackAsync(
          exeVertex.getWorkerActor(),
          res -> {
            if (!res) {
              LOG.info("Vertex {} doesn't need to rollback, skip it.", exeVertex);
              return;
            }
            interruptCheckpointAndRollback(rollbackRequest);
          },
          throwable -> {
            LOG.error(
                "Exception when calling checkIfNeedRollbackAsync, maybe vertex is dead"
                    + ", ignore this request, vertex={}.",
                exeVertex,
                throwable);
          });
    }

    LOG.info("Deal with rollback request {} success.", rollbackRequest);
  }

  private void interruptCheckpointAndRollback(WorkerRollbackRequest rollbackRequest) {
    // assign a cascadingGroupId
    if (rollbackRequest.cascadingGroupId == null) {
      rollbackRequest.cascadingGroupId = currentCascadingGroupId++;
    }
    // get last valid checkpoint id then call worker rollback
    rollback(
        jobMaster.getRuntimeContext().getLastValidCheckpointId(),
        rollbackRequest,
        currentCascadingGroupId);
    // we interrupt current checkpoint for 2 considerations:
    // 1. current checkpoint might be timeout, because barrier might be lost after failover. so we
    // interrupt current checkpoint to avoid waiting.
    // 2. when we want to rollback vertex to n, job finished checkpoint n+1 and cleared state
    // of checkpoint n.
    jobMaster.getRuntimeContext().cpCmds.offer(new InterruptCheckpointRequest());
  }

  /**
   * call worker rollback, and deal with it's reports. callback won't be finished until the entire
   * DAG back to normal.
   *
   * @param checkpointId checkpointId to be rollback
   * @param rollbackRequest worker rollback request
   * @param cascadingGroupId all rollback of a cascading group should have same ID
   */
  private void rollback(
      long checkpointId, WorkerRollbackRequest rollbackRequest, long cascadingGroupId) {
    ExecutionVertex exeVertex = getExeVertexFromRequest(rollbackRequest);
    LOG.info(
        "Call vertex {} to rollback, checkpoint id is {}, cascadingGroupId={}.",
        exeVertex,
        checkpointId,
        cascadingGroupId);

    isRollbacking.put(exeVertex, true);

    asyncRemoteCaller.rollback(
        exeVertex.getWorkerActor(),
        checkpointId,
        result -> {
          List<WorkerRollbackRequest> newRollbackRequests = new ArrayList<>();
          switch (result.getResultEnum()) {
            case SUCCESS:
              ChannelRecoverInfo recoverInfo = result.getResultObj();
              LOG.info(
                  "Vertex {} rollback done, dataLostQueues={}, msg={}, cascadingGroupId={}.",
                  exeVertex,
                  recoverInfo.getDataLostQueues(),
                  result.getResultMsg(),
                  cascadingGroupId);
              // rollback upstream if vertex reports abnormal input queues
              newRollbackRequests =
                  cascadeUpstreamActors(
                      recoverInfo.getDataLostQueues(), exeVertex, cascadingGroupId);
              break;
            case SKIPPED:
              LOG.info(
                  "Vertex skip rollback, result = {}, cascadingGroupId={}.",
                  result,
                  cascadingGroupId);
              break;
            default:
              LOG.error(
                  "Rollback vertex {} failed, result={}, cascadingGroupId={},"
                      + " rollback this worker again after {} ms.",
                  exeVertex,
                  result,
                  cascadingGroupId,
                  ROLLBACK_RETRY_TIME_MS);
              Thread.sleep(ROLLBACK_RETRY_TIME_MS);
              LOG.info(
                  "Add rollback request for {} again, cascadingGroupId={}.",
                  exeVertex,
                  cascadingGroupId);
              newRollbackRequests.add(
                  new WorkerRollbackRequest(exeVertex, "", "Rollback failed, try again.", false));
              break;
          }

          // lock to avoid executing new rollback requests added.
          // consider such a case: A->B->C, C cascade B, and B cascade A
          // if B is rollback before B's rollback request is saved, and then JobMaster crashed,
          // then A will never be rollback.
          synchronized (cmdLock) {
            jobMaster.getRuntimeContext().foCmds.addAll(newRollbackRequests);
            // this rollback request is finished, remove it.
            jobMaster.getRuntimeContext().unfinishedFoCmds.remove(rollbackRequest);
            jobMaster.saveContext();
          }
          isRollbacking.put(exeVertex, false);
        },
        throwable -> {
          LOG.error("Exception when calling vertex to rollback, vertex={}.", exeVertex, throwable);
          isRollbacking.put(exeVertex, false);
        });

    LOG.info("Finish rollback vertex {}, checkpoint id is {}.", exeVertex, checkpointId);
  }

  private List<WorkerRollbackRequest> cascadeUpstreamActors(
      Set<String> dataLostQueues, ExecutionVertex fromVertex, long cascadingGroupId) {
    List<WorkerRollbackRequest> cascadedRollbackRequest = new ArrayList<>();
    // rollback upstream if vertex reports abnormal input queues
    dataLostQueues.forEach(
        q -> {
          BaseActorHandle upstreamActor =
              graphManager.getExecutionGraph().getPeerActor(fromVertex.getWorkerActor(), q);
          ExecutionVertex upstreamExeVertex = getExecutionVertex(upstreamActor);
          // vertexes that has already cascaded by other vertex in the same level
          // of graph should be ignored.
          if (isRollbacking.get(upstreamExeVertex)) {
            return;
          }
          LOG.info(
              "Call upstream vertex {} of vertex {} to rollback, cascadingGroupId={}.",
              upstreamExeVertex,
              fromVertex,
              cascadingGroupId);
          String hostname = "";
          Optional<Container> container =
              ResourceUtil.getContainerById(
                  jobMaster.getResourceManager().getRegisteredContainers(),
                  upstreamExeVertex.getContainerId());
          if (container.isPresent()) {
            hostname = container.get().getHostname();
          }
          // force upstream vertexes to rollback
          WorkerRollbackRequest upstreamRequest =
              new WorkerRollbackRequest(
                  upstreamExeVertex,
                  hostname,
                  String.format("Cascading rollback from %s", fromVertex),
                  true);
          upstreamRequest.cascadingGroupId = cascadingGroupId;
          cascadedRollbackRequest.add(upstreamRequest);
        });
    return cascadedRollbackRequest;
  }

  private ExecutionVertex getExeVertexFromRequest(WorkerRollbackRequest rollbackRequest) {
    ActorId actorId = rollbackRequest.fromActorId;
    Optional<BaseActorHandle> rayActor = graphManager.getExecutionGraph().getActorById(actorId);
    if (!rayActor.isPresent()) {
      throw new RuntimeException("Can not find ray actor of ID " + actorId);
    }
    return getExecutionVertex(rollbackRequest.fromActorId);
  }

  private ExecutionVertex getExecutionVertex(BaseActorHandle actor) {
    return graphManager.getExecutionGraph().getExecutionVertexByActorId(actor.getId());
  }

  private ExecutionVertex getExecutionVertex(ActorId actorId) {
    return graphManager.getExecutionGraph().getExecutionVertexByActorId(actorId);
  }
}
