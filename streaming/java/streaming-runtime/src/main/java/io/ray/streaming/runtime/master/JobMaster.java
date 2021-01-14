package io.ray.streaming.runtime.master;

import com.google.common.base.Preconditions;
import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.api.id.ActorId;
import io.ray.streaming.jobgraph.JobGraph;
import io.ray.streaming.runtime.config.StreamingConfig;
import io.ray.streaming.runtime.config.StreamingMasterConfig;
import io.ray.streaming.runtime.context.ContextBackend;
import io.ray.streaming.runtime.context.ContextBackendFactory;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionGraph;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.resource.Container;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.master.context.JobMasterRuntimeContext;
import io.ray.streaming.runtime.master.coordinator.CheckpointCoordinator;
import io.ray.streaming.runtime.master.coordinator.FailoverCoordinator;
import io.ray.streaming.runtime.master.coordinator.command.WorkerCommitReport;
import io.ray.streaming.runtime.master.coordinator.command.WorkerRollbackRequest;
import io.ray.streaming.runtime.master.graphmanager.GraphManager;
import io.ray.streaming.runtime.master.graphmanager.GraphManagerImpl;
import io.ray.streaming.runtime.master.resourcemanager.ResourceManager;
import io.ray.streaming.runtime.master.resourcemanager.ResourceManagerImpl;
import io.ray.streaming.runtime.master.scheduler.JobSchedulerImpl;
import io.ray.streaming.runtime.util.CheckpointStateUtil;
import io.ray.streaming.runtime.util.ResourceUtil;
import io.ray.streaming.runtime.util.Serializer;
import io.ray.streaming.runtime.worker.JobWorker;
import java.util.Map;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * JobMaster is the core controller in streaming job as a ray actor. It is responsible for all the
 * controls facing the {@link JobWorker}.
 */
public class JobMaster {

  private static final Logger LOG = LoggerFactory.getLogger(JobMaster.class);

  private JobMasterRuntimeContext runtimeContext;
  private ResourceManager resourceManager;
  private JobSchedulerImpl scheduler;
  private GraphManager graphManager;
  private StreamingMasterConfig conf;

  private ContextBackend contextBackend;

  private ActorHandle<JobMaster> jobMasterActor;

  // coordinators
  private CheckpointCoordinator checkpointCoordinator;
  private FailoverCoordinator failoverCoordinator;

  public JobMaster(Map<String, String> confMap) {
    LOG.info("Creating job master with conf: {}.", confMap);

    StreamingConfig streamingConfig = new StreamingConfig(confMap);
    this.conf = streamingConfig.masterConfig;
    this.contextBackend = ContextBackendFactory.getContextBackend(this.conf);

    // init runtime context
    runtimeContext = new JobMasterRuntimeContext(streamingConfig);

    // load checkpoint if is recover
    if (!Ray.getRuntimeContext().isSingleProcess()
        && Ray.getRuntimeContext().wasCurrentActorRestarted()) {
      loadMasterCheckpoint();
    }

    LOG.info("Finished creating job master.");
  }

  public static String getJobMasterRuntimeContextKey(StreamingMasterConfig conf) {
    return conf.checkpointConfig.jobMasterContextCpPrefixKey() + conf.commonConfig.jobName();
  }

  private void loadMasterCheckpoint() {
    LOG.info("Start to load JobMaster's checkpoint.");
    // recover runtime context
    byte[] bytes =
        CheckpointStateUtil.get(contextBackend, getJobMasterRuntimeContextKey(getConf()));
    if (bytes == null) {
      LOG.warn("JobMaster got empty checkpoint from state backend. Skip loading checkpoint.");
      // cp 0 was automatically saved when job started, see StreamTask.
      runtimeContext.checkpointIds.add(0L);
      return;
    }

    this.runtimeContext = Serializer.decode(bytes);

    // FO case, triggered by ray, we need to register context when loading checkpoint
    LOG.info("JobMaster recover runtime context[{}] from state backend.", runtimeContext);
    init(true);
  }

  /**
   * Init JobMaster. To initiate or recover other components(like metrics and extra coordinators).
   *
   * <p>Returns init result
   */
  public Boolean init(boolean isRecover) {
    LOG.info("Initializing job master, isRecover={}.", isRecover);

    if (this.runtimeContext.getExecutionGraph() == null) {
      LOG.error("Init job master failed. Job graphs is null.");
      return false;
    }

    ExecutionGraph executionGraph = graphManager.getExecutionGraph();
    Preconditions.checkArgument(executionGraph != null, "no execution graph");

    // init coordinators
    checkpointCoordinator = new CheckpointCoordinator(this);
    checkpointCoordinator.start();
    failoverCoordinator = new FailoverCoordinator(this, isRecover);
    failoverCoordinator.start();

    saveContext();

    LOG.info("Finished initializing job master.");
    return true;
  }

  /**
   * Submit job to run:
   *
   * <ol>
   *   <li>Using GraphManager to build physical plan according to the logical plan.
   *   <li>Using ResourceManager to manage and allocate the resources.
   *   <li>Using JobScheduler to schedule the job to run.
   * </ol>
   *
   * @param jobMasterActor JobMaster actor
   * @param jobGraph logical plan Returns submit result
   */
  public boolean submitJob(ActorHandle<JobMaster> jobMasterActor, JobGraph jobGraph) {
    LOG.info("Begin submitting job using logical plan: {}.", jobGraph);

    this.jobMasterActor = jobMasterActor;

    // init manager
    graphManager = new GraphManagerImpl(runtimeContext);
    resourceManager = new ResourceManagerImpl(runtimeContext);

    // build and set graph into runtime context
    ExecutionGraph executionGraph = graphManager.buildExecutionGraph(jobGraph);
    runtimeContext.setJobGraph(jobGraph);
    runtimeContext.setExecutionGraph(executionGraph);

    // init scheduler
    try {
      scheduler = new JobSchedulerImpl(this);
      scheduler.scheduleJob(graphManager.getExecutionGraph());
    } catch (Exception e) {
      e.printStackTrace();
      LOG.error("Failed to submit job {}.", e, e);
      return false;
    }
    return true;
  }

  public synchronized void saveContext() {
    if (runtimeContext != null && getConf() != null) {
      LOG.debug("Save JobMaster context.");

      byte[] contextBytes = Serializer.encode(runtimeContext);
      CheckpointStateUtil.put(
          contextBackend, getJobMasterRuntimeContextKey(getConf()), contextBytes);
    }
  }

  public byte[] reportJobWorkerCommit(byte[] reportBytes) {
    Boolean ret = false;
    RemoteCall.BaseWorkerCmd reportPb;
    try {
      reportPb = RemoteCall.BaseWorkerCmd.parseFrom(reportBytes);
      ActorId actorId = ActorId.fromBytes(reportPb.getActorId().toByteArray());
      long remoteCallCost = System.currentTimeMillis() - reportPb.getTimestamp();
      LOG.info(
          "Vertex {}, request job worker commit cost {}ms, actorId={}.",
          getExecutionVertex(actorId),
          remoteCallCost,
          actorId);
      RemoteCall.WorkerCommitReport commit =
          reportPb.getDetail().unpack(RemoteCall.WorkerCommitReport.class);
      WorkerCommitReport report = new WorkerCommitReport(actorId, commit.getCommitCheckpointId());
      ret = checkpointCoordinator.reportJobWorkerCommit(report);
    } catch (InvalidProtocolBufferException e) {
      LOG.error("Parse job worker commit has exception.", e);
    }
    return RemoteCall.BoolResult.newBuilder().setBoolRes(ret).build().toByteArray();
  }

  public byte[] requestJobWorkerRollback(byte[] requestBytes) {
    Boolean ret = false;
    RemoteCall.BaseWorkerCmd requestPb;
    try {
      requestPb = RemoteCall.BaseWorkerCmd.parseFrom(requestBytes);
      ActorId actorId = ActorId.fromBytes(requestPb.getActorId().toByteArray());
      long remoteCallCost = System.currentTimeMillis() - requestPb.getTimestamp();
      ExecutionGraph executionGraph = graphManager.getExecutionGraph();
      Optional<BaseActorHandle> rayActor = executionGraph.getActorById(actorId);
      if (!rayActor.isPresent()) {
        LOG.warn("Skip this invalid rollback, actor id {} is not found.", actorId);
        return RemoteCall.BoolResult.newBuilder().setBoolRes(false).build().toByteArray();
      }
      ExecutionVertex exeVertex = getExecutionVertex(actorId);
      LOG.info(
          "Vertex {}, request job worker rollback cost {}ms, actorId={}.",
          exeVertex,
          remoteCallCost,
          actorId);
      RemoteCall.WorkerRollbackRequest rollbackPb =
          RemoteCall.WorkerRollbackRequest.parseFrom(requestPb.getDetail().getValue());
      exeVertex.setPid(rollbackPb.getWorkerPid());
      // To find old container where slot is located in.
      String hostname = "";
      Optional<Container> container =
          ResourceUtil.getContainerById(
              resourceManager.getRegisteredContainers(), exeVertex.getContainerId());
      if (container.isPresent()) {
        hostname = container.get().getHostname();
      }
      WorkerRollbackRequest request =
          new WorkerRollbackRequest(
              actorId, rollbackPb.getExceptionMsg(), hostname, exeVertex.getPid());

      ret = failoverCoordinator.requestJobWorkerRollback(request);
      LOG.info(
          "Vertex {} request rollback, exception msg : {}.",
          exeVertex,
          rollbackPb.getExceptionMsg());

    } catch (Throwable e) {
      LOG.error("Parse job worker rollback has exception.", e);
    }
    return RemoteCall.BoolResult.newBuilder().setBoolRes(ret).build().toByteArray();
  }

  private ExecutionVertex getExecutionVertex(ActorId id) {
    return graphManager.getExecutionGraph().getExecutionVertexByActorId(id);
  }

  public ActorHandle<JobMaster> getJobMasterActor() {
    return jobMasterActor;
  }

  public JobMasterRuntimeContext getRuntimeContext() {
    return runtimeContext;
  }

  public ResourceManager getResourceManager() {
    return resourceManager;
  }

  public GraphManager getGraphManager() {
    return graphManager;
  }

  public StreamingMasterConfig getConf() {
    return conf;
  }
}
