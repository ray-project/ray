package io.ray.streaming.runtime.worker.tasks;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.runtime.barrier.Barrier;
import io.ray.streaming.runtime.config.global.StateBackendConfig;
import io.ray.streaming.runtime.config.worker.WorkerInternalConfig;
import io.ray.streaming.runtime.core.collector.OutputCollector;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.master.coordinator.command.WorkerCommitReport;
import io.ray.streaming.runtime.rpc.RemoteCallMaster;
import io.ray.streaming.runtime.state.OpCheckpointInfo;
import io.ray.streaming.runtime.state.StateBackend;
import io.ray.streaming.runtime.transfer.DataReader;
import io.ray.streaming.runtime.transfer.DataWriter;
import io.ray.streaming.runtime.transfer.OffsetInfo;
import io.ray.streaming.runtime.transfer.QueueRecoverInfo;
import io.ray.streaming.runtime.util.CheckpointStateUtil;
import io.ray.streaming.runtime.util.Serializer;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import io.ray.streaming.runtime.worker.context.StreamingRuntimeContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamTask} is a while-loop thread to read message, process message, and send result
 * messages to downstream operators
 */
public abstract class StreamTask implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);

  protected Processor processor;
  protected JobWorker jobWorker;
  protected DataReader reader;
  protected DataWriter writer;
  List<Collector> collectors = new ArrayList<>();

  private final StateBackend<String, byte[], StateBackendConfig> checkpointState;
  private Set<Long> outdatedCheckpoints = new HashSet<>();

  protected volatile boolean running = true;
  protected volatile boolean stopped = false;
  public volatile boolean isInitialState = true;

  public long lastCheckpointId;

  private Thread thread;

  protected StreamTask(Processor processor, JobWorker jobWorker) {
    this.processor = processor;
    this.jobWorker = jobWorker;
    this.checkpointState = jobWorker.stateBackend;

    this.thread = new Thread(Ray.wrapRunnable(this),
        this.getClass().getName() + "-" + System.currentTimeMillis());
    this.thread.setDaemon(true);
  }

  public QueueRecoverInfo recover(boolean isRecover) {

    if (isRecover) {
      LOG.info("Stream task begin recover.");
    } else {
      LOG.info("Stream task first start begin.");
    }
    prepareTask(isRecover);

    // start runner
    QueueRecoverInfo recoverInfo = new QueueRecoverInfo(new HashMap<>());
    if (reader != null) {
      recoverInfo = reader.getQueueRecoverInfo();
    }

    thread.setUncaughtExceptionHandler((t, e) -> LOG.error("Uncaught exception in runner thread.", e));
    thread.start();

    if (isRecover) {
      LOG.info("Stream task recover end.");
    } else {
      LOG.info("Stream task first start finished.");
    }

    return recoverInfo;
  }

  /**
   * Load checkpoint and build upstream and downstream data transmission
   * channels according to {@link ExecutionVertex}.
   */
  private void prepareTask(boolean isRecreate) {
    LOG.debug("Preparing stream task.");
    ExecutionVertex executionVertex = jobWorker.getExecutionVertex();

    // set vertex info into config for native using
    jobWorker.getWorkerConfig().workerInternalConfig.setProperty(
        WorkerInternalConfig.WORKER_NAME_INTERNAL, executionVertex.getExecutionVertexName());
    jobWorker.getWorkerConfig().workerInternalConfig.setProperty(
        WorkerInternalConfig.OP_NAME_INTERNAL, executionVertex.getExecutionJobVertexName());


    OpCheckpointInfo opCheckpointInfo = new OpCheckpointInfo();
    byte[] bytes = null;

    // Fetch checkpoint from storage only in recreate mode not for new startup worker
    // in rescaling or something like that.
    if (isRecreate) {
      String cpKey = genOpCheckpointKey(lastCheckpointId);
      bytes = CheckpointStateUtil.get(checkpointState, cpKey);
    }

    // when use memory state, if actor throw exception, will miss state
    Map<String, OffsetInfo> inputCheckpoints = new HashMap<>();
    Map<String, OffsetInfo> outputCheckpoints = new HashMap<>();
    if (bytes != null) {
      LOG.info("Stream task recover from checkpoint state.");
      opCheckpointInfo = Serializer.decode(bytes);
      inputCheckpoints = opCheckpointInfo.inputPoints;
      outputCheckpoints = opCheckpointInfo.outputPoints;
    }

    // writer
    if (!executionVertex.getInputEdges().isEmpty()) {
      LOG.info("Register queue writer, channels {}.", executionVertex.getOutputChannelIdList());
      writer = new DataWriter(
          executionVertex.getOutputChannelIdList(),
          executionVertex.getOutputActorList(),
          inputCheckpoints,
          lastCheckpointId,
          jobWorker.getWorkerConfig()
      );
    }

    // reader
    if (!executionVertex.getOutputEdges().isEmpty()) {
      LOG.info("Register queue reader, channels {}.", executionVertex.getInputChannelIdList());
      reader = new DataReader(
          executionVertex.getInputChannelIdList(),
          executionVertex.getInputActorList(),
          outputCheckpoints,
          lastCheckpointId,
          jobWorker.getWorkerConfig()
      );
    }

    openProcessor();

    LOG.debug("Finished preparing stream task.");
  }

  /**
   * Create one collector for each distinct output operator(i.e. each {@link ExecutionJobVertex})
   */
  private void openProcessor() {
    ExecutionVertex executionVertex = jobWorker.getExecutionVertex();
    List<ExecutionEdge> outputEdges = executionVertex.getOutputEdges();

    Map<String, List<String>> opGroupedChannelId = new HashMap<>();
    Map<String, List<BaseActorHandle>> opGroupedActor = new HashMap<>();
    Map<String, Partition> opPartitionMap = new HashMap<>();
    for (int i = 0; i < outputEdges.size(); ++i) {
      ExecutionEdge edge = outputEdges.get(i);
      String opName = edge.getTargetExecutionJobVertexName();
      if (!opPartitionMap.containsKey(opName)) {
        opGroupedChannelId.put(opName, new ArrayList<>());
        opGroupedActor.put(opName, new ArrayList<>());
      }
      opGroupedChannelId.get(opName).add(executionVertex.getOutputChannelIdList().get(i));
      opGroupedActor.get(opName).add(executionVertex.getInputActorList().get(i));
      opPartitionMap.put(opName, edge.getPartition());
    }
    opPartitionMap.keySet().forEach(opName -> {
      collectors.add(new OutputCollector(
          writer, opGroupedChannelId.get(opName),
          opGroupedActor.get(opName), opPartitionMap.get(opName)
      ));
    });

    RuntimeContext runtimeContext = new StreamingRuntimeContext(executionVertex,
        jobWorker.getWorkerConfig().configMap, executionVertex.getParallelism());

    processor.open(collectors, runtimeContext);
  }

  /**
   * Task initialization related work.
   */
  protected abstract void init() throws Exception;

  /**
   * Stop running tasks.
   */
  protected void cancelTask() throws Exception {
    running = false;
    while (!stopped) {
      Thread.sleep(100);
    }
  }

  public void start() {
    LOG.info("Start stream task: {}", this.getClass().getSimpleName());
    this.thread.start();
  }

  /**
   * Close running tasks.
   */
  public void close() {
    this.running = false;
    if (thread.isAlive() && !Ray.getRuntimeContext().isSingleProcess()) {
      // `Runtime.halt` is used because System.exist can't ensure the process killing.
      Runtime.getRuntime().halt(0);
      LOG.warn("runtime halt 0");
      System.exit(0);
    }
    LOG.info("Stream task close success.");
  }

  // ----------------------------------------------------------------------
  // Checkpoint
  // ----------------------------------------------------------------------

  public boolean triggerCheckpoint(Barrier barrier) {
    LOG.error("Unsupported access.");
    return false;
  }

  public void doCheckpoint(long checkpointId, Map<String, OffsetInfo> inputPoints) {
    LOG.info("Start do checkpoint, cp id {}, inputPoints {}.", checkpointId, inputPoints);

    Map<String, OffsetInfo> outputPoints = null;
    if (writer != null) {
      outputPoints = writer.getOutputCheckpoints();
      RemoteCall.Barrier barrierPb =
          RemoteCall.Barrier.newBuilder().setId(checkpointId).build();
      ByteBuffer byteBuffer = ByteBuffer.wrap(barrierPb.toByteArray());
      byteBuffer.order(ByteOrder.nativeOrder());
      writer.broadcastBarrier(checkpointId, byteBuffer);
    }
    this.lastCheckpointId = checkpointId;
    Object processorCheckpoint = processor.doCheckpoint(checkpointId);

    try {
      OpCheckpointInfo opCpInfo = new OpCheckpointInfo(inputPoints, outputPoints, processorCheckpoint,
          checkpointId);
      saveCpStateAndReport(opCpInfo, checkpointId);
    } catch (Exception e) {
      // there will be exceptions when flush state to backend.
      // we ignore the exception to prevent failover
      LOG.error("Processor or op checkpoint exception.", e);
    }

    LOG.info("Operator do checkpoint {} finish.", checkpointId);
  }

  private void saveCpStateAndReport(
      OpCheckpointInfo opCheckpointInfo,
      long checkpointId) {
    saveCp(opCheckpointInfo, checkpointId);
    reportCommit(checkpointId);

    LOG.info("Finish save cp state and report, checkpoint id is {}.", checkpointId);
  }

  private void saveCp(OpCheckpointInfo opCheckpointInfo, long checkpointId) {
    byte[] bytes = Serializer.encode(opCheckpointInfo);
    String cpKey = genOpCheckpointKey(checkpointId);
    synchronized (checkpointState) {
      if (outdatedCheckpoints.contains(checkpointId)) {
        LOG.info("Outdated checkpoint, skip save checkpoint.");
        outdatedCheckpoints.remove(checkpointId);
      } else {
        CheckpointStateUtil.put(checkpointState, cpKey, bytes);
      }
    }
  }

  private void reportCommit(long checkpointId) {
    final JobWorkerContext context = jobWorker.getWorkerContext();
    LOG.info("Report commit async, checkpoint id {}.", checkpointId);
    RemoteCallMaster.reportJobWorkerCommitAsync(context.getMaster(),
        new WorkerCommitReport(context.getWorkerActorId(), checkpointId));
  }

  public void notifyCheckpointTimeout(long checkpointId) {
    String cpKey = genOpCheckpointKey(checkpointId);
    try {
      synchronized (checkpointState) {
        if (checkpointState.exists(cpKey)) {
          checkpointState.remove(cpKey);
        } else {
          outdatedCheckpoints.add(checkpointId);
        }
      }
    } catch (Exception e) {
      LOG.error("Notify checkpoint timeout failed, checkpointId is {}.", checkpointId, e);
    }
  }

  public void clearExpiredCpState(long checkpointId) {
    String cpKey = genOpCheckpointKey(checkpointId);
    try {
      checkpointState.remove(cpKey);
    } catch (Exception e) {
      LOG.error("Failed to remove key {} from state backend.", cpKey, e);
    }
  }

  public void clearExpiredQueueMsg(long checkpointId) {
    // get operator checkpoint
    String cpKey = genOpCheckpointKey(checkpointId);
    byte[] bytes;
    try {
      bytes = checkpointState.get(cpKey);
    } catch (Exception e) {
      LOG.error("Failed to get key {} from state backend.", cpKey, e);
      return;
    }
    if (bytes != null) {
      final OpCheckpointInfo opCheckpointInfo = Serializer.decode(bytes);
      long cpId = opCheckpointInfo.checkpointId;
      if (writer != null) {
        writer.clearCheckpoint(cpId);
      }
    }
  }

  public String genOpCheckpointKey(long checkpointId) {
    // TODO: need to support job restart and actorId changed
    final JobWorkerContext context = jobWorker.getWorkerContext();
    return jobWorker.getWorkerConfig().checkpointConfig.jobWorkerOpCpPrefixKey()
        + context.getJobName() + "_" + context.getWorkerName() + "_" + checkpointId;
  }

  // ----------------------------------------------------------------------
  // Failover
  // ----------------------------------------------------------------------
  protected void requestRollback(String exceptionMsg) {
    jobWorker.requestRollback(exceptionMsg);
  }

  public boolean isAlive() {
    return this.thread.isAlive();
  }
}
