package io.ray.streaming.runtime.worker.tasks;

import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.context.RuntimeContext;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.runtime.config.worker.WorkerInternalConfig;
import io.ray.streaming.runtime.context.ContextBackend;
import io.ray.streaming.runtime.context.OperatorCheckpointInfo;
import io.ray.streaming.runtime.core.collector.OutputCollector;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionEdge;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionJobVertex;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.master.coordinator.command.WorkerCommitReport;
import io.ray.streaming.runtime.rpc.RemoteCallMaster;
import io.ray.streaming.runtime.transfer.DataReader;
import io.ray.streaming.runtime.transfer.DataWriter;
import io.ray.streaming.runtime.transfer.channel.ChannelRecoverInfo;
import io.ray.streaming.runtime.transfer.channel.OffsetInfo;
import io.ray.streaming.runtime.util.CheckpointStateUtil;
import io.ray.streaming.runtime.util.Serializer;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import io.ray.streaming.runtime.worker.context.StreamingRuntimeContext;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link StreamTask} is a while-loop thread to read message, process message, and send result
 * messages to downstream operators
 */
public abstract class StreamTask implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(StreamTask.class);
  private final ContextBackend checkpointState;
  public volatile boolean isInitialState = true;
  public long lastCheckpointId;
  protected Processor processor;
  protected JobWorker jobWorker;
  protected DataReader reader;
  protected DataWriter writer;
  protected volatile boolean running = true;
  protected volatile boolean stopped = false;
  List<Collector> collectors = new ArrayList<>();
  private Set<Long> outdatedCheckpoints = new HashSet<>();
  private Thread thread;

  protected StreamTask(Processor processor, JobWorker jobWorker, long lastCheckpointId) {
    this.processor = processor;
    this.jobWorker = jobWorker;
    this.checkpointState = jobWorker.contextBackend;
    this.lastCheckpointId = lastCheckpointId;

    this.thread =
        new Thread(
            Ray.wrapRunnable(this), this.getClass().getName() + "-" + System.currentTimeMillis());
    this.thread.setDaemon(true);
  }

  public ChannelRecoverInfo recover(boolean isRecover) {

    if (isRecover) {
      LOG.info("Stream task begin recover.");
    } else {
      LOG.info("Stream task first start begin.");
    }
    prepareTask(isRecover);

    // start runner
    ChannelRecoverInfo recoverInfo = new ChannelRecoverInfo(new HashMap<>());
    if (reader != null) {
      recoverInfo = reader.getQueueRecoverInfo();
    }

    thread.setUncaughtExceptionHandler(
        (t, e) -> LOG.error("Uncaught exception in runner thread.", e));
    LOG.info("Start stream task: {}.", this.getClass().getSimpleName());
    thread.start();

    if (isRecover) {
      LOG.info("Stream task recover end.");
    } else {
      LOG.info("Stream task first start finished.");
    }

    return recoverInfo;
  }

  /**
   * Load checkpoint and build upstream and downstream data transmission channels according to
   * {@link ExecutionVertex}.
   */
  private void prepareTask(boolean isRecreate) {
    LOG.info("Preparing stream task, isRecreate={}.", isRecreate);
    ExecutionVertex executionVertex = jobWorker.getExecutionVertex();

    // set vertex info into config for native using
    jobWorker
        .getWorkerConfig()
        .workerInternalConfig
        .setProperty(
            WorkerInternalConfig.WORKER_NAME_INTERNAL, executionVertex.getExecutionVertexName());
    jobWorker
        .getWorkerConfig()
        .workerInternalConfig
        .setProperty(
            WorkerInternalConfig.OP_NAME_INTERNAL, executionVertex.getExecutionJobVertexName());

    OperatorCheckpointInfo operatorCheckpointInfo = new OperatorCheckpointInfo();
    byte[] bytes = null;

    // Fetch checkpoint from storage only in recreate mode not for new startup worker
    // in rescaling or something like that.
    if (isRecreate) {
      String cpKey = genOpCheckpointKey(lastCheckpointId);
      LOG.info(
          "Getting task checkpoints from state, cpKey={}, checkpointId={}.",
          cpKey,
          lastCheckpointId);
      bytes = CheckpointStateUtil.get(checkpointState, cpKey);
      if (bytes == null) {
        String msg = String.format("Task recover failed, checkpoint is null! cpKey=%s", cpKey);
        throw new RuntimeException(msg);
      }
    }

    // when use memory state, if actor throw exception, will miss state
    if (bytes != null) {
      operatorCheckpointInfo = Serializer.decode(bytes);
      processor.loadCheckpoint(operatorCheckpointInfo.processorCheckpoint);
      LOG.info(
          "Stream task recover from checkpoint state, checkpoint bytes len={}, checkpointInfo={}.",
          bytes.length,
          operatorCheckpointInfo);
    }

    // writer
    if (!executionVertex.getOutputEdges().isEmpty()) {
      LOG.info(
          "Register queue writer, channels={}, outputCheckpoints={}.",
          executionVertex.getOutputChannelIdList(),
          operatorCheckpointInfo.outputPoints);
      writer =
          new DataWriter(
              executionVertex.getOutputChannelIdList(),
              executionVertex.getOutputActorList(),
              operatorCheckpointInfo.outputPoints,
              jobWorker.getWorkerConfig());
    }

    // reader
    if (!executionVertex.getInputEdges().isEmpty()) {
      LOG.info(
          "Register queue reader, channels={}, inputCheckpoints={}.",
          executionVertex.getInputChannelIdList(),
          operatorCheckpointInfo.inputPoints);
      reader =
          new DataReader(
              executionVertex.getInputChannelIdList(),
              executionVertex.getInputActorList(),
              operatorCheckpointInfo.inputPoints,
              jobWorker.getWorkerConfig());
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
      opGroupedActor.get(opName).add(executionVertex.getOutputActorList().get(i));
      opPartitionMap.put(opName, edge.getPartition());
    }
    opPartitionMap
        .keySet()
        .forEach(
            opName -> {
              collectors.add(
                  new OutputCollector(
                      writer,
                      opGroupedChannelId.get(opName),
                      opGroupedActor.get(opName),
                      opPartitionMap.get(opName)));
            });

    RuntimeContext runtimeContext =
        new StreamingRuntimeContext(
            executionVertex,
            jobWorker.getWorkerConfig().configMap,
            executionVertex.getParallelism());

    processor.open(collectors, runtimeContext);
  }

  /** Task initialization related work. */
  protected abstract void init() throws Exception;

  /** Close running tasks. */
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

  public boolean triggerCheckpoint(Long barrierId) {
    throw new UnsupportedOperationException("Only source operator supports trigger checkpoints.");
  }

  public void doCheckpoint(long checkpointId, Map<String, OffsetInfo> inputPoints) {
    Map<String, OffsetInfo> outputPoints = null;
    if (writer != null) {
      outputPoints = writer.getOutputCheckpoints();
      RemoteCall.Barrier barrierPb = RemoteCall.Barrier.newBuilder().setId(checkpointId).build();
      ByteBuffer byteBuffer = ByteBuffer.wrap(barrierPb.toByteArray());
      byteBuffer.order(ByteOrder.nativeOrder());
      writer.broadcastBarrier(checkpointId, byteBuffer);
    }

    LOG.info(
        "Start do checkpoint, cp id={}, inputPoints={}, outputPoints={}.",
        checkpointId,
        inputPoints,
        outputPoints);

    this.lastCheckpointId = checkpointId;
    Serializable processorCheckpoint = processor.saveCheckpoint();

    try {
      OperatorCheckpointInfo opCpInfo =
          new OperatorCheckpointInfo(inputPoints, outputPoints, processorCheckpoint, checkpointId);
      saveCpStateAndReport(opCpInfo, checkpointId);
    } catch (Exception e) {
      // there will be exceptions when flush state to backend.
      // we ignore the exception to prevent failover
      LOG.error("Processor or op checkpoint exception.", e);
    }

    LOG.info("Operator do checkpoint {} finish.", checkpointId);
  }

  private void saveCpStateAndReport(
      OperatorCheckpointInfo operatorCheckpointInfo, long checkpointId) {
    saveCp(operatorCheckpointInfo, checkpointId);
    reportCommit(checkpointId);

    LOG.info("Finish save cp state and report, checkpoint id is {}.", checkpointId);
  }

  private void saveCp(OperatorCheckpointInfo operatorCheckpointInfo, long checkpointId) {
    byte[] bytes = Serializer.encode(operatorCheckpointInfo);
    String cpKey = genOpCheckpointKey(checkpointId);
    LOG.info(
        "Saving task checkpoint, cpKey={}, byte len={}, checkpointInfo={}.",
        cpKey,
        bytes.length,
        operatorCheckpointInfo);
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
    RemoteCallMaster.reportJobWorkerCommitAsync(
        context.getMaster(), new WorkerCommitReport(context.getWorkerActorId(), checkpointId));
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
      final OperatorCheckpointInfo operatorCheckpointInfo = Serializer.decode(bytes);
      long cpId = operatorCheckpointInfo.checkpointId;
      if (writer != null) {
        writer.clearCheckpoint(cpId);
      }
    }
  }

  public String genOpCheckpointKey(long checkpointId) {
    // TODO: need to support job restart and actorId changed
    final JobWorkerContext context = jobWorker.getWorkerContext();
    return jobWorker.getWorkerConfig().checkpointConfig.jobWorkerOpCpPrefixKey()
        + context.getJobName()
        + "_"
        + context.getWorkerName()
        + "_"
        + checkpointId;
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
