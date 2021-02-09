package io.ray.streaming.runtime.worker;

import io.ray.api.Ray;
import io.ray.streaming.runtime.config.StreamingWorkerConfig;
import io.ray.streaming.runtime.config.types.TransferChannelType;
import io.ray.streaming.runtime.context.ContextBackend;
import io.ray.streaming.runtime.context.ContextBackendFactory;
import io.ray.streaming.runtime.core.graph.executiongraph.ExecutionVertex;
import io.ray.streaming.runtime.core.processor.OneInputProcessor;
import io.ray.streaming.runtime.core.processor.ProcessBuilder;
import io.ray.streaming.runtime.core.processor.SourceProcessor;
import io.ray.streaming.runtime.core.processor.StreamProcessor;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.master.coordinator.command.WorkerRollbackRequest;
import io.ray.streaming.runtime.message.CallResult;
import io.ray.streaming.runtime.rpc.RemoteCallMaster;
import io.ray.streaming.runtime.transfer.TransferHandler;
import io.ray.streaming.runtime.transfer.channel.ChannelRecoverInfo;
import io.ray.streaming.runtime.transfer.channel.ChannelRecoverInfo.ChannelCreationStatus;
import io.ray.streaming.runtime.util.CheckpointStateUtil;
import io.ray.streaming.runtime.util.EnvUtil;
import io.ray.streaming.runtime.util.Serializer;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import io.ray.streaming.runtime.worker.tasks.OneInputStreamTask;
import io.ray.streaming.runtime.worker.tasks.SourceStreamTask;
import io.ray.streaming.runtime.worker.tasks.StreamTask;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The streaming worker implementation class, it is ray actor. JobWorker is created by {@link
 * JobMaster} through ray api, and JobMaster communicates with JobWorker through Ray.call().
 *
 * <p>The JobWorker is responsible for creating tasks and defines the methods of communication
 * between workers.
 */
public class JobWorker implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(JobWorker.class);

  // special flag to indicate this actor not ready
  private static final byte[] NOT_READY_FLAG = new byte[4];

  static {
    EnvUtil.loadNativeLibraries();
  }

  public final Object initialStateChangeLock = new Object();
  /** isRecreate=true means this worker is initialized more than once after actor created. */
  public AtomicBoolean isRecreate = new AtomicBoolean(false);

  public ContextBackend contextBackend;
  private JobWorkerContext workerContext;
  private ExecutionVertex executionVertex;
  private StreamingWorkerConfig workerConfig;
  /** The while-loop thread to read message, process message, and write results */
  private StreamTask task;
  /** transferHandler handles messages by ray direct call */
  private TransferHandler transferHandler;
  /**
   * A flag to avoid duplicated rollback. Becomes true after requesting rollback, set to false when
   * finish rollback.
   */
  private boolean isNeedRollback = false;

  private int rollbackCount = 0;

  public JobWorker(ExecutionVertex executionVertex) {
    LOG.info("Creating job worker.");

    // TODO: the following 3 lines is duplicated with that in init(), try to optimise it later.
    this.executionVertex = executionVertex;
    this.workerConfig = new StreamingWorkerConfig(executionVertex.getWorkerConfig());
    this.contextBackend = ContextBackendFactory.getContextBackend(this.workerConfig);

    LOG.info(
        "Ray.getRuntimeContext().wasCurrentActorRestarted()={}",
        Ray.getRuntimeContext().wasCurrentActorRestarted());
    if (!Ray.getRuntimeContext().wasCurrentActorRestarted()) {
      saveContext();
      LOG.info("Job worker is fresh started, init success.");
      return;
    }

    LOG.info("Begin load job worker checkpoint state.");

    byte[] bytes = CheckpointStateUtil.get(contextBackend, getJobWorkerContextKey());
    if (bytes != null) {
      JobWorkerContext context = Serializer.decode(bytes);
      LOG.info(
          "Worker recover from checkpoint state, byte len={}, context={}.", bytes.length, context);
      init(context);
      requestRollback("LoadCheckpoint request rollback in new actor.");
    } else {
      LOG.error(
          "Worker is reconstructed, but can't load checkpoint. "
              + "Check whether you checkpoint state is reliable. Current checkpoint state is {}.",
          contextBackend.getClass().getName());
    }
  }

  public synchronized void saveContext() {
    byte[] contextBytes = Serializer.encode(workerContext);
    String key = getJobWorkerContextKey();
    LOG.info(
        "Saving context, worker context={}, serialized byte length={}, key={}.",
        workerContext,
        contextBytes.length,
        key);
    CheckpointStateUtil.put(contextBackend, key, contextBytes);
  }

  /** Initialize JobWorker and data communication pipeline. */
  public Boolean init(JobWorkerContext workerContext) {
    // IMPORTANT: some test cases depends on this log to find workers' pid,
    // be careful when changing this log.
    LOG.info(
        "Initiating job worker: {}. Worker context is: {}, pid={}.",
        workerContext.getWorkerName(),
        workerContext,
        EnvUtil.getJvmPid());

    this.workerContext = workerContext;
    this.executionVertex = workerContext.getExecutionVertex();
    this.workerConfig = new StreamingWorkerConfig(executionVertex.getWorkerConfig());
    // init state backend
    this.contextBackend = ContextBackendFactory.getContextBackend(this.workerConfig);

    LOG.info("Initiating job worker succeeded: {}.", workerContext.getWorkerName());
    saveContext();
    return true;
  }

  /**
   * Start worker's stream tasks with specific checkpoint ID.
   *
   * <p>Returns a {@link CallResult} with {@link ChannelRecoverInfo}, contains {@link
   * ChannelCreationStatus} of each input queue.
   */
  public CallResult<ChannelRecoverInfo> rollback(Long checkpointId, Long startRollbackTs) {
    synchronized (initialStateChangeLock) {
      if (task != null
          && task.isAlive()
          && checkpointId == task.lastCheckpointId
          && task.isInitialState) {
        return CallResult.skipped("Task is already in initial state, skip this rollback.");
      }
    }
    long remoteCallCost = System.currentTimeMillis() - startRollbackTs;

    LOG.info(
        "Start rollback[{}], checkpoint is {}, remote call cost {}ms.",
        executionVertex.getExecutionJobVertexName(),
        checkpointId,
        remoteCallCost);

    rollbackCount++;
    if (rollbackCount > 1) {
      isRecreate.set(true);
    }

    try {
      // Init transfer
      TransferChannelType channelType = workerConfig.transferConfig.channelType();
      if (TransferChannelType.NATIVE_CHANNEL == channelType) {
        transferHandler = new TransferHandler();
      }

      if (task != null) {
        // make sure the task is closed
        task.close();
        task = null;
      }

      // create stream task
      task = createStreamTask(checkpointId);
      ChannelRecoverInfo channelRecoverInfo = task.recover(isRecreate.get());
      isNeedRollback = false;

      LOG.info(
          "Rollback job worker success, checkpoint is {}, channelRecoverInfo is {}.",
          checkpointId,
          channelRecoverInfo);

      return CallResult.success(channelRecoverInfo);
    } catch (Exception e) {
      LOG.error("Rollback job worker has exception.", e);
      return CallResult.fail(ExceptionUtils.getStackTrace(e));
    }
  }

  /** Create tasks based on the processor corresponding of the operator. */
  private StreamTask createStreamTask(long checkpointId) {
    StreamTask task;
    StreamProcessor streamProcessor =
        ProcessBuilder.buildProcessor(executionVertex.getStreamOperator());
    LOG.debug("Stream processor created: {}.", streamProcessor);

    if (streamProcessor instanceof SourceProcessor) {
      task = new SourceStreamTask(streamProcessor, this, checkpointId);
    } else if (streamProcessor instanceof OneInputProcessor) {
      task = new OneInputStreamTask(streamProcessor, this, checkpointId);
    } else {
      throw new RuntimeException("Unsupported processor type:" + streamProcessor);
    }
    LOG.info("Stream task created: {}.", task);
    return task;
  }

  // ----------------------------------------------------------------------
  // Checkpoint
  // ----------------------------------------------------------------------

  /** Trigger source job worker checkpoint */
  public Boolean triggerCheckpoint(Long barrierId) {
    LOG.info("Receive trigger, barrierId is {}.", barrierId);
    if (task != null) {
      return task.triggerCheckpoint(barrierId);
    }
    return false;
  }

  public Boolean notifyCheckpointTimeout(Long checkpointId) {
    LOG.info("Notify checkpoint timeout, checkpoint id is {}.", checkpointId);
    if (task != null) {
      task.notifyCheckpointTimeout(checkpointId);
    }
    return true;
  }

  public Boolean clearExpiredCheckpoint(Long expiredStateCpId, Long expiredQueueCpId) {
    LOG.info(
        "Clear expired checkpoint state, checkpoint id is {}; "
            + "Clear expired queue msg, checkpoint id is {}",
        expiredStateCpId,
        expiredQueueCpId);
    if (task != null) {
      if (expiredStateCpId > 0) {
        task.clearExpiredCpState(expiredStateCpId);
      }
      task.clearExpiredQueueMsg(expiredQueueCpId);
    }
    return true;
  }

  // ----------------------------------------------------------------------
  // Failover
  // ----------------------------------------------------------------------
  public void requestRollback(String exceptionMsg) {
    LOG.info("Request rollback.");
    isNeedRollback = true;
    isRecreate.set(true);
    boolean requestRet =
        RemoteCallMaster.requestJobWorkerRollback(
            workerContext.getMaster(),
            new WorkerRollbackRequest(
                workerContext.getWorkerActorId(),
                exceptionMsg,
                EnvUtil.getHostName(),
                EnvUtil.getJvmPid()));
    if (!requestRet) {
      LOG.warn("Job worker request rollback failed! exceptionMsg={}.", exceptionMsg);
    }
  }

  public Boolean checkIfNeedRollback(Long startCallTs) {
    // No save checkpoint in this query.
    long remoteCallCost = System.currentTimeMillis() - startCallTs;
    LOG.info(
        "Finished checking if need to rollback with result: {}, rpc delay={}ms.",
        isNeedRollback,
        remoteCallCost);
    return isNeedRollback;
  }

  public StreamingWorkerConfig getWorkerConfig() {
    return workerConfig;
  }

  public JobWorkerContext getWorkerContext() {
    return workerContext;
  }

  public ExecutionVertex getExecutionVertex() {
    return executionVertex;
  }

  public StreamTask getTask() {
    return task;
  }

  private String getJobWorkerContextKey() {
    return workerConfig.checkpointConfig.jobWorkerContextCpPrefixKey()
        + workerConfig.commonConfig.jobName()
        + "_"
        + executionVertex.getExecutionVertexId();
  }

  /** Used by upstream streaming queue to send data to this actor */
  public void onReaderMessage(byte[] buffer) {
    if (transferHandler != null) {
      transferHandler.onReaderMessage(buffer);
    }
  }

  /**
   * Used by upstream streaming queue to send data to this actor and receive result from this actor
   */
  public byte[] onReaderMessageSync(byte[] buffer) {
    if (transferHandler == null) {
      return NOT_READY_FLAG;
    }
    return transferHandler.onReaderMessageSync(buffer);
  }

  /** Used by downstream streaming queue to send data to this actor */
  public void onWriterMessage(byte[] buffer) {
    if (transferHandler != null) {
      transferHandler.onWriterMessage(buffer);
    }
  }

  /**
   * Used by downstream streaming queue to send data to this actor and receive result from this
   * actor
   */
  public byte[] onWriterMessageSync(byte[] buffer) {
    if (transferHandler == null) {
      return NOT_READY_FLAG;
    }
    return transferHandler.onWriterMessageSync(buffer);
  }
}
