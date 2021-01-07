package io.ray.streaming.runtime.worker.tasks;

import io.ray.streaming.operator.SourceOperator;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.core.processor.SourceProcessor;
import io.ray.streaming.runtime.transfer.exception.ChannelInterruptException;
import io.ray.streaming.runtime.worker.JobWorker;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceStreamTask extends StreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStreamTask.class);

  private final SourceProcessor sourceProcessor;

  /** The pending barrier ID to be triggered. */
  private final AtomicReference<Long> pendingBarrier = new AtomicReference<>();

  private long lastCheckpointId = 0;

  /**
   * SourceStreamTask for executing a {@link SourceOperator}. It is responsible for running the
   * corresponding source operator.
   */
  public SourceStreamTask(Processor sourceProcessor, JobWorker jobWorker, long lastCheckpointId) {
    super(sourceProcessor, jobWorker, lastCheckpointId);
    this.sourceProcessor = (SourceProcessor) processor;
  }

  @Override
  protected void init() {}

  @Override
  public void run() {
    LOG.info("Source stream task thread start.");
    Long barrierId;
    try {
      while (running) {
        isInitialState = false;

        // check checkpoint
        barrierId = pendingBarrier.get();
        if (barrierId != null) {
          // Important: because cp maybe timeout, master will use the old checkpoint id again
          if (pendingBarrier.compareAndSet(barrierId, null)) {
            // source fetcher only have outputPoints
            LOG.info(
                "Start to do checkpoint {}, worker name is {}.",
                barrierId,
                jobWorker.getWorkerContext().getWorkerName());

            doCheckpoint(barrierId, null);

            LOG.info("Finish to do checkpoint {}.", barrierId);
          } else {
            // pendingCheckpointId has modify, should not happen
            LOG.warn(
                "Pending checkpointId modify unexpected, expect={}, now={}.",
                barrierId,
                pendingBarrier.get());
          }
        }

        sourceProcessor.fetch();
      }
    } catch (Throwable e) {
      if (e instanceof ChannelInterruptException
          || ExceptionUtils.getRootCause(e) instanceof ChannelInterruptException) {
        LOG.info("queue has stopped.");
      } else {
        // occur error, need to rollback
        LOG.error("Last success checkpointId={}, now occur error.", lastCheckpointId, e);
        requestRollback(ExceptionUtils.getStackTrace(e));
      }
    }

    LOG.info("Source stream task thread exit.");
  }

  @Override
  public boolean triggerCheckpoint(Long barrierId) {
    return pendingBarrier.compareAndSet(null, barrierId);
  }
}
