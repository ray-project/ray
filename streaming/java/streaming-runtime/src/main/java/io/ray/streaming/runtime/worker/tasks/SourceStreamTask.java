package io.ray.streaming.runtime.worker.tasks;

import java.util.concurrent.atomic.AtomicReference;

import io.ray.streaming.operator.SourceOperator;
import io.ray.streaming.runtime.barrier.Barrier;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.core.processor.SourceProcessor;
import io.ray.streaming.runtime.transfer.ChannelInterruptException;
import io.ray.streaming.runtime.worker.JobWorker;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SourceStreamTask extends StreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(SourceStreamTask.class);

  private final SourceProcessor sourceProcessor;
  private final AtomicReference<Barrier> pendingBarrier = new AtomicReference<>();
  private long lastCheckpointId = 0;

  /**
   * SourceStreamTask for executing a {@link SourceOperator}.
   * It is responsible for running the corresponding source operator.
   */
  public SourceStreamTask(Processor sourceProcessor, JobWorker jobWorker) {
    super(sourceProcessor, jobWorker);
    this.sourceProcessor = (SourceProcessor) processor;
  }

  @Override
  protected void init() {
  }

  @Override
  public void run() {
    LOG.info("Source stream task thread start.");
    Barrier barrier;
    try {
      while (running) {
        isInitialState = false;

        // check checkpoint
        barrier = pendingBarrier.get();
        if (barrier != null) {
          // Important: because cp maybe timeout, master will use the old checkpoint id again
          if (pendingBarrier.compareAndSet(barrier, null)) {
            // source fetcher only have outputPoints
            LOG.info("Start to do checkpoint {}, worker name is {}.",
                barrier.getId(), jobWorker.getWorkerContext().getWorkerName());

            doCheckpoint(barrier.getId(), null);

            LOG.info("Finish to do checkpoint {}.", barrier.getId());
          } else {
            // pendingCheckpointId has modify, should not happen
            LOG.warn("Pending checkpointId modify unexpected, expect={}, now={}.", barrier,
                pendingBarrier.get());
          }
        }

        sourceProcessor.fetch(lastCheckpointId + 1);
      }
    } catch (Throwable e) {
      if (e instanceof ChannelInterruptException ||
          ExceptionUtils.getRootCause(e) instanceof ChannelInterruptException) {
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
  public boolean triggerCheckpoint(Barrier barrier) {
    return pendingBarrier.compareAndSet(null, barrier);
  }
}
