package io.ray.streaming.runtime.worker.tasks;

import com.google.common.base.MoreObjects;
import io.ray.streaming.runtime.core.processor.Processor;
import io.ray.streaming.runtime.generated.RemoteCall;
import io.ray.streaming.runtime.serialization.CrossLangSerializer;
import io.ray.streaming.runtime.serialization.JavaSerializer;
import io.ray.streaming.runtime.serialization.Serializer;
import io.ray.streaming.runtime.transfer.channel.OffsetInfo;
import io.ray.streaming.runtime.transfer.exception.ChannelInterruptException;
import io.ray.streaming.runtime.transfer.message.BarrierMessage;
import io.ray.streaming.runtime.transfer.message.ChannelMessage;
import io.ray.streaming.runtime.transfer.message.DataMessage;
import io.ray.streaming.runtime.worker.JobWorker;
import java.util.Map;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class InputStreamTask extends StreamTask {

  private static final Logger LOG = LoggerFactory.getLogger(InputStreamTask.class);

  private final io.ray.streaming.runtime.serialization.Serializer javaSerializer;
  private final io.ray.streaming.runtime.serialization.Serializer crossLangSerializer;
  private final long readTimeoutMillis;

  public InputStreamTask(Processor processor, JobWorker jobWorker, long lastCheckpointId) {
    super(processor, jobWorker, lastCheckpointId);
    readTimeoutMillis = jobWorker.getWorkerConfig().transferConfig.readerTimerIntervalMs();
    javaSerializer = new JavaSerializer();
    crossLangSerializer = new CrossLangSerializer();
  }

  @Override
  protected void init() {}

  @Override
  public void run() {
    try {
      while (running) {
        ChannelMessage item;

        // reader.read() will change the consumer state once it got an item. This lock is to
        // ensure worker can get correct isInitialState value in exactly-once-mode's rollback.
        synchronized (jobWorker.initialStateChangeLock) {
          item = reader.read(readTimeoutMillis);
          if (item != null) {
            isInitialState = false;
          } else {
            continue;
          }
        }

        if (item instanceof DataMessage) {
          DataMessage dataMessage = (DataMessage) item;
          byte[] bytes = new byte[dataMessage.body().remaining() - 1];
          byte typeId = dataMessage.body().get();
          dataMessage.body().get(bytes);
          Object obj;
          if (typeId == Serializer.JAVA_TYPE_ID) {
            obj = javaSerializer.deserialize(bytes);
          } else {
            obj = crossLangSerializer.deserialize(bytes);
          }
          processor.process(obj);
        } else if (item instanceof BarrierMessage) {
          final BarrierMessage queueBarrier = (BarrierMessage) item;
          byte[] barrierData = new byte[queueBarrier.getData().remaining()];
          queueBarrier.getData().get(barrierData);
          RemoteCall.Barrier barrierPb = RemoteCall.Barrier.parseFrom(barrierData);
          final long checkpointId = barrierPb.getId();
          LOG.info(
              "Start to do checkpoint {}, worker name is {}.",
              checkpointId,
              jobWorker.getWorkerContext().getWorkerName());

          final Map<String, OffsetInfo> inputPoints = queueBarrier.getInputOffsets();
          doCheckpoint(checkpointId, inputPoints);
          LOG.info("Do checkpoint {} success.", checkpointId);
        }
      }
    } catch (Throwable throwable) {
      if (throwable instanceof ChannelInterruptException
          || ExceptionUtils.getRootCause(throwable) instanceof ChannelInterruptException) {
        LOG.info("queue has stopped.");
      } else {
        // error occurred, need to rollback
        LOG.error("Last success checkpointId={}, now occur error.", lastCheckpointId, throwable);
        requestRollback(ExceptionUtils.getStackTrace(throwable));
      }
    }
    LOG.info("Input stream task thread exit.");
    stopped = true;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this).add("processor", processor).toString();
  }
}
