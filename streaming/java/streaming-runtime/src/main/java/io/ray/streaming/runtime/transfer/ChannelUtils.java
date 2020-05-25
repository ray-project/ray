package io.ray.streaming.runtime.transfer;

import io.ray.streaming.runtime.generated.Streaming;
import io.ray.streaming.util.Config;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelUtils.class);

  static byte[] toNativeConf(Map<String, String> conf) {
    Streaming.StreamingConfig.Builder builder = Streaming.StreamingConfig.newBuilder();
    if (conf.containsKey(Config.STREAMING_JOB_NAME)) {
      builder.setJobName(conf.get(Config.STREAMING_JOB_NAME));
    }
    if (conf.containsKey(Config.STREAMING_WORKER_NAME)) {
      builder.setWorkerName(conf.get(Config.STREAMING_WORKER_NAME));
    }
    if (conf.containsKey(Config.STREAMING_OP_NAME)) {
      builder.setOpName(conf.get(Config.STREAMING_OP_NAME));
    }
    if (conf.containsKey(Config.STREAMING_RING_BUFFER_CAPACITY)) {
      builder.setRingBufferCapacity(
          Integer.parseInt(conf.get(Config.STREAMING_RING_BUFFER_CAPACITY)));
    }
    if (conf.containsKey(Config.STREAMING_EMPTY_MESSAGE_INTERVAL)) {
      builder.setEmptyMessageInterval(
          Integer.parseInt(conf.get(Config.STREAMING_EMPTY_MESSAGE_INTERVAL)));
    }
    if (conf.containsKey(Config.FLOW_CONTROL_TYPE)) {
      builder.setFlowControlType(
          Streaming.FlowControlType.forNumber(
              Integer.parseInt(conf.get(Config.FLOW_CONTROL_TYPE))));
    }
    if (conf.containsKey(Config.WRITER_CONSUMED_STEP)) {
      builder.setWriterConsumedStep(
          Integer.parseInt(conf.get(Config.WRITER_CONSUMED_STEP)));
    }
    if (conf.containsKey(Config.READER_CONSUMED_STEP)) {
      builder.setReaderConsumedStep(
          Integer.parseInt(conf.get(Config.READER_CONSUMED_STEP)));
    }
    Streaming.StreamingConfig streamingConf = builder.build();
    LOGGER.info("Streaming native conf {}", streamingConf.toString());
    return streamingConf.toByteArray();
  }

}

