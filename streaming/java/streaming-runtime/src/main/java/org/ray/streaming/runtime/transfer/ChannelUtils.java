package org.ray.streaming.runtime.transfer;

import java.util.Map;
import org.ray.streaming.runtime.generated.Streaming;
import org.ray.streaming.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChannelUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelUtils.class);

  static byte[] toNativeConf(Map<String, String> conf) {
    Streaming.StreamingConfig.Builder builder = Streaming.StreamingConfig.newBuilder();
    if (conf.containsKey(Config.STREAMING_JOB_NAME)) {
      builder.setJobName(conf.get(Config.STREAMING_JOB_NAME));
    }
    if (conf.containsKey(Config.TASK_JOB_ID)) {
      builder.setTaskJobId(conf.get(Config.TASK_JOB_ID));
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
    Streaming.StreamingConfig streamingConf = builder.build();
    LOGGER.info("Streaming native conf {}", streamingConf.toString());
    return streamingConf.toByteArray();
  }

}

