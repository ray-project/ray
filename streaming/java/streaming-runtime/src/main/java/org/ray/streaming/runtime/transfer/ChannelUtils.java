package org.ray.streaming.runtime.transfer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import org.ray.api.Ray;
import org.ray.api.id.ActorId;
import org.ray.api.runtime.RayRuntime;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.RayNativeRuntime;
import org.ray.streaming.runtime.generated.Streaming;
import org.ray.streaming.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class ChannelUtils {
  private static final Logger LOGGER = LoggerFactory.getLogger(ChannelUtils.class);

  public static byte[] qidStrToBytes(String qid) {
    byte[] qidBytes = DatatypeConverter.parseHexBinary(qid.toUpperCase());
    assert qidBytes.length == ChannelID.ID_LENGTH;
    return qidBytes;
  }

  public static String qidBytesToString(byte[] qid) {
    assert qid.length == ChannelID.ID_LENGTH;
    return DatatypeConverter.printHexBinary(qid).toLowerCase();
  }

  public static byte[][] stringQueueIdListToByteArray(Collection<String> queueIds) {
    byte[][] res = new byte[queueIds.size()][20];
    int i = 0;
    for (String s : queueIds) {
      res[i] = ChannelUtils.qidStrToBytes(s);
      i++;
    }
    return res;
  }

  public static long[] longToPrimitives(List<Long> arr) {
    long[] res = new long[arr.size()];
    for (int i = 0; i < arr.size(); ++i) {
      res[i] = arr.get(i);
    }
    return res;
  }

  public static String genRandomChannelID() {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < ChannelID.ID_LENGTH * 2; ++i) {
      sb.append((char) (random.nextInt(6) + 'A'));
    }
    return sb.toString();
  }

  /**
   * generate channel name, which must be 20 character
   *
   * @param fromTaskId actor which write into channel
   * @param toTaskId   actor which read from channel
   * @return channel name
   */
  public static String genChannelName(int fromTaskId, int toTaskId, long ts) {
    /*
      | Queue Head | Timestamp | Empty | From  |  To    |
      | 8 bytes    |  4bytes   | 4bytes| 2bytes| 2bytes |
    */
    Preconditions.checkArgument(fromTaskId < Short.MAX_VALUE,
        "fromTaskId %d is larger than %d", fromTaskId, Short.MAX_VALUE);
    Preconditions.checkArgument(toTaskId < Short.MAX_VALUE,
        "toTaskId %d is larger than %d", fromTaskId, Short.MAX_VALUE);
    byte[] queueName = new byte[20];

    for (int i = 11; i >= 8; i--) {
      queueName[i] = (byte) (ts & 0xff);
      ts >>= 8;
    }

    queueName[16] = (byte) ((fromTaskId & 0xffff) >> 8);
    queueName[17] = (byte) (fromTaskId & 0xff);
    queueName[18] = (byte) ((toTaskId & 0xffff) >> 8);
    queueName[19] = (byte) (toTaskId & 0xff);

    return ChannelUtils.qidBytesToString(queueName);
  }

  public static byte[][] actorIdListToByteArray(Collection<ActorId> actorIds) {
    byte[][] res = new byte[actorIds.size()][ActorId.fromRandom().size()];
    int i = 0;
    for (ActorId id : actorIds) {
      res[i] = id.getBytes();
      i++;
    }
    return res;
  }

  public static byte[] toNativeConf(Map<String, String> conf) {
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

  // TODO: do not use reflection
  static long getNativeCoreWorker() {
    long pointer = 0;
    try {
      java.lang.reflect.Field pointerField =
          RayNativeRuntime.class.getDeclaredField("nativeCoreWorkerPointer");
      pointerField.setAccessible(true);
      pointer = (long) pointerField.get(((RayMultiWorkerNativeRuntime) (Ray.internal())).getCurrentRuntime());
    } catch (Exception e) {
      e.printStackTrace();
    }

    return pointer;
  }


}

