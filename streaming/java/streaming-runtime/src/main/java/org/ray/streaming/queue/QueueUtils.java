package org.ray.streaming.queue;

import java.util.Collection;
import java.util.List;
import java.util.Random;

import javax.xml.bind.DatatypeConverter;

import org.ray.api.id.ActorId;

import com.google.common.base.Preconditions;

public class QueueUtils {
  public static byte[] qidStrToBytes(String qid) {
    byte[] qidBytes = DatatypeConverter.parseHexBinary(qid.toUpperCase());
    assert qidBytes.length == QueueID.ID_LENGTH;
    return qidBytes;
  }

  public static String qidBytesToString(byte[] qid) {
    assert qid.length == QueueID.ID_LENGTH;
    return DatatypeConverter.printHexBinary(qid).toLowerCase();
  }

  public static byte[][] stringQueueIdListToByteArray(Collection<String> queueIds) {
    byte[][] res = new byte[queueIds.size()][20];
    int i = 0;
    for (String s : queueIds) {
      res[i] = QueueUtils.qidStrToBytes(s);
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

  public static String getRandomQueueId() {
    StringBuilder sb = new StringBuilder();
    Random random = new Random();
    for (int i = 0; i < QueueID.ID_LENGTH * 2; ++i) {
      sb.append((char) (random.nextInt(6) + 'A'));
    }
    return sb.toString();
  }

  /**
   * generate queue name, which must be 20 character
   *
   * @param fromTaskIndex actor which write into queue
   * @param toTaskIndex   actor which read from queue
   * @return queue name
   */
  public static String genQueueName(int fromTaskIndex, int toTaskIndex, long ts) {
    /*
      | Queue Head | Timestamp | Empty | From  |  To    |
      | 8 bytes    |  4bytes   | 4bytes| 2bytes| 2bytes |
    */
    Preconditions.checkArgument(fromTaskIndex < Short.MAX_VALUE,
        "fromTaskIndex %d is larger than %d", fromTaskIndex, Short.MAX_VALUE);
    Preconditions.checkArgument(toTaskIndex < Short.MAX_VALUE,
        "toTaskIndex %d is larger than %d", fromTaskIndex, Short.MAX_VALUE);
    byte[] queueName = new byte[20];

    for (int i = 11; i >= 8; i--) {
      queueName[i] = (byte) (ts & 0xff);
      ts >>= 8;
    }

    queueName[16] = (byte) ((fromTaskIndex & 0xffff) >> 8);
    queueName[17] = (byte) (fromTaskIndex & 0xff);
    queueName[18] = (byte) ((toTaskIndex & 0xffff) >> 8);
    queueName[19] = (byte) (toTaskIndex & 0xff);

    return QueueUtils.qidBytesToString(queueName);
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
}

