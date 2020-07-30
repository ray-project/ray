package io.ray.streaming.runtime.transfer.channel;

import com.google.common.base.MoreObjects;
import java.io.Serializable;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ChannelRecoverInfo implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ChannelRecoverInfo.class);
  public Map<String, QueueCreationStatus> queueCreationStatusMap;


  public ChannelRecoverInfo(Map<String, QueueCreationStatus> queueCreationStatusMap) {
    this.queueCreationStatusMap = queueCreationStatusMap;
  }

  public Set<String> getDataLostQueues() {
    Set<String> dataLostQueues = new HashSet<>();
    queueCreationStatusMap.forEach((q, status) -> {
      if (status.equals(QueueCreationStatus.DataLost)) {
        dataLostQueues.add(q);
      }
    });
    return dataLostQueues;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("dataLostQueues", getDataLostQueues())
        .toString();
  }

  public enum QueueCreationStatus {
    FreshStarted(0),
    PullOk(1),
    Timeout(2),
    DataLost(3);

    private int id;

    QueueCreationStatus(int id) {
      this.id = id;
    }

    public static QueueCreationStatus fromInt(int id) {
      for (QueueCreationStatus status : QueueCreationStatus.values()) {
        if (status.id == id) {
          return status;
        }
      }
      return null;
    }
  }
}
