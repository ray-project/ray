package org.ray.streaming.queue.memory;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;

import org.ray.api.id.ActorId;
import org.ray.api.runtime.RayRuntime;
import org.ray.streaming.queue.QueueConsumer;
import org.ray.streaming.queue.QueueLink;
import org.ray.streaming.queue.QueueProducer;

public class MemQueueLinkImpl implements QueueLink, Serializable {

  private static final long serialVersionUID = -5005445369174784185L;

  public static Map<String, Queue<Object>> msgQueueMap = new ConcurrentHashMap<>();

  private Map<String, String> conf;

  @Override
  public void setRayRuntime(RayRuntime runtime) {
  }

  @Override
  public void setConfiguration(Map<String, String> conf) {
    this.conf = conf;
  }

  @Override
  public Map<String, String> getConfiguration() {
    return new HashMap<>();
  }

  @Override
  public QueueProducer registerQueueProducer(Collection<String> outputQueueIds,
                                             Map<String, ActorId> outputActorIds) {
    return new MemQueueProducerImpl(outputQueueIds, msgQueueMap, conf);
  }

  @Override
  public QueueConsumer registerQueueConsumer(Collection<String> inputQueueIds,
                                             Map<String, ActorId> inputActorIds) {
    return new MemQueueConsumerImpl(inputQueueIds, msgQueueMap);
  }

  @Override
  public void onQueueTransfer(byte[] buffer) {

  }

  @Override
  public byte[] onQueueTransferSync(byte[] buffer) {
    return new byte[0];
  }

}