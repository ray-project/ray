package org.ray.streaming.runtime.queue.memory;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;

import org.ray.streaming.runtime.queue.QueueID;
import org.ray.streaming.runtime.queue.QueueProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemQueueProducerImpl implements QueueProducer {

  private final static Logger LOG = LoggerFactory.getLogger(MemQueueProducerImpl.class);
  private final static int QUEUE_SIZE_MAX = 100;
  private Set<String> outputQueueIds = new HashSet<>();
  private Map<String, Queue<Object>> msgQueueMap;

  public MemQueueProducerImpl(
      Collection<String> outputQueueIds,
      Map<String, Queue<Object>> msgQueueMap, Map<String, String> conf) {
    this.outputQueueIds.addAll(outputQueueIds);
    this.msgQueueMap = msgQueueMap;
    outputQueueIds.forEach(qidItem -> {
      Queue<Object> queue = msgQueueMap.get(qidItem);
      if (null == queue) {
        queue = new ConcurrentLinkedQueue<>();
        msgQueueMap.put(qidItem, queue);
      }
    });
  }

  @Override
  public void produce(QueueID qid, ByteBuffer item) {
    String id = qid.toString();
    LOG.debug("Produce item for queue id: {}, item: {}.", id, item);
    Queue<Object> queue = msgQueueMap.get(id);
    if (null == queue) {
      queue = new ConcurrentLinkedQueue<>();
      msgQueueMap.put(id, queue);
    }

    // check if back pressure
    while (queue.size() > QUEUE_SIZE_MAX) {
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (Exception e) {
        // do nothing
      }
    }

    queue.add(new MemQueueMessageImpl(id, item));
  }

  @Override
  public void produce(Set<QueueID> qids, ByteBuffer item) {
    qids.forEach(qid -> produce(qid, item.duplicate()));
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public void stop() {

  }

}
