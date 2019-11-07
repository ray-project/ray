package org.ray.streaming.runtime.queue.memory;

import java.util.Collection;
import java.util.Map;
import java.util.Queue;

import org.ray.streaming.runtime.queue.QueueConsumer;
import org.ray.streaming.runtime.queue.QueueItem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemQueueConsumerImpl implements QueueConsumer {

  private static final Logger LOG = LoggerFactory.getLogger(MemQueueConsumerImpl.class);

  private String[] inputQueueIds;
  private Map<String, Queue<Object>> msgQueueMap;

  public MemQueueConsumerImpl(Collection<String> inputQueueIds, Map<String, Queue<Object>> msgQueueMap) {
    this.inputQueueIds = inputQueueIds.toArray(new String[0]);
    this.msgQueueMap = msgQueueMap;
  }

  @Override
  public QueueItem pull(long timeoutMillis) {
    int index = 0;
    for (; index < inputQueueIds.length; ++index) {
      String qid = inputQueueIds[index];
      Queue<Object> queue = msgQueueMap.get(qid);
      if (null == queue || queue.isEmpty()) {
        continue;
      }
      break;
    }

    if (index < inputQueueIds.length) {
      String qid = inputQueueIds[index];
      Queue<Object> queue = msgQueueMap.get(qid);
      LOG.debug("consumer item for qid: {}.", qid);
      return (QueueItem) queue.poll();
    }

    return null;
  }

  @Override
  public void close() {
    // do nothing
  }

  @Override
  public void stop() {

  }

}
