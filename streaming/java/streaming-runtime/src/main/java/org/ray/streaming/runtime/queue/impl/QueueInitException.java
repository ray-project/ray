package org.ray.streaming.runtime.queue.impl;

import java.util.ArrayList;
import java.util.List;

import org.ray.streaming.runtime.queue.QueueUtils;

public class QueueInitException extends Exception {

  private final List<byte[]> abnormalQueues;

  public QueueInitException(String message, List<byte[]> abnormalQueues) {
    super(message);
    this.abnormalQueues = abnormalQueues;
  }

  public List<byte[]> getAbnormalQueues() {
    return abnormalQueues;
  }

  public List<String> getAbnormalQueuesString() {
    List<String> res = new ArrayList<>();
    abnormalQueues.forEach(ele -> res.add(QueueUtils.qidBytesToString(ele)));
    return res;
  }
}
