//package org.ray.streaming.runtime.streamingqueue;
//
//import org.ray.streaming.runtime.queue.QueueLink;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//
//public class Worker {
//  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);
//
//  protected QueueLink queueLink = null;
//  public void onStreamingTransfer(byte[] buffer) {
//    LOGGER.info("onStreamingTransfer called, buffer size: {}", buffer.length);
//    queueLink.onQueueTransfer(buffer);
//  }
//
//  public byte[] onStreamingTransferSync(byte[] buffer) {
//    LOGGER.info("onStreamingTransferSync called, buffer size: {}", buffer.length);
//    if (queueLink == null) {
//      return new byte[1];
//    }
//    return queueLink.onQueueTransferSync(buffer);
//  }
//}
