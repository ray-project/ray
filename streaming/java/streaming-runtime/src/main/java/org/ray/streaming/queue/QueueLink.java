package org.ray.streaming.queue;

import java.util.Collection;
import java.util.Map;

import org.ray.api.id.ActorId;
import org.ray.api.runtime.RayRuntime;

public interface QueueLink {

  /**
   * set ray runtime
   *
   * @param runtime ray runtime
   */
  void setRayRuntime(RayRuntime runtime);

  /**
   * set queue configuration
   *
   * @param conf queue configuration
   */
  void setConfiguration(Map<String, String> conf);

  /**
   * get queue configuration
   *
   * @return conf queue configuration
   */
  Map<String, String> getConfiguration();

  /**
   * get queue consumer of input queues
   *
   * @param inputQueueIds input queue ids
   * @return queue consumer
   */
  QueueConsumer registerQueueConsumer(Collection<String> inputQueueIds, Map<String, ActorId> inputActorIds);

  /**
   * get queue producer of output queues
   *
   * @param outputQueueIds output queue ids
   * @return queue producer
   */
  QueueProducer registerQueueProducer(Collection<String> outputQueueIds, Map<String, ActorId> inputActorIds);

  /**
   * used in direct call mode
   */
  void onQueueTransfer(byte[] buffer);

  /**
   * used in direct call mode
   */
  byte[] onQueueTransferSync(byte[] buffer);
}
