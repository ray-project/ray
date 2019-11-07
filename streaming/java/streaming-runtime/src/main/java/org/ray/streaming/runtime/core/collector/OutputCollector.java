package org.ray.streaming.runtime.core.collector;

import java.nio.ByteBuffer;
import java.util.Collection;

import org.ray.runtime.util.Serializer;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.runtime.queue.QueueID;
import org.ray.streaming.runtime.queue.QueueProducer;
import org.ray.streaming.message.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputCollector implements Collector<Record> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OutputCollector.class);

  private Partition partition;
  private QueueProducer producer;
  private QueueID[] outputQueues;

  public OutputCollector(Collection<String> outputQueueIds, QueueProducer producer, Partition partition) {
    this.outputQueues = outputQueueIds.stream().map(QueueID::from).toArray(QueueID[]::new);
    this.producer = producer;
    this.partition = partition;
    LOGGER.debug("OutputCollector constructed, outputQueueIds:{}, partition:{}.", outputQueueIds, this.partition);
  }

  @Override
  public void collect(Record record) {
    int[] partitions = this.partition.partition(record, outputQueues.length);
    ByteBuffer msgBuffer = ByteBuffer.wrap(Serializer.encode(record));
    for (int partition : partitions) {
      producer.produce(outputQueues[partition], msgBuffer);
    }
  }

}
