package io.ray.streaming.runtime.core.collector;

import io.ray.runtime.serializer.Serializer;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.message.Record;
import io.ray.streaming.runtime.transfer.ChannelID;
import io.ray.streaming.runtime.transfer.DataWriter;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputCollector implements Collector<Record> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OutputCollector.class);

  private Partition partition;
  private DataWriter writer;
  private ChannelID[] outputQueues;

  public OutputCollector(Collection<String> outputQueueIds,
                         DataWriter writer,
                         Partition partition) {
    this.outputQueues = outputQueueIds.stream().map(ChannelID::from).toArray(ChannelID[]::new);
    this.writer = writer;
    this.partition = partition;
    LOGGER.debug("OutputCollector constructed, outputQueueIds:{}, partition:{}.",
        outputQueueIds, this.partition);
  }

  @Override
  public void collect(Record record) {
    int[] partitions = this.partition.partition(record, outputQueues.length);
    ByteBuffer msgBuffer = ByteBuffer.wrap(Serializer.encode(record).getLeft());
    for (int partition : partitions) {
      writer.write(outputQueues[partition], msgBuffer);
    }
  }

}
