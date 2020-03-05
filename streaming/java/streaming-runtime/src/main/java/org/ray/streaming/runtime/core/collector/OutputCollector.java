package org.ray.streaming.runtime.core.collector;

import java.nio.ByteBuffer;
import java.util.Collection;
import org.ray.api.RayActor;
import org.ray.api.RayPyActor;
import org.ray.streaming.api.Language;
import org.ray.streaming.api.collector.Collector;
import org.ray.streaming.api.partition.Partition;
import org.ray.streaming.message.Record;
import org.ray.streaming.runtime.serialization.JavaSerializer;
import org.ray.streaming.runtime.serialization.XLangSerializer;
import org.ray.streaming.runtime.transfer.ChannelID;
import org.ray.streaming.runtime.transfer.DataWriter;
import org.ray.streaming.runtime.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputCollector implements Collector<Record> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OutputCollector.class);

  private DataWriter writer;
  private ChannelID[] outputQueues;
  private Collection<RayActor> targetActors;
  private Language[] targetLanguages;
  private Partition partition;
  private Serializer javaSerializer = new JavaSerializer();
  private Serializer xLangSerializer = new XLangSerializer();

  public OutputCollector(DataWriter writer,
                         Collection<String> outputQueueIds,
                         Collection<RayActor> targetActors,
                         Partition partition) {
    this.writer = writer;
    this.outputQueues = outputQueueIds.stream().map(ChannelID::from).toArray(ChannelID[]::new);
    this.targetActors = targetActors;
    this.targetLanguages = targetActors.stream()
        .map(actor -> actor instanceof RayPyActor ? Language.PYTHON : Language.JAVA)
        .toArray(Language[]::new);
    this.partition = partition;
    LOGGER.debug("OutputCollector constructed, outputQueueIds:{}, partition:{}.",
        outputQueueIds, this.partition);
  }

  private Serializer[] createSerializers(Collection<RayActor> targetActors) {
    return targetActors.stream()
        .map(actor -> {
          if (actor instanceof RayPyActor) {
            return new XLangSerializer();
          } else {
            return new JavaSerializer();
          }
        }).toArray(Serializer[]::new);
  }

  @Override
  public void collect(Record record) {
    int[] partitions = this.partition.partition(record, outputQueues.length);
    ByteBuffer javaBuffer = null;
    ByteBuffer xLangBuffer = null;
    for (int partition : partitions) {
      if (targetLanguages[partition] == Language.JAVA) {
        // avoid repeated serialization
        if (javaBuffer == null) {
          javaBuffer = ByteBuffer.wrap(javaSerializer.serialize(record));
        }
        writer.write(outputQueues[partition], javaBuffer);
      } else {
        // avoid repeated serialization
        if (xLangBuffer == null) {
          xLangBuffer = ByteBuffer.wrap(xLangSerializer.serialize(record));
        }
        writer.write(outputQueues[partition], xLangBuffer);
      }
    }
  }

}
