package io.ray.streaming.runtime.core.collector;

import io.ray.api.BaseActor;
import io.ray.api.RayActor;
import io.ray.api.RayPyActor;
import io.ray.streaming.api.Language;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.message.Record;
import io.ray.streaming.runtime.serialization.CrossLangSerializer;
import io.ray.streaming.runtime.serialization.JavaSerializer;
import io.ray.streaming.runtime.serialization.Serializer;
import io.ray.streaming.runtime.transfer.ChannelID;
import io.ray.streaming.runtime.transfer.DataWriter;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputCollector implements Collector<Record> {
  private static final Logger LOGGER = LoggerFactory.getLogger(OutputCollector.class);

  private final DataWriter writer;
  private final ChannelID[] outputQueues;
  private final Collection<BaseActor> targetActors;
  private final Language[] targetLanguages;
  private final Partition partition;
  private final Serializer javaSerializer = new JavaSerializer();
  private final Serializer crossLangSerializer = new CrossLangSerializer();

  public OutputCollector(DataWriter writer,
                         Collection<String> outputQueueIds,
                         Collection<BaseActor> targetActors,
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

  @Override
  public void collect(Record record) {
    int[] partitions = this.partition.partition(record, outputQueues.length);
    ByteBuffer javaBuffer = null;
    ByteBuffer crossLangBuffer = null;
    for (int partition : partitions) {
      if (targetLanguages[partition] == Language.JAVA) {
        // avoid repeated serialization
        if (javaBuffer == null) {
          byte[] bytes = javaSerializer.serialize(record);
          javaBuffer = ByteBuffer.allocate(1 + bytes.length);
          javaBuffer.put(Serializer.JAVA_TYPE_ID);
          // TODO(mubai) remove copy
          javaBuffer.put(bytes);
          javaBuffer.flip();
        }
        writer.write(outputQueues[partition], javaBuffer.duplicate());
      } else {
        // avoid repeated serialization
        if (crossLangBuffer == null) {
          byte[] bytes = crossLangSerializer.serialize(record);
          crossLangBuffer = ByteBuffer.allocate(1 + bytes.length);
          crossLangBuffer.put(Serializer.CROSS_LANG_TYPE_ID);
          // TODO(mubai) remove copy
          crossLangBuffer.put(bytes);
          crossLangBuffer.flip();
        }
        writer.write(outputQueues[partition], crossLangBuffer.duplicate());
      }
    }
  }

}
