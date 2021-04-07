package io.ray.streaming.runtime.core.collector;

import io.ray.api.BaseActorHandle;
import io.ray.api.PyActorHandle;
import io.ray.streaming.api.Language;
import io.ray.streaming.api.collector.Collector;
import io.ray.streaming.api.partition.Partition;
import io.ray.streaming.message.Record;
import io.ray.streaming.runtime.serialization.CrossLangSerializer;
import io.ray.streaming.runtime.serialization.JavaSerializer;
import io.ray.streaming.runtime.serialization.Serializer;
import io.ray.streaming.runtime.transfer.DataWriter;
import io.ray.streaming.runtime.transfer.channel.ChannelId;
import java.nio.ByteBuffer;
import java.util.Collection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OutputCollector implements Collector<Record> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OutputCollector.class);

  private final DataWriter writer;
  private final ChannelId[] outputQueues;
  private final Collection<BaseActorHandle> targetActors;
  private final Language[] targetLanguages;
  private final Partition partition;
  private final Serializer javaSerializer = new JavaSerializer();
  private final Serializer crossLangSerializer = new CrossLangSerializer();

  public OutputCollector(
      DataWriter writer,
      Collection<String> outputChannelIds,
      Collection<BaseActorHandle> targetActors,
      Partition partition) {
    this.writer = writer;
    this.outputQueues = outputChannelIds.stream().map(ChannelId::from).toArray(ChannelId[]::new);
    this.targetActors = targetActors;
    this.targetLanguages =
        targetActors.stream()
            .map(actor -> actor instanceof PyActorHandle ? Language.PYTHON : Language.JAVA)
            .toArray(Language[]::new);
    this.partition = partition;
    LOGGER.debug(
        "OutputCollector constructed, outputChannelIds:{}, partition:{}.",
        outputChannelIds,
        this.partition);
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
          // TODO(chaokunyang) remove copy
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
          // TODO(chaokunyang) remove copy
          crossLangBuffer.put(bytes);
          crossLangBuffer.flip();
        }
        writer.write(outputQueues[partition], crossLangBuffer.duplicate());
      }
    }
  }
}
