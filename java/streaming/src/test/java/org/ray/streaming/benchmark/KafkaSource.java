package org.ray.streaming.benchmark;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.ray.streaming.api.function.impl.SourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KafkaSource implements SourceFunction<String> {

  private static final Logger LOGGER = LoggerFactory.getLogger(KafkaSource.class);

  private ConsumerIterator<byte[], byte[]> consumerIter;
  private int eventCount = 0;
  private int paralIndex = 0;
  List<KafkaStream<byte[], byte[]>> streams;

  @Override
  public void init(int parallel, int index) {
    LOGGER.info("do init, parallel:{}, index:{}", parallel, index);
    paralIndex = index;

    Properties props = new Properties();

    // NOTE(zhenxuanpan):
    props.put("zookeeper.connect", "");
    props.put("group.id", "");
    props.put("zookeeper.session.timeout.ms", "6000");
    props.put("zookeeper.sync.time.ms", "200");
    props.put("auto.commit.interval.ms", "1000");
    props.put("bootstrap.servers", "");
    props.put("auto.offset.reset", "");
    String topic = "";

    ConsumerConnector consumer = kafka.consumer.Consumer
        .createJavaConsumerConnector(new ConsumerConfig(props));
    Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
    topicCountMap.put(topic, new Integer(1));
    Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
        .createMessageStreams(topicCountMap);

    KafkaStream<byte[], byte[]> stream = consumerMap.get(topic).get(0);
    consumerIter = stream.iterator();
  }

  @Override
  public void fetch(long l, SourceContext<String> sourceContext) throws Exception {
    if (consumerIter.hasNext()) {
      MessageAndMetadata<byte[], byte[]> data = consumerIter.next();
      String topic = data.topic();
      int partition = data.partition();
      long offset = data.offset();
      String msg = new String(data.message());
      if (eventCount % 100 == 0 || eventCount == 0) {
        LOGGER.info("DATA: topic:{} partition:{} offset:{} msg:{} eventCount: {}", topic, partition,
            offset, msg, eventCount);
      }
      eventCount++;
      sourceContext.collect(msg);
    } else {
      LOGGER.info("Alert! no data.");
    }
  }

  @Override
  public void close() {

  }

}
