package org.ray.streaming.benchmark;

import java.io.Serializable;
import org.ray.streaming.api.context.StreamingContext;
import org.ray.streaming.api.stream.StreamSource;
import org.ray.streaming.util.Record2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * We implement Yahoo Streaming Benchmark on Ray Streaming.
 * Yahoo Streaming Benchmark is #(https://github.com/yahoo/streaming-benchmarks).
 * Common package is copy from ysb source code.
 */
public class YSBBenchmark implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(YSBBenchmark.class);

  public static void main(String[] args) {
    StreamingContext streamingContext = StreamingContext.buildContext();
    StreamSource<String> source = new StreamSource<>(streamingContext, new KafkaSource());
    source.flatMap(new DeserializeBolt())
        .flatMap(new FilterUsingFlatMap())
        .map(record7 -> new Record2(record7.getF2(), record7.getF5()))
        .flatMap(new RedisJoinBolt())
        .keyBy(new MyKeyBySelector())
        .flatMap(new CampaignProcessor())
        .sink(x -> LOGGER.info(x));

    streamingContext.execute();
    try {
      Thread.sleep(1000000000);
    } catch (InterruptedException e) {
    }
  }

}
