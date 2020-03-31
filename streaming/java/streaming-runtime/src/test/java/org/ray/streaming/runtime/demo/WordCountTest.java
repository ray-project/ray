package io.ray.streaming.runtime.demo;

import com.google.common.collect.ImmutableMap;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.function.impl.SinkFunction;
import io.ray.streaming.api.stream.DataStreamSource;
import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.util.Config;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WordCountTest extends BaseUnitTest implements Serializable {

  private static final Logger LOGGER = LoggerFactory.getLogger(WordCountTest.class);

  // TODO(zhenxuanpan): this test only works in single-process mode, because we put
  //   results in this in-memory map.
  static Map<String, Integer> wordCount = new ConcurrentHashMap<>();

  @Test
  public void testWordCount() {
    StreamingContext streamingContext = StreamingContext.buildContext();
    Map<String, String> config = new HashMap<>();
    config.put(Config.STREAMING_BATCH_MAX_COUNT, "1");
    config.put(Config.CHANNEL_TYPE, Config.MEMORY_CHANNEL);
    streamingContext.withConfig(config);
    List<String> text = new ArrayList<>();
    text.add("hello world eagle eagle eagle");
    DataStreamSource<String> streamSource = DataStreamSource.buildSource(streamingContext, text);
    streamSource
        .flatMap((FlatMapFunction<String, WordAndCount>) (value, collector) -> {
          String[] records = value.split(" ");
          for (String record : records) {
            collector.collect(new WordAndCount(record, 1));
          }
        })
        .filter(pair -> !pair.word.contains("world"))
        .keyBy(pair -> pair.word)
        .reduce((ReduceFunction<WordAndCount>) (oldValue, newValue) ->
            new WordAndCount(oldValue.word, oldValue.count + newValue.count))
        .sink((SinkFunction<WordAndCount>)
            result -> wordCount.put(result.word, result.count));

    streamingContext.execute("testWordCount");

    // Sleep until the count for every word is computed.
    while (wordCount.size() < 2) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOGGER.warn("Got an exception while sleeping.", e);
      }
    }
    Assert.assertEquals(wordCount, ImmutableMap.of("eagle", 3, "hello", 1));
  }

  private static class WordAndCount implements Serializable {

    public final String word;
    public final Integer count;

    public WordAndCount(String key, Integer count) {
      this.word = key;
      this.count = count;
    }
  }

}
