package io.ray.streaming.runtime.demo;

import com.google.common.collect.ImmutableMap;
import io.ray.api.Ray;
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
import org.testng.annotations.Test;

public class WordCountTest extends BaseUnitTest implements Serializable {

  private static final Logger LOG = LoggerFactory.getLogger(WordCountTest.class);

  static Map<String, Integer> wordCount = new ConcurrentHashMap<>();

  @Test(timeOut = 60000)
  public void testWordCount() {
    Ray.shutdown();

    StreamingContext streamingContext = StreamingContext.buildContext();
    Map<String, String> config = new HashMap<>();
    config.put(Config.CHANNEL_TYPE, "MEMORY_CHANNEL");
    streamingContext.withConfig(config);
    List<String> text = new ArrayList<>();
    text.add("hello world eagle eagle eagle");
    DataStreamSource<String> streamSource = DataStreamSource.fromCollection(streamingContext, text);
    streamSource
        .flatMap(
            (FlatMapFunction<String, WordAndCount>)
                (value, collector) -> {
                  String[] records = value.split(" ");
                  for (String record : records) {
                    collector.collect(new WordAndCount(record, 1));
                  }
                })
        .filter(pair -> !pair.word.contains("world"))
        .keyBy(pair -> pair.word)
        .reduce(
            (ReduceFunction<WordAndCount>)
                (oldValue, newValue) ->
                    new WordAndCount(oldValue.word, oldValue.count + newValue.count))
        .sink((SinkFunction<WordAndCount>) result -> wordCount.put(result.word, result.count));

    streamingContext.execute("testWordCount");

    ImmutableMap<String, Integer> expected = ImmutableMap.of("eagle", 3, "hello", 1);
    while (!wordCount.equals(expected)) {
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOG.warn("Got an exception while sleeping.", e);
      }
    }
    streamingContext.stop();
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
