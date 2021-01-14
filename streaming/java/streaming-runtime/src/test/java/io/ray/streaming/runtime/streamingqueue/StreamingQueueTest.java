package io.ray.streaming.runtime.streamingqueue;

import com.google.common.collect.ImmutableMap;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.function.impl.FlatMapFunction;
import io.ray.streaming.api.function.impl.ReduceFunction;
import io.ray.streaming.api.stream.DataStreamSource;
import io.ray.streaming.runtime.BaseUnitTest;
import io.ray.streaming.runtime.transfer.channel.ChannelId;
import io.ray.streaming.runtime.util.EnvUtil;
import io.ray.streaming.util.Config;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class StreamingQueueTest extends BaseUnitTest implements Serializable {

  private static Logger LOGGER = LoggerFactory.getLogger(StreamingQueueTest.class);

  static {
    EnvUtil.loadNativeLibraries();
  }

  @org.testng.annotations.BeforeSuite
  public void suiteSetUp() throws Exception {
    LOGGER.info("Do set up");
    String management = ManagementFactory.getRuntimeMXBean().getName();
    String pid = management.split("@")[0];

    LOGGER.info("StreamingQueueTest pid: {}", pid);
    LOGGER.info("java.library.path = {}", System.getProperty("java.library.path"));
  }

  @org.testng.annotations.AfterSuite
  public void suiteTearDown() throws Exception {
    LOGGER.warn("Do tear down");
  }

  @BeforeMethod
  void beforeMethod() {
    LOGGER.info("beforeTest");
    Ray.shutdown();
    System.setProperty("ray.head-args.0", "--num-cpus=4");
    System.setProperty("ray.head-args.1", "--resources={\"RES-A\":4}");
    System.setProperty("ray.run-mode", "CLUSTER");
    System.setProperty("ray.redirect-output", "true");
    Ray.init();
  }

  @AfterMethod
  void afterMethod() {
    LOGGER.info("afterTest");
    Ray.shutdown();
    System.clearProperty("ray.run-mode");
    System.clearProperty("ray.head-args.0");
    System.clearProperty("ray.head-args.1");
  }

  @Test(timeOut = 300000)
  public void testReaderWriter() {
    LOGGER.info(
        "StreamingQueueTest.testReaderWriter run-mode: {}", System.getProperty("ray.run-mode"));
    Ray.shutdown();
    System.setProperty("ray.head-args.0", "--num-cpus=4");
    System.setProperty("ray.head-args.1", "--resources={\"RES-A\":4}");

    System.setProperty("ray.run-mode", "CLUSTER");
    System.setProperty("ray.redirect-output", "true");
    // ray init
    Ray.init();

    ActorHandle<WriterWorker> writerActor = Ray.actor(WriterWorker::new, "writer").remote();
    ActorHandle<ReaderWorker> readerActor = Ray.actor(ReaderWorker::new, "reader").remote();

    LOGGER.info(
        "call getName on writerActor: {}", writerActor.task(WriterWorker::getName).remote().get());
    LOGGER.info(
        "call getName on readerActor: {}", readerActor.task(ReaderWorker::getName).remote().get());

    // LOGGER.info(writerActor.task(WriterWorker::testCallReader, readerActor).remote().get());
    List<String> outputQueueList = new ArrayList<>();
    List<String> inputQueueList = new ArrayList<>();
    int queueNum = 2;
    for (int i = 0; i < queueNum; ++i) {
      String qid = ChannelId.genRandomIdStr();
      LOGGER.info("getRandomQueueId: {}", qid);
      inputQueueList.add(qid);
      outputQueueList.add(qid);
      readerActor.getId();
    }

    final int msgCount = 100;
    readerActor.task(ReaderWorker::init, inputQueueList, writerActor, msgCount).remote();
    try {
      Thread.sleep(1000);
    } catch (Exception e) {
      e.printStackTrace();
    }
    writerActor.task(WriterWorker::init, outputQueueList, readerActor, msgCount).remote();

    long time = 0;
    while (time < 20000
        && readerActor.task(ReaderWorker::getTotalMsg).remote().get() < msgCount * queueNum) {
      try {
        Thread.sleep(1000);
        time += 1000;
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    Assert.assertEquals(
        readerActor.task(ReaderWorker::getTotalMsg).remote().get().intValue(), msgCount * queueNum);
  }

  @Test(timeOut = 60000)
  public void testWordCount() {
    Ray.shutdown();
    System.setProperty("ray.head-args.0", "--num-cpus=4");
    System.setProperty("ray.head-args.1", "--resources={\"RES-A\":4}");

    System.setProperty("ray.run-mode", "CLUSTER");
    System.setProperty("ray.redirect-output", "true");
    // ray init
    Ray.init();
    LOGGER.info("testWordCount");
    LOGGER.info(
        "StreamingQueueTest.testWordCount run-mode: {}", System.getProperty("ray.run-mode"));
    String resultFile = "/tmp/io.ray.streaming.runtime.streamingqueue.testWordCount.txt";
    deleteResultFile(resultFile);

    Map<String, Integer> wordCount = new ConcurrentHashMap<>();
    StreamingContext streamingContext = StreamingContext.buildContext();
    Map<String, String> config = new HashMap<>();
    config.put(Config.CHANNEL_TYPE, "NATIVE_CHANNEL");
    config.put(Config.CHANNEL_SIZE, "100000");
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
        .keyBy(pair -> pair.word)
        .reduce(
            (ReduceFunction<WordAndCount>)
                (oldValue, newValue) -> {
                  LOGGER.info("reduce: {} {}", oldValue, newValue);
                  return new WordAndCount(oldValue.word, oldValue.count + newValue.count);
                })
        .sink(
            s -> {
              LOGGER.info("sink {} {}", s.word, s.count);
              wordCount.put(s.word, s.count);
              serializeResultToFile(resultFile, wordCount);
            });

    streamingContext.execute("testSQWordCount");

    Map<String, Integer> checkWordCount =
        (Map<String, Integer>) deserializeResultFromFile(resultFile);
    // Sleep until the count for every word is computed.
    while (checkWordCount == null || checkWordCount.size() < 3) {
      LOGGER.info("sleep");
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        LOGGER.warn("Got an exception while sleeping.", e);
      }
      checkWordCount = (Map<String, Integer>) deserializeResultFromFile(resultFile);
    }
    LOGGER.info("check");
    Assert.assertEquals(checkWordCount, ImmutableMap.of("eagle", 3, "hello", 1, "world", 1));
  }

  private void serializeResultToFile(String fileName, Object obj) {
    try {
      ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(fileName));
      out.writeObject(obj);
    } catch (Exception e) {
      LOGGER.error(String.valueOf(e));
    }
  }

  private Object deserializeResultFromFile(String fileName) {
    Map<String, Integer> checkWordCount = null;
    try {
      ObjectInputStream in = new ObjectInputStream(new FileInputStream(fileName));
      checkWordCount = (Map<String, Integer>) in.readObject();
      Assert.assertEquals(checkWordCount, ImmutableMap.of("eagle", 3, "hello", 1, "world", 1));
    } catch (Exception e) {
      LOGGER.error(String.valueOf(e));
    }
    return checkWordCount;
  }

  private static class WordAndCount implements Serializable {

    public final String word;
    public final Integer count;

    public WordAndCount(String key, Integer count) {
      this.word = key;
      this.count = count;
    }
  }

  private void deleteResultFile(String path) {
    File file = new File(path);
    file.deleteOnExit();
  }
}
