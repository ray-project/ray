package org.ray.streaming.runtime.demo;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.ActorCreationOptions.Builder;
import org.ray.streaming.runtime.queue.QueueID;
import org.ray.streaming.runtime.queue.QueueUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;


public class StreamingQueueTest implements Serializable {

  private static Logger LOGGER = LoggerFactory.getLogger(StreamingQueueTest.class);
  private String runModeBackup = null;

  @org.testng.annotations.BeforeSuite
  public void suiteSetUp() throws Exception {
    LOGGER.info("Do set up");
    String management = ManagementFactory.getRuntimeMXBean().getName();
    String pid = management.split("@")[0];

    LOGGER.info("integrationTest pid: {}", pid);
    LOGGER.info("java.library.path = {}", System.getProperty("java.library.path"));
  }

  @org.testng.annotations.AfterSuite
  public void suiteTearDown() throws Exception {
    LOGGER.warn("Do tear down");
  }

  @BeforeClass
  public void setUp() {
  }

  @BeforeMethod
  void beforeMethod() {

    LOGGER.info("beforeTest");
    Ray.shutdown();
    System.setProperty("ray.resources", "CPU:4,RES-A:4");
    System.setProperty("ray.raylet.config.num_workers_per_process_java", "1");

    runModeBackup = System.getProperty("ray.run-mode");
    LOGGER.info("runModeBackup: {}", runModeBackup);
    System.setProperty("ray.run-mode", "CLUSTER");
    System.setProperty("ray.redirect-output", "true");
    // ray init
    Ray.init();
  }

  @AfterMethod
  void afterMethod() {
    LOGGER.info("afterTest");
    Ray.shutdown();
    System.setProperty("ray.run-mode", "SINGLE_PROCESS");
  }

  @Test(timeOut = 3000000)
  public void integrationTest() {
    LOGGER.info("StreamingQueueTest.integrationTest");

    ActorCreationOptions.Builder builder = new Builder(); // .setUseDirectCall(true).setMaxReconstructions(1000);

    RayActor<WriterWorker> writerActor = Ray.createActor(WriterWorker::new, "writer",
        builder.createActorCreationOptions());
    RayActor<ReaderWorker> readerActor = Ray.createActor(ReaderWorker::new, "reader",
        builder.createActorCreationOptions());

    LOGGER.info("call getName on writerActor: {}", Ray.call(WriterWorker::getName, writerActor).get());
    LOGGER.info("call getName on readerActor: {}", Ray.call(ReaderWorker::getName, readerActor).get());

//    LOGGER.info(Ray.call(WriterWorker::testCallReader, writerActor, readerActor).get());
    List<String> outputQueueList = new ArrayList<>();
    List<String> inputQueueList = new ArrayList<>();
    int queueNum = 2;
    for (int i = 0; i < queueNum; ++i) {
      String qid = QueueUtils.getRandomQueueId();
      LOGGER.info("getRandomQueueId: {}", qid);
      inputQueueList.add(qid);
      outputQueueList.add(qid);
      readerActor.getId();
    }

    final int msgCount = 100;
    Ray.call(ReaderWorker::init, readerActor, inputQueueList, writerActor, msgCount);
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Ray.call(WriterWorker::init, writerActor, outputQueueList, readerActor, msgCount);


    try {
      Thread.sleep(20*1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    Assert.assertEquals(
        Ray.call(ReaderWorker::getTotalMsg, readerActor).get().intValue(),
        msgCount*queueNum);
  }

  private void deleteResultFile(String path) {
    File file = new File(path);
    file.deleteOnExit();
  }

  private void writeResultToFile(String value, String path) {
    try {
      BufferedWriter out = new BufferedWriter(new FileWriter(path));
      out.write(value);
      out.flush();
      out.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private int readResultFromFile(String path) {
    String value = null;
    try {
      BufferedReader in = new BufferedReader(new FileReader(path));
      value = in.readLine();
      in.close();
      if (value != null) {
        return Integer.valueOf(value);
      }
    } catch (IOException e) {
      return 0;
    }

    return 0;
  }

//  @Test(timeOut = 60000)
//  public void testWordCountExample() throws Exception {
////    TestHelper.setUTPattern();
//    EnvContext envContext = Env.buildEnv();
//    envContext.withResource(2);
//
//    Map<String, String> config = new HashMap<>();
//    config.put(StreamingConstants.QUEUE_TYPE, StreamingConstants.STREAMING_QUEUE);
////    config.put(RayaGConfig.RAYAG_USE_CUSTOM_ALLOCATE, "true");
////    config.put(RayaGConfig.RAYAG_RUNNING_MODE, RunningMode.single_process.name());
////    config.put(StreamingConstants.CP_PANGU_ROOT_DIR, "file:///tmp/streaminglogs");
//    envContext.withConfig(config);
//
//    String resultFile = "/tmp/streaming_queue_test_testWordCountExample.txt";
//    deleteResultFile(resultFile);
//    final Map<String, Integer> wordCountMap = new ConcurrentHashMap<>();
//    envContext.submitJob((ISyncJob) (jobContext, input) -> {
//      List<String> text = new ArrayList<>();
//      text.add("hello world hello earth");
//      PStreamSource<String> source = PStreamSource.fromCollection(jobContext, text);
//      source.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (value, collector) -> {
//        String[] records = value.split(" ");
//        for (String record : records) {
//          collector.partition(new Tuple2<>(record, 1));
//        }
//      }).keyBy(a -> a.getF0()).reduce((a, b) -> Tuple2.of(a.getF0(), a.getF1() + b.getF1()))
//          .sink(s -> {
//            wordCountMap.put(s.f0, s.f1);
//            LOGGER.info("=======:{}", s);
//            writeResultToFile(String.valueOf(wordCountMap.size()), resultFile);
//          });
//
//      jobContext.start("testWordCountExample");
//      return null;
//    });
//
//    LOGGER.info("wordcount started");
//
//    while (readResultFromFile(resultFile) < 3) {
//      TimeUnit.MILLISECONDS.sleep(100);
//    }
//    TimeUnit.SECONDS.sleep(3);
//    TestHelper.clearUTPattern();
//  }

  public static void main(String[] args) {

    StreamingQueueTest test = new StreamingQueueTest();
    try {
      test.suiteSetUp();
    } catch (Exception e) {
      e.printStackTrace();
    }
//    test.integrationTest();
  }
}
