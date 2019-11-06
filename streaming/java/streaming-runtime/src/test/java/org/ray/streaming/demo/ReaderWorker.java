package org.ray.streaming.demo;

import sun.security.krb5.Config;

import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ActorId;
import org.ray.runtime.actor.NativeRayActor;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;

import org.ray.streaming.queue.QueueConsumer;
import org.ray.streaming.queue.QueueItem;
import org.ray.streaming.queue.QueueMessage;
import org.ray.streaming.queue.impl.QueueConfigKeys;

import org.ray.streaming.queue.impl.StreamingQueueLinkImpl;
import org.ray.streaming.util.ConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

@RayRemote
public class ReaderWorker extends Worker {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReaderWorker.class);

  private String name = null;
  private List<String> inputQueueList = null;
  private Map<String, ActorId> inputActorIds = new HashMap<>();
  private QueueConsumer consumer = null;
  private long handler = 0;
  private RayActor peerActor = null;
  private int msgCount = 0;
  private int totalMsg = 0;

  public ReaderWorker(String name) {
    LOGGER.info("ReaderWorker constructor");
    this.name = name;
  }

  public String getName() {
    String management = ManagementFactory.getRuntimeMXBean().getName();
    String pid = management.split("@")[0];

    LOGGER.info("pid: {} name: {}", pid, name);
    return name;
  }

  public boolean init(List<String> inputQueueList, RayActor peer, int msgCount) {

    this.inputQueueList = inputQueueList;
    this.peerActor = peer;
    this.msgCount = msgCount;

    LOGGER.info("ReaderWorker init");
    LOGGER.info("java.library.path = {}", System.getProperty("java.library.path"));

    for (String queue : this.inputQueueList) {
      inputActorIds.put(queue, this.peerActor.getId());
      LOGGER.info("ReaderWorker actorId: {}", inputActorIds.get(queue));
    }

    Map<String, String> conf = new HashMap<>(16);

    conf.put(ConfigKey.STREAMING_QUEUE_TYPE, ConfigKey.STREAMING_QUEUE);
    conf.put(QueueConfigKeys.QUEUE_SIZE, "100000");
    conf.put(QueueConfigKeys.STREAMING_WRITER_CONSUMED_STEP, "100");
    conf.put(QueueConfigKeys.STREAMING_READER_CONSUMED_STEP, "20");
    conf.put(QueueConfigKeys.STREAMING_JOB_NAME, "integrationTest1");
    queueLink = new StreamingQueueLinkImpl();

    queueLink.setConfiguration(conf);
    queueLink.setRayRuntime(Ray.internal());
    ((StreamingQueueLinkImpl) queueLink).setStreamingTransferFunction(
        new JavaFunctionDescriptor("com.alipay.streaming.runtime.queue.Worker", "onStreamingTransfer", "([B)V"));
    ((StreamingQueueLinkImpl) queueLink).setStreamingTransferSyncFunction(
        new JavaFunctionDescriptor("com.alipay.streaming.runtime.queue.Worker", "onStreamingTransferSync", "([B)[B"));
    consumer = queueLink.registerQueueConsumer(this.inputQueueList, this.inputActorIds);

    // Should not GetBundle in RayCall thread
    Thread readThread = new Thread(Ray.wrapRunnable(new Runnable() {
      @Override
      public void run() {
        consum();
      }
    }));
    readThread.start();

    LOGGER.info("ReaderWorker init done");

    return true;
  }

  public final void consum() {

    int checkPointId = 1;
    for (int i = 0; i < msgCount * inputQueueList.size(); ++i) {
      QueueItem item = consumer.pull(100);

      if (item == null) {
        LOGGER.error("item is null");
        i--;
        continue;
      }

      int bufferSize = item.body().remaining();
      int dataSize = item.body().getInt();

      // check size
      LOGGER.info("capacity {} bufferSize {} dataSize {}", item.body().capacity(), bufferSize, dataSize);
      Assert.assertEquals(bufferSize, dataSize);
      if (item instanceof QueueMessage) {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("{} : {}  message.", i, item.toString());
        }
        // check content
        for (int j = 0; j < dataSize - 4; ++j) {
          Assert.assertEquals(item.body().get(), (byte) j);
        }
      } else {
        LOGGER.error("unknown message type");
        Assert.assertTrue(false);
      }

      totalMsg++;
    }

    LOGGER.info("ReaderWorker consume data done.");
  }

  void onQueueTransfer(long handler, byte[] buffer) {
  }


  public boolean done() {
    return totalMsg == msgCount;
  }

  public int getTotalMsg() {
    return totalMsg;
  }
}
