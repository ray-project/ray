package org.ray.streaming.runtime.demo;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ActorId;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.streaming.runtime.queue.QueueID;
import org.ray.streaming.runtime.queue.QueueProducer;
import org.ray.streaming.runtime.queue.impl.QueueConfigKeys;
import org.ray.streaming.runtime.queue.impl.StreamingQueueLinkImpl;
import org.ray.streaming.util.ConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@RayRemote
public class WriterWorker extends Worker {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriterWorker.class);

  private String name = null;
  private List<String> outputQueueList = null;
  private Map<String, ActorId> outputActorIds = new HashMap<>();
  QueueProducer producer = null;
  RayActor peerActor = null;
  int msgCount = 0;
  public WriterWorker(String name) {
    this.name = name;
  }

  public String getName() {
    String management = ManagementFactory.getRuntimeMXBean().getName();
    String pid = management.split("@")[0];

    LOGGER.info("pid: {} name: {}", pid, name);
    return name;
  }

  public String testCallReader(RayActor readerActor) {
    String name = (String) Ray.call(ReaderWorker::getName, readerActor).get();
    LOGGER.info("testCallReader: {}", name);
    return name;
//    return "";
  }

  public boolean init(List<String> outputQueueList, RayActor peer, int msgCount) {

    this.outputQueueList = outputQueueList;
    this.peerActor = peer;
    this.msgCount = msgCount;

    LOGGER.info("WriterWorker init:");

    for (String queue : this.outputQueueList) {
      outputActorIds.put(queue, this.peerActor.getId());
      LOGGER.info("WriterWorker actorId: {}", outputActorIds.get(queue));
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
    producer = queueLink.registerQueueProducer(this.outputQueueList, this.outputActorIds);

    Thread writerThread = new Thread(Ray.wrapRunnable(new Runnable() {
      @Override
      public void run() {
        produce();
      }
    }));
    writerThread.start();

    LOGGER.info("WriterWorker init done");
    return true;
  }

  public final void produce() {

    int checkPointId = 1;
    Random random = new Random();
    for (int i = 0; i < this.msgCount; ++i) {
      for (int j = 0; j < outputQueueList.size(); ++j) {
        LOGGER.info("WriterWorker produce");
        int dataSize = (random.nextInt(100)) + 10;
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("dataSize: {}", dataSize);
        }
        ByteBuffer bb = ByteBuffer.allocate(dataSize);
        bb.putInt(dataSize);
        for (int k = 0; k < dataSize - 4; ++k) {
          bb.put((byte) k);
        }

        QueueID qid = QueueID.from(outputQueueList.get(j));
        producer.produce(qid, bb);
      }
    }
  }
}
