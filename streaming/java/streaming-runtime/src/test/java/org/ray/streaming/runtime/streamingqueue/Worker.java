package org.ray.streaming.runtime.streamingqueue;

import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.annotation.RayRemote;
import org.ray.api.id.ActorId;
import org.ray.runtime.RayMultiWorkerNativeRuntime;
import org.ray.runtime.actor.NativeRayActor;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.streaming.runtime.transfer.ChannelID;
import org.ray.streaming.runtime.transfer.DataMessage;
import org.ray.streaming.runtime.transfer.DataReader;
import org.ray.streaming.runtime.transfer.DataWriter;
import org.ray.streaming.runtime.transfer.TransferHandler;
import org.ray.streaming.util.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;

public class Worker {
  private static final Logger LOGGER = LoggerFactory.getLogger(Worker.class);

  protected TransferHandler transferHandler = null;

  public Worker() {
    transferHandler = new TransferHandler(((RayMultiWorkerNativeRuntime) Ray.internal())
        .getCurrentRuntime().getNativeCoreWorkerPointer(),
        new JavaFunctionDescriptor(Worker.class.getName(),
            "onWriterMessage", "([B)V"),
        new JavaFunctionDescriptor(Worker.class.getName(),
            "onWriterMessageSync", "([B)[B"),
        new JavaFunctionDescriptor(Worker.class.getName(),
            "onReaderMessage", "([B)V"),
        new JavaFunctionDescriptor(Worker.class.getName(),
            "onReaderMessageSync", "([B)[B"));
  }

  public void onReaderMessage(byte[] buffer) {
    transferHandler.onReaderMessage(buffer);
  }

  public byte[] onReaderMessageSync(byte[] buffer) {
    return transferHandler.onReaderMessageSync(buffer);
  }

  public void onWriterMessage(byte[] buffer) {
    transferHandler.onWriterMessage(buffer);
  }

  public byte[] onWriterMessageSync(byte[] buffer) {
    return transferHandler.onWriterMessageSync(buffer);
  }
}

@RayRemote
class ReaderWorker extends Worker {
  private static final Logger LOGGER = LoggerFactory.getLogger(ReaderWorker.class);

  private String name = null;
  private List<String> inputQueueList = null;
  private List<ActorId> inputActorIds = new ArrayList<>();
  private DataReader dataReader = null;
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

  public String testRayCall() {
    LOGGER.info("testRayCall called");
    return "testRayCall";
  }

  public boolean init(List<String> inputQueueList, RayActor peer, int msgCount) {

    this.inputQueueList = inputQueueList;
    this.peerActor = peer;
    this.msgCount = msgCount;

    LOGGER.info("ReaderWorker init");
    LOGGER.info("java.library.path = {}", System.getProperty("java.library.path"));

    for (String queue : this.inputQueueList) {
      inputActorIds.add(this.peerActor.getId());
      LOGGER.info("ReaderWorker actorId: {}", this.peerActor.getId());
    }

    Map<String, String> conf = new HashMap<>();

    conf.put(Config.CHANNEL_TYPE, Config.NATIVE_CHANNEL);
    conf.put(Config.CHANNEL_SIZE, "100000");
    conf.put(Config.STREAMING_JOB_NAME, "integrationTest1");
    dataReader = new DataReader(inputQueueList, inputActorIds, conf);

    // Should not GetBundle in RayCall thread
    Thread readThread = new Thread(Ray.wrapRunnable(new Runnable() {
      @Override
      public void run() {
        consume();
      }
    }));
    readThread.start();

    LOGGER.info("ReaderWorker init done");

    return true;
  }

  public final void consume() {

    int checkPointId = 1;
    for (int i = 0; i < msgCount * inputQueueList.size(); ++i) {
      DataMessage dataMessage = dataReader.read(100);

      if (dataMessage == null) {
        LOGGER.error("dataMessage is null");
        i--;
        continue;
      }

      int bufferSize = dataMessage.body().remaining();
      int dataSize = dataMessage.body().getInt();

      // check size
      LOGGER.info("capacity {} bufferSize {} dataSize {}",
          dataMessage.body().capacity(), bufferSize, dataSize);
      Assert.assertEquals(bufferSize, dataSize);
      if (dataMessage instanceof DataMessage) {
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info("{} : {}  message.", i, dataMessage.toString());
        }
        // check content
        for (int j = 0; j < dataSize - 4; ++j) {
          Assert.assertEquals(dataMessage.body().get(), (byte) j);
        }
      } else {
        LOGGER.error("unknown message type");
        Assert.fail();
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

@RayRemote
class WriterWorker extends Worker {
  private static final Logger LOGGER = LoggerFactory.getLogger(WriterWorker.class);

  private String name = null;
  private List<String> outputQueueList = null;
  private List<ActorId> outputActorIds = new ArrayList<>();
  DataWriter dataWriter = null;
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
  }

  public boolean init(List<String> outputQueueList, RayActor peer, int msgCount) {

    this.outputQueueList = outputQueueList;
    this.peerActor = peer;
    this.msgCount = msgCount;

    LOGGER.info("WriterWorker init:");

    for (String queue : this.outputQueueList) {
      outputActorIds.add(this.peerActor.getId());
      LOGGER.info("WriterWorker actorId: {}", this.peerActor.getId());
    }

    LOGGER.info("Peer isDirectActorCall: {}", ((NativeRayActor) peer).isDirectCallActor());
    int count = 3;
    while (count-- != 0) {
      Ray.call(ReaderWorker::testRayCall, peer).get();
    }

    try {
      Thread.sleep(2 * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Map<String, String> conf = new HashMap<>();

    conf.put(Config.CHANNEL_TYPE, Config.NATIVE_CHANNEL);
    conf.put(Config.CHANNEL_SIZE, "100000");
    conf.put(Config.STREAMING_JOB_NAME, "integrationTest1");

    dataWriter = new DataWriter(this.outputQueueList, this.outputActorIds, conf);
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
    this.msgCount = 100;
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

        bb.clear();
        ChannelID qid = ChannelID.from(outputQueueList.get(j));
        dataWriter.write(qid, bb);
      }
    }
    try {
      Thread.sleep(20 * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }
}