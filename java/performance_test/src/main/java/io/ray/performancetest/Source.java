package io.ray.performancetest;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Source {
  private static final int BATCH_SIZE;

  private static final Logger LOGGER = LoggerFactory.getLogger(Source.class);

  private final List<ActorHandle<Receiver>> receivers;

  static {
    String batchSizeString = System.getenv().get("PERF_TEST_BATCH_SIZE");
    if (batchSizeString != null) {
      BATCH_SIZE = Integer.valueOf(batchSizeString);
    } else {
      BATCH_SIZE = 1000;
    }
  }

  public Source(List<ActorHandle<Receiver>> receivers) {
    this.receivers = receivers;
  }

  public boolean startTest(
      boolean hasReturn, boolean ignoreReturn, int argSize, boolean useDirectByteBuffer) {
    LOGGER.info("Source startTest");
    byte[] bytes = null;
    ByteBuffer buffer = null;
    if (argSize > 0) {
      bytes = new byte[argSize];
      new Random().nextBytes(bytes);
      buffer = ByteBuffer.wrap(bytes);
    } else {
      Preconditions.checkState(!useDirectByteBuffer);
    }

    // Wait for actors to be created.
    for (ActorHandle<Receiver> receiver : receivers) {
      receiver.task(Receiver::ping).remote().get();
    }

    LOGGER.info(
        "Started executing tasks, useDirectByteBuffer: {}, argSize: {}, has return: {}",
        useDirectByteBuffer,
        argSize,
        hasReturn);

    List<List<ObjectRef<Integer>>> returnObjects = new ArrayList<>();
    returnObjects.add(new ArrayList<>());
    returnObjects.add(new ArrayList<>());

    long startTime = System.currentTimeMillis();
    int numTasks = 0;
    long lastReport = 0;
    long totalTime = 0;
    long batchCount = 0;
    while (true) {
      numTasks++;
      boolean batchEnd = numTasks % BATCH_SIZE == 0;
      for (ActorHandle<Receiver> receiver : receivers) {
        if (hasReturn || batchEnd) {
          ObjectRef<Integer> returnObject;
          if (useDirectByteBuffer) {
            returnObject = receiver.task(Receiver::byteBufferHasReturn, buffer).remote();
          } else if (argSize > 0) {
            returnObject = receiver.task(Receiver::bytesHasReturn, bytes).remote();
          } else {
            returnObject = receiver.task(Receiver::noArgsHasReturn).remote();
          }
          returnObjects.get(1).add(returnObject);
        } else {
          if (useDirectByteBuffer) {
            receiver.task(Receiver::byteBufferNoReturn, buffer).remote();
          } else if (argSize > 0) {
            receiver.task(Receiver::bytesNoReturn, bytes).remote();
          } else {
            receiver.task(Receiver::noArgsNoReturn).remote();
          }
        }
      }

      if (batchEnd) {
        batchCount++;
        long getBeginTs = System.currentTimeMillis();
        Ray.get(returnObjects.get(0));
        long rt = System.currentTimeMillis() - getBeginTs;
        totalTime += rt;
        returnObjects.set(0, returnObjects.get(1));
        returnObjects.set(1, new ArrayList<>());

        long elapsedTime = System.currentTimeMillis() - startTime;
        if (elapsedTime / 60000 > lastReport) {
          lastReport = elapsedTime / 60000;
          LOGGER.info(
              "Finished executing {} tasks in {} ms, useDirectByteBuffer: {}, argSize: {}, "
                  + "has return: {}, avg get rt: {}",
              numTasks,
              elapsedTime,
              useDirectByteBuffer,
              argSize,
              hasReturn,
              totalTime / (float) batchCount);
        }
      }
    }
  }
}
