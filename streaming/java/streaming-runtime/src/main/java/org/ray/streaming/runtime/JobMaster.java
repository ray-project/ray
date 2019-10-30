package org.ray.streaming.runtime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.annotation.RayRemote;
import org.ray.streaming.core.graph.ExecutionGraph;
import org.ray.streaming.util.ConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RayRemote
public class JobMaster implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(JobMaster.class);

  private ExecutionGraph executionGraph;
  private Map<String, Object> config;
  List<RayActor<JobWorker>> sourceWorkers;
  List<RayActor<JobWorker>> sinkWorkers;
  private Thread batchControllerThread;
  private long maxBatch;
  private long frequency;

  public JobMaster() {
  }

  public Boolean init(ExecutionGraph executionGraph, Map<String, Object> config) {
    this.executionGraph = executionGraph;
    this.config = config;
    this.sourceWorkers = executionGraph.getSourceWorkers();
    this.sinkWorkers = executionGraph.getSinkWorkers();

    if (config.containsKey(ConfigKey.STREAMING_BATCH_MAX_COUNT)) {
      maxBatch = Long.valueOf(String.valueOf(config.get(ConfigKey.STREAMING_BATCH_MAX_COUNT)));
    } else {
      maxBatch = Long.MAX_VALUE;
    }
    if (config.containsKey(ConfigKey.STREAMING_BATCH_FREQUENCY)) {
      frequency = Long.valueOf(String.valueOf(config.get(ConfigKey.STREAMING_BATCH_FREQUENCY)));
    } else {
      frequency = ConfigKey.STREAMING_BATCH_FREQUENCY_DEFAULT;
    }
    startBatchController();
    return true;
  }

  private void startBatchController() {
    BatchController batchController = new BatchController();
    batchControllerThread = new Thread(batchController, "controller-thread");
    batchControllerThread.start();
    LOG.info("started BatchController, batchId frequency {}, maxBatchId {}",
        frequency, maxBatch);
  }

  class BatchController implements Runnable, Serializable {
    private AtomicLong batchId;

    BatchController() {
      this.batchId = new AtomicLong(0);
    }

    @Override
    public void run() {
      while (batchId.get() < maxBatch) {
        try {
          long newBatchId = batchId.getAndIncrement();
          List<RayObject<Boolean>> waits = new ArrayList<>();
          for (RayActor<JobWorker> sourceWorker : sourceWorkers) {
            waits.add(Ray.call(JobWorker::setBatchId, sourceWorker, newBatchId));
          }
          Ray.wait(waits);
          LOG.info("set batchId {} succeed", newBatchId);
          Thread.sleep(frequency);
        } catch (Exception e) {
          LOG.error(e.getMessage(), e);
        }
      }
    }
  }

}
