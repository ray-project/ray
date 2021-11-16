package io.ray.streaming.runtime.master.coordinator;

import io.ray.api.Ray;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.master.context.JobMasterRuntimeContext;
import io.ray.streaming.runtime.master.graphmanager.GraphManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseCoordinator implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(BaseCoordinator.class);

  protected final JobMaster jobMaster;

  protected final JobMasterRuntimeContext runtimeContext;
  protected final GraphManager graphManager;
  protected volatile boolean closed;
  private Thread thread;

  public BaseCoordinator(JobMaster jobMaster) {
    this.jobMaster = jobMaster;
    this.runtimeContext = jobMaster.getRuntimeContext();
    this.graphManager = jobMaster.getGraphManager();
  }

  public void start() {
    thread =
        new Thread(
            Ray.wrapRunnable(this), this.getClass().getName() + "-" + System.currentTimeMillis());
    thread.start();
  }

  public void stop() {
    closed = true;

    try {
      if (thread != null) {
        thread.join(30000);
      }
    } catch (InterruptedException e) {
      LOG.error("Coordinator thread exit has exception.", e);
    }
  }
}
