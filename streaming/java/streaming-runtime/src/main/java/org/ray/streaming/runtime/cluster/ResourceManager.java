package org.ray.streaming.runtime.cluster;

import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Resource-Manager is used to do the management of resources
 */
public class ResourceManager {

  public List<RayActor<JobWorker>> createWorkers(int workerNum) {
    List<RayActor<JobWorker>> workers = new ArrayList<>();
    for (int i = 0; i < workerNum; i++) {
      RayActor<JobWorker> worker = Ray.createActor(JobWorker::new);
      workers.add(worker);
    }
    return workers;
  }

}
