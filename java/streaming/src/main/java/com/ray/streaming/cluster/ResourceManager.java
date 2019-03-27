package com.ray.streaming.cluster;

import com.ray.streaming.core.runtime.StreamWorker;
import java.util.ArrayList;
import java.util.List;
import org.ray.api.Ray;
import org.ray.api.RayActor;

public class ResourceManager {

  public List<RayActor<StreamWorker>> createWorker(int workerNum) {
    List<RayActor<StreamWorker>> workers = new ArrayList<>();
    for (int i = 0; i < workerNum; i++) {
      RayActor<StreamWorker> worker = Ray.createActor(StreamWorker::new);
      workers.add(worker);
    }
    return workers;
  }

}
