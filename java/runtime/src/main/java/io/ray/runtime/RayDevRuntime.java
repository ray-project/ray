package io.ray.runtime;

import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.api.id.JobId;
import io.ray.api.id.ObjectId;
import io.ray.api.id.PlacementGroupId;
import io.ray.api.placementgroup.PlacementGroup;
import io.ray.api.runtimecontext.ResourceValue;
import io.ray.runtime.config.RayConfig;
import io.ray.runtime.context.LocalModeWorkerContext;
import io.ray.runtime.functionmanager.FunctionManager;
import io.ray.runtime.gcs.GcsClient;
import io.ray.runtime.generated.Common.TaskSpec;
import io.ray.runtime.object.LocalModeObjectStore;
import io.ray.runtime.task.LocalModeTaskExecutor;
import io.ray.runtime.task.LocalModeTaskSubmitter;
import io.ray.runtime.util.BinaryFileUtil;
import io.ray.runtime.util.JniUtils;
import io.ray.runtime.util.SystemUtil;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class RayDevRuntime extends AbstractRayRuntime {

  private AtomicInteger jobCounter = new AtomicInteger(0);

  public RayDevRuntime(RayConfig rayConfig) {
    super(rayConfig);
  }

  @Override
  public void start() {
    if (rayConfig.getJobId().isNil()) {
      rayConfig.setJobId(nextJobId());
    }

    updateSessionDir(rayConfig);
    JniUtils.loadLibrary(rayConfig.sessionDir, BinaryFileUtil.CORE_WORKER_JAVA_LIBRARY, true);

    taskExecutor = new LocalModeTaskExecutor(this);
    workerContext = new LocalModeWorkerContext(rayConfig.getJobId());
    objectStore = new LocalModeObjectStore(workerContext);
    functionManager = new FunctionManager(rayConfig.codeSearchPath);
    taskSubmitter =
        new LocalModeTaskSubmitter(this, taskExecutor, (LocalModeObjectStore) objectStore);
    ((LocalModeObjectStore) objectStore)
        .addObjectPutCallback(
            objectId -> {
              if (taskSubmitter != null) {
                ((LocalModeTaskSubmitter) taskSubmitter).onObjectPut(objectId);
              }
            });
  }

  @Override
  public void run() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void shutdown() {
    if (taskSubmitter != null) {
      ((LocalModeTaskSubmitter) taskSubmitter).shutdown();
      taskSubmitter = null;
    }
    taskExecutor = null;
  }

  @Override
  public void killActor(BaseActorHandle actor, boolean noRestart) {
    throw new UnsupportedOperationException();
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T extends BaseActorHandle> Optional<T> getActor(String name, String namespace) {
    return (Optional<T>) ((LocalModeTaskSubmitter) taskSubmitter).getActor(name);
  }

  @Override
  public GcsClient getGcsClient() {
    throw new UnsupportedOperationException("Ray doesn't have gcs client in local mode.");
  }

  @Override
  public Map<String, List<ResourceValue>> getAvailableResourceIds() {
    throw new UnsupportedOperationException("Ray doesn't support get resources ids in local mode.");
  }

  @Override
  List<ObjectId> getCurrentReturnIds(int numReturns, ActorId actorId) {
    return null;
  }

  @Override
  public PlacementGroup getPlacementGroup(PlacementGroupId id) {
    // @TODO(clay4444): We need a LocalGcsClient before implements this.
    throw new UnsupportedOperationException(
        "Ray doesn't support placement group operations in local mode.");
  }

  @Override
  public List<PlacementGroup> getAllPlacementGroups() {
    // @TODO(clay4444): We need a LocalGcsClient before implements this.
    throw new UnsupportedOperationException(
        "Ray doesn't support placement group operations in local mode.");
  }

  @Override
  public String getNamespace() {
    return null;
  }

  @Override
  public void exitActor() {}

  private JobId nextJobId() {
    return JobId.fromInt(jobCounter.getAndIncrement());
  }

  private static class AsyncContext {
    private TaskSpec task;

    private AsyncContext(TaskSpec task) {
      this.task = task;
    }
  }

  private static void updateSessionDir(RayConfig rayConfig) {
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd_hh-mm-ss-ms");
    Date date = new Date();
    String sessionDir =
        String.format("/tmp/ray/session_local_mode_%s_%d", format.format(date), SystemUtil.pid());
    rayConfig.setSessionDir(sessionDir);
  }
}
