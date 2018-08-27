package org.ray.core.impl;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.ConcurrentHashMap;
import org.ray.api.Ray;
import org.ray.api.RayActor;
import org.ray.api.RayRemote;
import org.ray.api.UniqueID;
import org.ray.core.RayRuntime;
import org.ray.core.UniqueIdHelper;
import org.ray.core.WorkerContext;
import org.ray.core.model.RayParameters;
import org.ray.spi.NopRemoteFunctionManager;
import org.ray.spi.PathConfig;
import org.ray.spi.RemoteFunctionManager;
import org.ray.spi.impl.MockLocalScheduler;
import org.ray.spi.impl.MockObjectStore;
import org.ray.util.exception.TaskExecutionException;
import org.ray.util.logger.RayLog;

public class RayDevRuntime extends RayRuntime {

  private final ConcurrentHashMap<UniqueID, Object> actors = new ConcurrentHashMap<>();

  protected RayDevRuntime() {
  }

  @RayRemote
  private static byte[] createActor(String className) {
    return ((RayDevRuntime) RayRuntime.getInstance()).createLocalActor(className);
  }

  private byte[] createLocalActor(String className) {
    UniqueID taskId = WorkerContext.currentTask().taskId;
    UniqueID actorId = UniqueIdHelper.taskComputeReturnId(taskId, 0);
    try {
      Class<?> cls = Class.forName(className);

      Constructor<?>[] cts = cls.getConstructors();
      for (Constructor<?> ct : cts) {
        System.err.println(ct.getName() + ", param count = " + ct.getParameterCount());
      }

      Object r = cls.getConstructor().newInstance();
      actors.put(actorId, r);
      RayLog.core.info("TaskId " + taskId + ", create actor ok " + actorId);
      return actorId.getBytes();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException
        | IllegalArgumentException | InvocationTargetException | NoSuchMethodException
        | SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
      String logInfo =
          "TaskId " + taskId + " error at RayDevRuntime createLocalActor, create actor " + actorId
              + " for " + className + "  failed";
      System.err.println(logInfo + ", ex = " + e.getMessage());
      RayLog.core.error(logInfo, e);
      throw new TaskExecutionException(logInfo, e);
    }
  }

  @Override
  public void start(RayParameters params) {
    PathConfig pathConfig = new PathConfig(configReader);
    RemoteFunctionManager rfm = new NopRemoteFunctionManager(params.driver_id);
    MockObjectStore store = new MockObjectStore();
    MockLocalScheduler scheduler = new MockLocalScheduler(store);
    init(scheduler, store, rfm, pathConfig);
    scheduler.setLocalFunctionManager(this.functions);
  }

  @Override
  public void cleanUp() {
    // nothing to do
  }

  @Override
  public Object getLocalActor(UniqueID id) {
    return actors.get(id);
  }

  @Override
  public <T> RayActor<T> create(Class<T> cls) {
    return new RayActor<>(Ray.call(RayDevRuntime::createActor, cls.getName()).getId());
  }
}
