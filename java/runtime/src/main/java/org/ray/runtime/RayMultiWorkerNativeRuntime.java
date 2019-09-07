package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.Callable;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.ray.api.function.RayFunc;
import org.ray.api.id.ObjectId;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtimecontext.RuntimeContext;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.generated.Common.WorkerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a proxy runtime for multi-worker support. It holds multiple {@link RayNativeRuntime}
 * instances and redirect calls to the correct one based on thread context.
 */
public class RayMultiWorkerNativeRuntime implements RayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayMultiWorkerNativeRuntime.class);

  /**
   * The number of workers per worker process.
   */
  private final int numWorkers;
  /**
   * The worker threads.
   */
  private final Thread[] threads;
  /**
   * The {@link RayNativeRuntime} instances of workers.
   */
  private final RayNativeRuntime[] runtimes;
  /**
   * The {@link RayNativeRuntime} instance of current thread.
   */
  private final ThreadLocal<RayNativeRuntime> currentThreadRuntime = new ThreadLocal<>();

  public RayMultiWorkerNativeRuntime(RayConfig rayConfig) {
    Preconditions.checkState(
        rayConfig.runMode == RunMode.CLUSTER && rayConfig.workerMode == WorkerType.WORKER);
    Preconditions.checkState(rayConfig.numWorkersPerProcess > 0,
        "numWorkersPerProcess must be greater than 0.");
    numWorkers = rayConfig.numWorkersPerProcess;
    runtimes = new RayNativeRuntime[numWorkers];
    threads = new Thread[numWorkers];

    LOGGER.info("Starting {} workers.", numWorkers);

    for (int i = 0; i < numWorkers; i++) {
      final int workerIndex = i;
      threads[i] = new Thread(() -> {
        RayNativeRuntime runtime = new RayNativeRuntime(rayConfig);
        runtimes[workerIndex] = runtime;
        currentThreadRuntime.set(runtime);
        runtime.run();
      });
    }
  }

  public void run() {
    for (int i = 0; i < numWorkers; i++) {
      threads[i].start();
    }
    for (int i = 0; i < numWorkers; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void shutdown() {
    for (int i = 0; i < numWorkers; i++) {
      runtimes[i].shutdown();
    }
    for (int i = 0; i < numWorkers; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  public RayNativeRuntime getCurrentRuntime() {
    RayNativeRuntime currentRuntime = currentThreadRuntime.get();
    Preconditions.checkNotNull(currentRuntime,
        "RayRuntime is not set on current thread."
            + " If you want to use Ray API in your own threads,"
            + " please wrap your `Runnable`s or `Callable`s with"
            + " `Ray.wrapRunnable` or `Ray.wrapCallable`.");
    return currentRuntime;
  }

  @Override
  public <T> RayObject<T> put(T obj) {
    return getCurrentRuntime().put(obj);
  }

  @Override
  public <T> T get(ObjectId objectId) {
    return getCurrentRuntime().get(objectId);
  }

  @Override
  public <T> List<T> get(List<ObjectId> objectIds) {
    return getCurrentRuntime().get(objectIds);
  }

  @Override
  public <T> WaitResult<T> wait(List<RayObject<T>> waitList, int numReturns, int timeoutMs) {
    return getCurrentRuntime().wait(waitList, numReturns, timeoutMs);
  }

  @Override
  public void free(List<ObjectId> objectIds, boolean localOnly, boolean deleteCreatingTasks) {
    getCurrentRuntime().free(objectIds, localOnly, deleteCreatingTasks);
  }

  @Override
  public void setResource(String resourceName, double capacity, UniqueId nodeId) {
    getCurrentRuntime().setResource(resourceName, capacity, nodeId);
  }

  @Override
  public RayObject call(RayFunc func, Object[] args, CallOptions options) {
    return getCurrentRuntime().call(func, args, options);
  }

  @Override
  public RayObject call(RayFunc func, RayActor<?> actor, Object[] args) {
    return getCurrentRuntime().call(func, actor, args);
  }

  @Override
  public <T> RayActor<T> createActor(RayFunc actorFactoryFunc, Object[] args,
      ActorCreationOptions options) {
    return getCurrentRuntime().createActor(actorFactoryFunc, args, options);
  }

  @Override
  public RuntimeContext getRuntimeContext() {
    return getCurrentRuntime().getRuntimeContext();
  }

  @Override
  public RayObject callPy(String moduleName, String functionName, Object[] args,
      CallOptions options) {
    return getCurrentRuntime().callPy(moduleName, functionName, args, options);
  }

  @Override
  public RayObject callPy(RayPyActor pyActor, String functionName, Object[] args) {
    return getCurrentRuntime().callPy(pyActor, functionName, args);
  }

  @Override
  public RayPyActor createPyActor(String moduleName, String className, Object[] args,
      ActorCreationOptions options) {
    return getCurrentRuntime().createPyActor(moduleName, className, args, options);
  }

  @Override
  public Runnable wrapRunnable(Runnable runnable) {
    RayNativeRuntime runtime = getCurrentRuntime();
    return () -> {
      currentThreadRuntime.set(runtime);
      runnable.run();
    };
  }

  @Override
  public Callable wrapCallable(Callable callable) {
    RayNativeRuntime runtime = getCurrentRuntime();
    return () -> {
      currentThreadRuntime.set(runtime);
      return callable.call();
    };
  }
}
