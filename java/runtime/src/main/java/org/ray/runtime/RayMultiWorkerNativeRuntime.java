package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.Callable;
import org.ray.api.BaseActor;
import org.ray.api.RayActor;
import org.ray.api.RayObject;
import org.ray.api.RayPyActor;
import org.ray.api.WaitResult;
import org.ray.api.function.PyActorClass;
import org.ray.api.function.PyActorMethod;
import org.ray.api.function.PyRemoteFunction;
import org.ray.api.function.RayFunc;
import org.ray.api.id.ObjectId;
import org.ray.api.id.UniqueId;
import org.ray.api.options.ActorCreationOptions;
import org.ray.api.options.CallOptions;
import org.ray.api.runtime.RayRuntime;
import org.ray.api.runtimecontext.RuntimeContext;
import org.ray.runtime.config.RayConfig;
import org.ray.runtime.config.RunMode;
import org.ray.runtime.functionmanager.FunctionManager;
import org.ray.runtime.generated.Common.WorkerType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a proxy runtime for multi-worker support. It holds multiple {@link RayNativeRuntime}
 * instances and redirect calls to the correct one based on thread context.
 */
public class RayMultiWorkerNativeRuntime implements RayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayMultiWorkerNativeRuntime.class);

  private final FunctionManager functionManager;

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

  public RayMultiWorkerNativeRuntime(RayConfig rayConfig, FunctionManager functionManager) {
    this.functionManager = functionManager;
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
        RayNativeRuntime runtime = new RayNativeRuntime(rayConfig, functionManager);
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
  public void killActor(BaseActor actor, boolean noReconstruction) {
    getCurrentRuntime().killActor(actor, noReconstruction);
  }

  @Override
  public RayObject call(RayFunc func, Object[] args, CallOptions options) {
    return getCurrentRuntime().call(func, args, options);
  }

  @Override
  public RayObject call(PyRemoteFunction pyRemoteFunction, Object[] args,
                        CallOptions options) {
    return getCurrentRuntime().call(pyRemoteFunction, args, options);
  }

  @Override
  public RayObject callActor(RayActor<?> actor, RayFunc func, Object[] args) {
    return getCurrentRuntime().callActor(actor, func, args);
  }

  @Override
  public RayObject callActor(RayPyActor pyActor, PyActorMethod pyActorMethod, Object[] args) {
    return getCurrentRuntime().callActor(pyActor, pyActorMethod, args);
  }

  @Override
  public <T> RayActor<T> createActor(RayFunc actorFactoryFunc, Object[] args,
                                     ActorCreationOptions options) {
    return getCurrentRuntime().createActor(actorFactoryFunc, args, options);
  }

  @Override
  public RayPyActor createActor(PyActorClass pyActorClass, Object[] args,
                                ActorCreationOptions options) {
    return getCurrentRuntime().createActor(pyActorClass, args, options);
  }

  @Override
  public RuntimeContext getRuntimeContext() {
    return getCurrentRuntime().getRuntimeContext();
  }

  @Override
  public Object getAsyncContext() {
    return getCurrentRuntime();
  }

  @Override
  public void setAsyncContext(Object asyncContext) {
    currentThreadRuntime.set((RayNativeRuntime) asyncContext);
  }

  @Override
  public Runnable wrapRunnable(Runnable runnable) {
    Object asyncContext = getAsyncContext();
    return () -> {
      setAsyncContext(asyncContext);
      runnable.run();
    };
  }

  @Override
  public Callable wrapCallable(Callable callable) {
    Object asyncContext = getAsyncContext();
    return () -> {
      setAsyncContext(asyncContext);
      return callable.call();
    };
  }
}
