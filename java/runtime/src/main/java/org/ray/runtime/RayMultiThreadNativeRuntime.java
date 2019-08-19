package org.ray.runtime;

import com.google.common.base.Preconditions;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
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

public class RayMultiThreadNativeRuntime implements RayRuntime {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayMultiThreadNativeRuntime.class);

  private final int threadCount;
  private final Thread[] threads;
  private final RayNativeRuntime[] runtimes;
  private final ThreadLocal<RayNativeRuntime> currentThreadRuntime = new ThreadLocal<>();

  private CountDownLatch shutdownCountDownLatch = new CountDownLatch(1);

  public RayMultiThreadNativeRuntime(RayConfig rayConfig) {
    Preconditions.checkState(
        rayConfig.runMode == RunMode.CLUSTER && rayConfig.workerMode == WorkerType.WORKER);
    Preconditions.checkState(rayConfig.numWorkersPerProcess > 0,
        "numWorkersPerProcess must be greater than 0.");
    threadCount = rayConfig.numWorkersPerProcess;
    runtimes = new RayNativeRuntime[threadCount];
    threads = new Thread[threadCount];

    LOGGER.info("Starting {} workers.", threadCount);

    for (int i = 0; i < threadCount; i++) {
      int finalI = i;
      threads[i] = new Thread(() -> {
        RayNativeRuntime runtime = new RayNativeRuntime(rayConfig);
        runtimes[finalI] = runtime;
        currentThreadRuntime.set(runtime);
        runtime.run();
      });
    }
  }

  public void run() {
    for (int i = 0; i < threadCount; i++) {
      threads[i].start();
    }
    for (int i = 0; i < threadCount; i++) {
      try {
        threads[i].join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void shutdown() {
    shutdownCountDownLatch.countDown();
    for (int i = 0; i < threadCount; i++) {
      try {
        runtimes[i].shutdown();
        threads[i].join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private RayNativeRuntime getCurrentRuntime() {
    RayNativeRuntime currentRuntime = currentThreadRuntime.get();
    Preconditions.checkNotNull(currentRuntime,
        "RayRuntime is not set on current thread. If the task runs with multiple threads, please ensure Ray.asyncClosure is called when starting new threads.");
    return currentRuntime;
  }

  void setCurrentRuntime(RayNativeRuntime rayRuntime) {
    currentThreadRuntime.set(rayRuntime);
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
  public Runnable asyncClosure(Runnable runnable) {
    RayNativeRuntime runtime = getCurrentRuntime();
    return () -> {
      currentThreadRuntime.set(runtime);
      runnable.run();
    };
  }

  @Override
  public Callable asyncClosure(Callable callable) {
    RayNativeRuntime runtime = getCurrentRuntime();
    return () -> {
      currentThreadRuntime.set(runtime);
      return callable.call();
    };
  }
}
