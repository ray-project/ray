package io.ray.streaming.runtime.rpc.async;

import io.ray.api.ObjectRef;
import io.ray.api.Ray;
import io.ray.api.WaitResult;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoteCallPool implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallPool.class);
  private static final int WAIT_TIME_MS = 5;
  private static final long WARNING_PERIOD = 10000;
  private final List<RemoteCallBundle> pendingObjectBundles = new LinkedList<>();
  private Map<RemoteCallBundle, Callback<Object>> singletonHandlerMap = new ConcurrentHashMap<>();
  private Map<RemoteCallBundle, Callback<List<Object>>> bundleHandlerMap =
      new ConcurrentHashMap<>();
  private Map<RemoteCallBundle, ExceptionHandler<Throwable>> bundleExceptionHandlerMap =
      new ConcurrentHashMap<>();
  private ThreadPoolExecutor callBackPool =
      new ThreadPoolExecutor(
          2,
          Runtime.getRuntime().availableProcessors(),
          1,
          TimeUnit.MINUTES,
          new LinkedBlockingQueue<>(),
          new CallbackThreadFactory());
  private volatile boolean stop = false;

  public RemoteCallPool() {
    Thread t = new Thread(Ray.wrapRunnable(this), "remote-pool-loop");
    t.setUncaughtExceptionHandler(
        (thread, throwable) -> LOG.error("Error in remote call pool thread.", throwable));
    t.start();
  }

  @SuppressWarnings("unchecked")
  public <T> void bindCallback(
      ObjectRef<T> obj, Callback<T> callback, ExceptionHandler<Throwable> onException) {
    List objectRefList = Collections.singletonList(obj);
    RemoteCallBundle bundle = new RemoteCallBundle(objectRefList, true);
    singletonHandlerMap.put(bundle, (Callback<Object>) callback);
    bundleExceptionHandlerMap.put(bundle, onException);
    synchronized (pendingObjectBundles) {
      pendingObjectBundles.add(bundle);
    }
  }

  public void bindCallback(
      List<ObjectRef<Object>> objectBundle,
      Callback<List<Object>> callback,
      ExceptionHandler<Throwable> onException) {
    RemoteCallBundle bundle = new RemoteCallBundle(objectBundle, false);
    bundleHandlerMap.put(bundle, callback);
    bundleExceptionHandlerMap.put(bundle, onException);
    synchronized (pendingObjectBundles) {
      pendingObjectBundles.add(bundle);
    }
  }

  public void stop() {
    stop = true;
  }

  public void run() {
    while (!stop) {
      try {
        if (pendingObjectBundles.isEmpty()) {
          Thread.sleep(WAIT_TIME_MS);
          continue;
        }
        synchronized (pendingObjectBundles) {
          Iterator<RemoteCallBundle> itr = pendingObjectBundles.iterator();
          while (itr.hasNext()) {
            RemoteCallBundle bundle = itr.next();
            WaitResult<Object> waitResult =
                Ray.wait(bundle.objects, bundle.objects.size(), WAIT_TIME_MS);
            List<ObjectRef<Object>> readyObjs = waitResult.getReady();
            if (readyObjs.size() != bundle.objects.size()) {
              long now = System.currentTimeMillis();
              long waitingTime = now - bundle.createTime;
              if (waitingTime > WARNING_PERIOD && now - bundle.lastWarnTs > WARNING_PERIOD) {
                bundle.lastWarnTs = now;
                LOG.warn("Bundle has being waiting for {} ms, bundle = {}.", waitingTime, bundle);
              }
              continue;
            }

            ExceptionHandler<Throwable> exceptionHandler = bundleExceptionHandlerMap.get(bundle);
            if (bundle.isSingletonBundle) {
              callBackPool.execute(
                  Ray.wrapRunnable(
                      () -> {
                        try {
                          singletonHandlerMap.get(bundle).handle(readyObjs.get(0).get());
                          singletonHandlerMap.remove(bundle);
                        } catch (Throwable th) {
                          LOG.error(
                              "Error when get object, objectId = {}.",
                              readyObjs.get(0).toString(),
                              th);
                          if (exceptionHandler != null) {
                            exceptionHandler.handle(th);
                          }
                        }
                      }));
            } else {
              List<Object> results =
                  readyObjs.stream().map(ObjectRef::get).collect(Collectors.toList());
              List<String> resultIds =
                  readyObjs.stream().map(ObjectRef::toString).collect(Collectors.toList());
              callBackPool.execute(
                  Ray.wrapRunnable(
                      () -> {
                        try {
                          bundleHandlerMap.get(bundle).handle(results);
                          bundleHandlerMap.remove(bundle);
                        } catch (Throwable th) {
                          LOG.error("Error when get object, objectIds = {}.", resultIds, th);
                          if (exceptionHandler != null) {
                            exceptionHandler.handle(th);
                          }
                        }
                      }));
            }
            itr.remove();
          }
        }

      } catch (Exception e) {
        LOG.error("Exception in wait loop.", e);
      }
    }
    LOG.info("Wait loop finished.");
  }

  @FunctionalInterface
  public interface ExceptionHandler<T> {

    void handle(T object);
  }

  @FunctionalInterface
  public interface Callback<T> {

    void handle(T object) throws Throwable;
  }

  private static class RemoteCallBundle {

    List<ObjectRef<Object>> objects;
    boolean isSingletonBundle;
    long lastWarnTs = System.currentTimeMillis();
    long createTime = System.currentTimeMillis();

    RemoteCallBundle(List<ObjectRef<Object>> objects, boolean isSingletonBundle) {
      this.objects = objects;
      this.isSingletonBundle = isSingletonBundle;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("[");
      objects.forEach(rayObj -> sb.append(rayObj.toString()).append(","));
      sb.append("]");
      return sb.toString();
    }
  }

  static class CallbackThreadFactory implements ThreadFactory {

    private AtomicInteger cnt = new AtomicInteger(0);

    @Override
    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setUncaughtExceptionHandler((thread, throwable) -> LOG.error("Callback err.", throwable));
      t.setName("callback-thread-" + cnt.getAndIncrement());
      return t;
    }
  }
}
