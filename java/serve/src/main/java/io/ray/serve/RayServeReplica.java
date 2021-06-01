package io.ray.serve;

import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Histogram;
import io.ray.runtime.metric.TagKey;
import io.ray.serve.api.Serve;
import io.ray.serve.poll.KeyListener;
import io.ray.serve.poll.KeyType;
import io.ray.serve.poll.LongPollClient;
import io.ray.serve.poll.LongPollNamespace;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ReflectUtil;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Handles requests with the provided callable.
 */
public class RayServeReplica {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayServeReplica.class);

  private String backendTag;

  private String replicaTag;

  private BackendConfig config;

  private AtomicInteger numOngoingRequests = new AtomicInteger();

  private Object callable;

  private Count requestCounter;

  private Count errorCounter;

  private Count restartCounter;

  private Histogram processingLatencyTracker;

  private Gauge numProcessingItems;

  @SuppressWarnings("unused")
  private LongPollClient longPollClient;

  public RayServeReplica(Object callable, BackendConfig backendConfig, BaseActorHandle actorHandle)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    this.backendTag = Serve.getReplicaContext().getBackendTag();
    this.replicaTag = Serve.getReplicaContext().getReplicaTag();
    this.callable = callable;
    this.config = backendConfig;
    this.reconfigure(backendConfig.getUserConfig());

    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(new KeyType(LongPollNamespace.BACKEND_CONFIGS, backendTag),
        newConfig -> updateBackendConfigs(newConfig));
    this.longPollClient = new LongPollClient(actorHandle, keyListeners);

    Map<TagKey, String> backendTags = new HashMap<>();
    backendTags.put(new TagKey("backend"), backendTag);
    this.requestCounter = new Count("serve_backend_request_counter",
        "The number of queries that have been processed in this replica.", "", backendTags);
    this.errorCounter = new Count("serve_backend_error_counter",
        "The number of exceptions that have occurred in the backend.", "", backendTags);

    Map<TagKey, String> replicaTags = new HashMap<>();
    replicaTags.put(new TagKey("backend"), backendTag);
    replicaTags.put(new TagKey("replica"), replicaTag);
    this.restartCounter = new Count("serve_backend_replica_starts",
        "The number of times this replica has been restarted due to failure.", "", replicaTags);
    this.processingLatencyTracker = new Histogram("serve_backend_processing_latency_ms",
        "The latency for queries to be processed.", "", Constants.DEFAULT_LATENCY_BUCKET_MS,
        replicaTags);
    this.numProcessingItems = new Gauge("serve_replica_processing_queries",
        "The current number of queries being processed.", "", replicaTags);

    this.restartCounter.inc(1.0);

    LogUtil.setLayout(backendTag, replicaTag);

  }

  public Object handleRequest(Query request) throws Throwable {
    long startTime = System.currentTimeMillis();
    LOGGER.debug("Replica {} received request {}", this.replicaTag,
        request.getMetadata().getRequestId());

    this.numProcessingItems.update(numOngoingRequests.incrementAndGet());
    Object result = invokeSingle(request);
    this.numOngoingRequests.decrementAndGet();

    long requestTimeMs = System.currentTimeMillis() - startTime;
    LOGGER.debug("Replica {} finished request {} in {}ms", this.replicaTag,
        request.getMetadata().getRequestId(), requestTimeMs);

    return result;
  }

  private Object invokeSingle(Query requestItem) throws Throwable {
    LOGGER.debug("Replica {} started executing request {}", this.replicaTag,
        requestItem.getMetadata().getRequestId());

    long start = System.currentTimeMillis();
    Method methodToCall = null;
    Object result = null;
    try {
      methodToCall = getRunnerMethod(requestItem);
      result = methodToCall.invoke(callable, requestItem.getArgs());
      this.requestCounter.inc(1.0);
    } catch (Throwable e) {
      this.errorCounter.inc(1.0);
      throw e;
    }

    long latencyMs = System.currentTimeMillis() - start;
    this.processingLatencyTracker.update(latencyMs);

    return result;
  }

  private Method getRunnerMethod(Query query) {
    String methodName = query.getMetadata().getCallMethod();

    try {
      return ReflectUtil.getMethod(callable.getClass(), methodName,
          query.getArgs() == null ? null : query.getArgs());
    } catch (NoSuchMethodException e) {
      throw new RayServeException(LogUtil.format(
          "Backend doesn't have method {} which is specified in the request. "
              + "The available methods are {}",
          methodName, ReflectUtil.getMethodStrings(callable.getClass())));
    }

  }

  /**
   * Perform graceful shutdown. Trigger a graceful shutdown protocol that will wait for all the
   * queued tasks to be completed and return to the controller.
   */
  public void drainPendingQueries() {
    while (true) {
      try {
        Thread.sleep(config.getExperimentalGracefulShutdownWaitLoopS() * 1000);
      } catch (InterruptedException e) {
        LOGGER.error("Replica {} was interrupted in sheep when draining pending queries",
            this.replicaTag);
      }
      if (this.numOngoingRequests.get() == 0) {
        break;
      } else {
        LOGGER.debug(
            "Waiting for an additional {}s to shut down because there are {} ongoing requests.",
            this.config.getExperimentalGracefulShutdownWaitLoopS(), this.numOngoingRequests.get());
      }
    }
    Ray.exitActor();
  }

  /**
   * Reconfigure user's configuration in the callable object through its reconfigure method.
   * 
   * @param userConfig new user's configuration
   * @throws IllegalAccessException from {@link Method#invoke(Object, Object...)}}
   * @throws IllegalArgumentException from {@link Method#invoke(Object, Object...)}}
   * @throws InvocationTargetException from {@link Method#invoke(Object, Object...)}}
   */
  private void reconfigure(Object userConfig)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    Method reconfigureMethod = null;
    try {
      reconfigureMethod = ReflectUtil.getMethod(callable.getClass(),
          Constants.BACKEND_RECONFIGURE_METHOD, userConfig);
    } catch (NoSuchMethodException e) {
      throw new RayServeException(
          LogUtil.format("user_config specified but backend {}  missing {} method", backendTag,
              Constants.BACKEND_RECONFIGURE_METHOD));
    }
    reconfigureMethod.invoke(callable, userConfig);
  }

  /**
   * Update backend configs.
   * 
   * @param newConfig the new configuration of backend
   * @throws IllegalAccessException from {@link RayServeReplica#reconfigure(Object)}
   * @throws IllegalArgumentException from {@link RayServeReplica#reconfigure(Object)}
   * @throws InvocationTargetException from {@link RayServeReplica#reconfigure(Object)}
   */
  private void updateBackendConfigs(Object newConfig)
      throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
    config = (BackendConfig) newConfig;
    reconfigure(((BackendConfig) newConfig).getUserConfig());
  }

}
