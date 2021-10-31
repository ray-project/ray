package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ByteString;
import io.ray.api.BaseActorHandle;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Histogram;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.BackendVersion;
import io.ray.serve.generated.RequestWrapper;
import io.ray.serve.poll.KeyListener;
import io.ray.serve.poll.KeyType;
import io.ray.serve.poll.LongPollClient;
import io.ray.serve.poll.LongPollNamespace;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ReflectUtil;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles requests with the provided callable. */
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

  private LongPollClient longPollClient;

  private BackendVersion version;

  private boolean isDeleted = false;

  public RayServeReplica(
      Object callable,
      BackendConfig backendConfig,
      BackendVersion version,
      BaseActorHandle actorHandle) {
    this.backendTag = Serve.getReplicaContext().getBackendTag();
    this.replicaTag = Serve.getReplicaContext().getReplicaTag();
    this.callable = callable;
    this.config = backendConfig;
    this.version = version;

    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(
        new KeyType(LongPollNamespace.BACKEND_CONFIGS, backendTag),
        newConfig -> updateBackendConfigs(newConfig));
    this.longPollClient = new LongPollClient(actorHandle, keyListeners);
    this.longPollClient.start();
    registerMetrics();
  }

  private void registerMetrics() {
    RayServeMetrics.execute(
        () ->
            requestCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_BACKEND_REQUEST_COUNTER.getName())
                    .description(RayServeMetrics.SERVE_BACKEND_REQUEST_COUNTER.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            errorCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_BACKEND_ERROR_COUNTER.getName())
                    .description(RayServeMetrics.SERVE_BACKEND_ERROR_COUNTER.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            restartCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_BACKEND_REPLICA_STARTS.getName())
                    .description(RayServeMetrics.SERVE_BACKEND_REPLICA_STARTS.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            processingLatencyTracker =
                Metrics.histogram()
                    .name(RayServeMetrics.SERVE_BACKEND_PROCESSING_LATENCY_MS.getName())
                    .description(
                        RayServeMetrics.SERVE_BACKEND_PROCESSING_LATENCY_MS.getDescription())
                    .unit("")
                    .boundaries(Constants.DEFAULT_LATENCY_BUCKET_MS)
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            numProcessingItems =
                Metrics.gauge()
                    .name(RayServeMetrics.SERVE_REPLICA_PROCESSING_QUERIES.getName())
                    .description(RayServeMetrics.SERVE_REPLICA_PROCESSING_QUERIES.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_BACKEND,
                            backendTag,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(() -> restartCounter.inc(1.0));
  }

  public Object handleRequest(Query request) {
    long startTime = System.currentTimeMillis();
    LOGGER.debug(
        "Replica {} received request {}", replicaTag, request.getMetadata().getRequestId());

    numOngoingRequests.incrementAndGet();
    RayServeMetrics.execute(() -> numProcessingItems.update(numOngoingRequests.get()));
    Object result = invokeSingle(request);
    numOngoingRequests.decrementAndGet();

    long requestTimeMs = System.currentTimeMillis() - startTime;
    LOGGER.debug(
        "Replica {} finished request {} in {}ms",
        replicaTag,
        request.getMetadata().getRequestId(),
        requestTimeMs);

    return result;
  }

  private Object invokeSingle(Query requestItem) {

    long start = System.currentTimeMillis();
    Method methodToCall = null;
    try {
      LOGGER.debug(
          "Replica {} started executing request {}",
          replicaTag,
          requestItem.getMetadata().getRequestId());

      Object[] args = parseRequestItem(requestItem);
      methodToCall = getRunnerMethod(requestItem.getMetadata().getCallMethod(), args);
      Object result = methodToCall.invoke(callable, args);
      RayServeMetrics.execute(() -> requestCounter.inc(1.0));
      return result;
    } catch (Throwable e) {
      RayServeMetrics.execute(() -> errorCounter.inc(1.0));
      throw new RayServeException(
          LogUtil.format(
              "Replica {} failed to invoke method {}",
              replicaTag,
              methodToCall == null ? "unknown" : methodToCall.getName()),
          e);
    } finally {
      RayServeMetrics.execute(
          () -> processingLatencyTracker.update(System.currentTimeMillis() - start));
    }
  }

  private Object[] parseRequestItem(Query requestItem) {
    if (requestItem.getArgs() == null) {
      return new Object[0];
    }

    // From Java Proxy or Handle.
    if (requestItem.getArgs() instanceof Object[]) {
      return (Object[]) requestItem.getArgs();
    }

    // From other language Proxy or Handle.
    RequestWrapper requestWrapper = (RequestWrapper) requestItem.getArgs();
    if (requestWrapper.getBody() == null || requestWrapper.getBody().isEmpty()) {
      return new Object[0];
    }

    return MessagePackSerializer.decode(requestWrapper.getBody().toByteArray(), Object[].class);
  }

  private Method getRunnerMethod(String methodName, Object[] args) {

    try {
      return ReflectUtil.getMethod(callable.getClass(), methodName, args);
    } catch (NoSuchMethodException e) {
      throw new RayServeException(
          LogUtil.format(
              "Backend doesn't have method {} which is specified in the request. "
                  + "The available methods are {}",
              methodName,
              ReflectUtil.getMethodStrings(callable.getClass())));
    }
  }

  /**
   * Perform graceful shutdown. Trigger a graceful shutdown protocol that will wait for all the
   * queued tasks to be completed and return to the controller.
   */
  public synchronized boolean prepareForShutdown() {
    while (true) {
      // Sleep first because we want to make sure all the routers receive the notification to remove
      // this replica first.
      try {
        Thread.sleep((long) (config.getGracefulShutdownWaitLoopS() * 1000));
      } catch (InterruptedException e) {
        LOGGER.error(
            "Replica {} was interrupted in sheep when draining pending queries", replicaTag);
      }
      if (numOngoingRequests.get() == 0) {
        break;
      } else {
        LOGGER.info(
            "Waiting for an additional {}s to shut down because there are {} ongoing requests.",
            config.getGracefulShutdownWaitLoopS(),
            numOngoingRequests.get());
      }
    }

    // Explicitly call the del method to trigger clean up. We set isDeleted = true after
    // succssifully calling it so the destructor is called only once.
    try {
      if (!isDeleted) {
        ReflectUtil.getMethod(callable.getClass(), "del").invoke(callable);
      }
    } catch (NoSuchMethodException e) {
      LOGGER.warn("Deployment {} has no del method.", backendTag);
    } catch (Throwable e) {
      LOGGER.error("Exception during graceful shutdown of replica.");
    } finally {
      isDeleted = true;
    }
    return true;
  }

  /**
   * Reconfigure user's configuration in the callable object through its reconfigure method.
   *
   * @param userConfig new user's configuration
   */
  public BackendVersion reconfigure(Object userConfig) {
    BackendVersion.Builder builder = BackendVersion.newBuilder();
    builder.setCodeVersion(version.getCodeVersion());
    if (userConfig != null) {
      builder.setUserConfig(ByteString.copyFrom((byte[]) userConfig));
    }
    version = builder.build();

    try {
      Method reconfigureMethod =
          ReflectUtil.getMethod(
              callable.getClass(),
              Constants.BACKEND_RECONFIGURE_METHOD,
              userConfig != null
                  ? MessagePackSerializer.decode((byte[]) userConfig, Object[].class)
                  : new Object[0]); // TODO cache reconfigure method
      reconfigureMethod.invoke(callable, userConfig);
    } catch (NoSuchMethodException e) {
      LOGGER.warn(
          "user_config specified but backend {} missing {} method",
          backendTag,
          Constants.BACKEND_RECONFIGURE_METHOD);
    } catch (Throwable e) {
      throw new RayServeException(
          LogUtil.format("Backend {} failed to reconfigure user_config {}", backendTag, userConfig),
          e);
    }
    return version;
  }

  /**
   * Update backend configs.
   *
   * @param newConfig the new configuration of backend
   */
  private void updateBackendConfigs(Object newConfig) {
    config = (BackendConfig) newConfig;
  }

  public BackendVersion getVersion() {
    return version;
  }
}
