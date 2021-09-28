package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Histogram;
import io.ray.runtime.metric.MetricConfig;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.DeploymentConfig;
import io.ray.serve.generated.RequestWrapper;
import io.ray.serve.poll.KeyListener;
import io.ray.serve.poll.KeyType;
import io.ray.serve.poll.LongPollClient;
import io.ray.serve.poll.LongPollNamespace;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ReflectUtil;
import io.ray.serve.util.ServeProtoUtil;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles requests with the provided callable. */
public class RayServeReplica {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayServeReplica.class);

  private String deploymentTag;

  private String replicaTag;

  private DeploymentConfig config;

  private AtomicInteger numOngoingRequests = new AtomicInteger();

  private Object callable;

  private boolean metricsRegistered = false;

  private Count requestCounter;

  private Count errorCounter;

  private Count restartCounter;

  private Histogram processingLatencyTracker;

  private Gauge numProcessingItems;

  private LongPollClient longPollClient;

  public RayServeReplica(
      Object callable, DeploymentConfig deploymentConfig, BaseActorHandle actorHandle) {
    this.deploymentTag = Serve.getReplicaContext().getDeploymentTag();
    this.replicaTag = Serve.getReplicaContext().getReplicaTag();
    this.callable = callable;
    this.config = deploymentConfig;
    this.reconfigure(ServeProtoUtil.parseUserConfig(deploymentConfig));

    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(
        new KeyType(LongPollNamespace.DEPLOYMENT_CONFIGS, deploymentTag),
        newConfig -> updateDeploymentConfigs(newConfig));
    this.longPollClient = new LongPollClient(actorHandle, keyListeners);
    this.longPollClient.start();
    registerMetrics();
  }

  private void registerMetrics() {
    if (!Ray.isInitialized() || Ray.getRuntimeContext().isSingleProcess()) {
      return;
    }

    Metrics.init(MetricConfig.DEFAULT_CONFIG);
    requestCounter =
        Metrics.count()
            .name("serve_deployment_request_counter")
            .description("The number of queries that have been processed in this replica.")
            .unit("")
            .tags(ImmutableMap.of("deployment", deploymentTag, "replica", replicaTag))
            .register();

    errorCounter =
        Metrics.count()
            .name("serve_deployment_error_counter")
            .description("The number of exceptions that have occurred in this replica.")
            .unit("")
            .tags(ImmutableMap.of("deployment", deploymentTag, "replica", replicaTag))
            .register();

    restartCounter =
        Metrics.count()
            .name("serve_deployment_replica_starts")
            .description("The number of times this replica has been restarted due to failure.")
            .unit("")
            .tags(ImmutableMap.of("deployment", deploymentTag, "replica", replicaTag))
            .register();

    processingLatencyTracker =
        Metrics.histogram()
            .name("serve_deployment_processing_latency_ms")
            .description("The latency for queries to be processed.")
            .unit("")
            .boundaries(Constants.DEFAULT_LATENCY_BUCKET_MS)
            .tags(ImmutableMap.of("deployment", deploymentTag, "replica", replicaTag))
            .register();

    numProcessingItems =
        Metrics.gauge()
            .name("serve_replica_processing_queries")
            .description("The current number of queries being processed.")
            .unit("")
            .tags(ImmutableMap.of("deployment", deploymentTag, "replica", replicaTag))
            .register();

    metricsRegistered = true;

    restartCounter.inc(1.0);
  }

  public Object handleRequest(Query request) {
    long startTime = System.currentTimeMillis();
    LOGGER.debug(
        "Replica {} received request {}", replicaTag, request.getMetadata().getRequestId());

    numOngoingRequests.incrementAndGet();
    reportMetrics(() -> numProcessingItems.update(numOngoingRequests.get()));
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
      reportMetrics(() -> requestCounter.inc(1.0));
      return result;
    } catch (Throwable e) {
      reportMetrics(() -> errorCounter.inc(1.0));
      throw new RayServeException(
          LogUtil.format(
              "Replica {} failed to invoke method {}",
              replicaTag,
              methodToCall == null ? "unknown" : methodToCall.getName()),
          e);
    } finally {
      reportMetrics(() -> processingLatencyTracker.update(System.currentTimeMillis() - start));
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
  public void drainPendingQueries() {
    while (true) {
      try {
        Thread.sleep((long) (config.getExperimentalGracefulShutdownWaitLoopS() * 1000));
      } catch (InterruptedException e) {
        LOGGER.error(
            "Replica {} was interrupted in sheep when draining pending queries", replicaTag);
      }
      if (numOngoingRequests.get() == 0) {
        break;
      } else {
        LOGGER.debug(
            "Waiting for an additional {}s to shut down because there are {} ongoing requests.",
            config.getExperimentalGracefulShutdownWaitLoopS(),
            numOngoingRequests.get());
      }
    }
    Ray.exitActor();
  }

  /**
   * Reconfigure user's configuration in the callable object through its reconfigure method.
   *
   * @param userConfig new user's configuration
   */
  private void reconfigure(Object userConfig) {
    if (userConfig == null) {
      return;
    }
    try {
      Method reconfigureMethod =
          ReflectUtil.getMethod(
              callable.getClass(),
              Constants.DEPLOYMENT_RECONFIGURE_METHOD,
              userConfig); // TODO cache reconfigureMethod
      reconfigureMethod.invoke(callable, userConfig);
    } catch (NoSuchMethodException e) {
      throw new RayServeException(
          LogUtil.format(
              "user_config specified but deployment {} missing {} method",
              deploymentTag,
              Constants.DEPLOYMENT_RECONFIGURE_METHOD));
    } catch (Throwable e) {
      throw new RayServeException(
          LogUtil.format(
              "Deployment {} failed to reconfigure user_config {}", deploymentTag, userConfig),
          e);
    }
  }

  /**
   * Update deployment configs.
   *
   * @param newConfig the new configuration of deployment
   */
  private void updateDeploymentConfigs(Object newConfig) {
    config = (DeploymentConfig) newConfig;
    reconfigure(((DeploymentConfig) newConfig).getUserConfig());
  }

  private void reportMetrics(Runnable runnable) {
    if (metricsRegistered) {
      runnable.run();
    }
  }
}
