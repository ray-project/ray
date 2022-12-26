package io.ray.serve.replica;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.gson.Gson;
import io.ray.api.BaseActorHandle;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Gauge;
import io.ray.runtime.metric.Histogram;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.deployment.DeploymentVersion;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.RequestMetadata;
import io.ray.serve.generated.RequestWrapper;
import io.ray.serve.metrics.RayServeMetrics;
import io.ray.serve.router.Query;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ReflectUtil;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles requests with the provided callable. */
public class RayServeReplicaImpl implements RayServeReplica {

  private static final Logger LOGGER = LoggerFactory.getLogger(RayServeReplicaImpl.class);

  private String deploymentName;

  private String replicaTag;

  private DeploymentConfig config;

  private AtomicInteger numOngoingRequests = new AtomicInteger();

  private Object callable;

  private Count requestCounter;

  private Count errorCounter;

  private Count restartCounter;

  private Histogram processingLatencyTracker;

  private Gauge numProcessingItems;

  private DeploymentVersion version;

  private boolean isDeleted = false;

  private final Method checkHealthMethod;

  private final Method callMethod;

  private Map<String, Pair<Method, Object>> signatures;

  public RayServeReplicaImpl(
      Object callable,
      DeploymentConfig deploymentConfig,
      DeploymentVersion version,
      BaseActorHandle actorHandle) {
    this.deploymentName = Serve.getReplicaContext().getDeploymentName();
    this.replicaTag = Serve.getReplicaContext().getReplicaTag();
    this.callable = callable;
    this.config = deploymentConfig;
    this.version = version;
    this.checkHealthMethod = getRunnerMethod(Constants.CHECK_HEALTH_METHOD, null, true);
    this.callMethod = getRunnerMethod(Constants.CALL_METHOD, new Object[] {new Object()}, true);
    registerMetrics();
    this.signatures = CallableUtils.getSignatures(deploymentConfig.getIngress(), callable);
  }

  private void registerMetrics() {
    RayServeMetrics.execute(
        () ->
            requestCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_DEPLOYMENT_REQUEST_COUNTER.getName())
                    .description(RayServeMetrics.SERVE_DEPLOYMENT_REQUEST_COUNTER.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_DEPLOYMENT,
                            deploymentName,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            errorCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_DEPLOYMENT_ERROR_COUNTER.getName())
                    .description(RayServeMetrics.SERVE_DEPLOYMENT_ERROR_COUNTER.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_DEPLOYMENT,
                            deploymentName,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            restartCounter =
                Metrics.count()
                    .name(RayServeMetrics.SERVE_DEPLOYMENT_REPLICA_STARTS.getName())
                    .description(RayServeMetrics.SERVE_DEPLOYMENT_REPLICA_STARTS.getDescription())
                    .unit("")
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_DEPLOYMENT,
                            deploymentName,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(
        () ->
            processingLatencyTracker =
                Metrics.histogram()
                    .name(RayServeMetrics.SERVE_DEPLOYMENT_PROCESSING_LATENCY_MS.getName())
                    .description(
                        RayServeMetrics.SERVE_DEPLOYMENT_PROCESSING_LATENCY_MS.getDescription())
                    .unit("")
                    .boundaries(Constants.DEFAULT_LATENCY_BUCKET_MS)
                    .tags(
                        ImmutableMap.of(
                            RayServeMetrics.TAG_DEPLOYMENT,
                            deploymentName,
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
                            RayServeMetrics.TAG_DEPLOYMENT,
                            deploymentName,
                            RayServeMetrics.TAG_REPLICA,
                            replicaTag))
                    .register());

    RayServeMetrics.execute(() -> restartCounter.inc(1.0));
  }

  @Override
  public Object handleRequest(Object requestMetadata, Object requestArgs) {
    long startTime = System.currentTimeMillis();
    Query request = new Query((RequestMetadata) requestMetadata, requestArgs);
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
      Object result = null;
      if (StringUtils.isNotBlank(requestItem.getMetadata().getCallMethodSignature())
          && signatures.containsKey(requestItem.getMetadata().getCallMethodSignature())) {
        Pair<Method, Object> methodCallablePair =
            signatures.get(requestItem.getMetadata().getCallMethodSignature());
        methodToCall = methodCallablePair.getLeft();
        result = methodToCall.invoke(methodCallablePair.getRight(), args);
      } else {
        methodToCall =
            args.length == 1 && callMethod != null
                ? callMethod
                : getRunnerMethod(requestItem.getMetadata().getCallMethod(), args, false);
        result = methodToCall.invoke(callable, args);
      }
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
    // Compatible with some http calls without http ingress
    if (StringUtils.equals(
        requestItem.getMetadata().getCallMethodSignature(), Constants.HTTP_PROXY_SIGNATURE)) {
      LOGGER.info(
          "body is {}, scope is {}",
          requestWrapper.getBody().toStringUtf8(),
          requestWrapper.getScopeMap().toString());
      if (signatures.containsKey(Constants.HTTP_PROXY_SIGNATURE)) {
        return new Object[] {requestWrapper};
      } else if (StringUtils.isNotBlank(requestWrapper.getScopeMap().get("query_string"))) {
        String[] fields = requestWrapper.getScopeMap().get("query_string").split("&");
        List<String> args = Lists.newArrayList();
        for (String field : fields) {
          String[] tmp = field.split("=");
          if (tmp.length > 1) {
            args.add(tmp[1]);
          }
        }
        return args.toArray();
      } else if (!requestWrapper.getBody().isEmpty()
          && StringUtils.contains(
              requestWrapper.getScopeMap().get("headers"), "application/json")) {
        Gson gson = new Gson();
        Object temp = gson.fromJson(requestWrapper.getBody().toStringUtf8(), Object.class);
        return new Object[] {temp};
      }
    }
    if (requestWrapper.getBody().isEmpty()) {
      return new Object[0];
    }

    return new Object[] {
      MessagePackSerializer.decode(requestWrapper.getBody().toByteArray(), Object.class)
    };
  }

  private Method getRunnerMethod(String methodName, Object[] args, boolean isNullable) {
    try {
      if (StringUtils.equals(methodName, Constants.HTTP_PROXY_SIGNATURE)) {
        methodName = Constants.CALL_METHOD;
      }
      return ReflectUtil.getMethod(callable.getClass(), methodName, args);
    } catch (NoSuchMethodException e) {
      String errMsg =
          LogUtil.format(
              "Tried to call a method {} that does not exist. Available methods: {}",
              methodName,
              ReflectUtil.getMethodStrings(callable.getClass()));
      if (isNullable) {
        LOGGER.warn(errMsg);
        return null;
      } else {
        LOGGER.error(errMsg, e);
        throw new RayServeException(errMsg, e);
      }
    }
  }

  /**
   * Perform graceful shutdown. Trigger a graceful shutdown protocol that will wait for all the
   * queued tasks to be completed and return to the controller.
   *
   * @return true if it is ready for shutdown.
   */
  @Override
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
      LOGGER.warn("Deployment {} has no del method.", deploymentName);
    } catch (Throwable e) {
      LOGGER.error("Exception during graceful shutdown of replica.");
    } finally {
      isDeleted = true;
    }
    return true;
  }

  @Override
  public DeploymentVersion reconfigure(Object userConfig) {
    DeploymentVersion deploymentVersion =
        new DeploymentVersion(version.getCodeVersion(), userConfig);
    version = deploymentVersion;
    if (userConfig == null) {
      return deploymentVersion;
    }

    LOGGER.info(
        "Replica {} of deployment {} reconfigure userConfig: {}",
        replicaTag,
        deploymentName,
        userConfig);
    try {
      ReflectUtil.getMethod(callable.getClass(), Constants.RECONFIGURE_METHOD, userConfig)
          .invoke(callable, userConfig);
      return version;
    } catch (NoSuchMethodException e) {
      String errMsg =
          LogUtil.format(
              "userConfig specified but deployment {} missing {} method",
              deploymentName,
              Constants.RECONFIGURE_METHOD);
      LOGGER.error(errMsg);
      throw new RayServeException(errMsg, e);
    } catch (Throwable e) {
      String errMsg =
          LogUtil.format(
              "Replica {} of deployment {} failed to reconfigure userConfig {}",
              replicaTag,
              deploymentName,
              userConfig);
      LOGGER.error(errMsg);
      throw new RayServeException(errMsg, e);
    } finally {
      LOGGER.info(
          "Replica {} of deployment {} finished reconfiguring userConfig: {}",
          replicaTag,
          deploymentName,
          userConfig);
    }
  }

  public DeploymentVersion getVersion() {
    return version;
  }

  @Override
  public boolean checkHealth() {
    if (checkHealthMethod == null) {
      return true;
    }
    boolean result = true;
    try {
      LOGGER.info(
          "Replica {} of deployment {} check health of {}",
          replicaTag,
          deploymentName,
          callable.getClass().getName());
      Object isHealthy = checkHealthMethod.invoke(callable);
      if (!(isHealthy instanceof Boolean)) {
        LOGGER.error(
            "The health check result {} of {} in replica {} of deployment {} is illegal.",
            isHealthy == null ? "null" : isHealthy.getClass().getName() + ":" + isHealthy,
            callable.getClass().getName(),
            replicaTag,
            deploymentName);
        result = false;
      } else {
        result = (boolean) isHealthy;
      }
    } catch (Throwable e) {
      LOGGER.error(
          "Replica {} of deployment {} failed to check health of {}",
          replicaTag,
          deploymentName,
          callable.getClass().getName(),
          e);
      result = false;
    } finally {
      LOGGER.info(
          "The health check result of {} in replica {} of deployment {} is {}.",
          callable.getClass().getName(),
          replicaTag,
          deploymentName,
          result);
    }
    return result;
  }

  public Object getCallable() {
    return callable;
  }
}
