package io.ray.serve.proxy;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import io.ray.api.BaseActorHandle;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.exception.RayServeException;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.poll.KeyListener;
import io.ray.serve.poll.KeyType;
import io.ray.serve.poll.LongPollClient;
import io.ray.serve.poll.LongPollNamespace;
import io.ray.serve.util.CollectionUtil;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.ReflectUtil;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyActor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProxyActor.class);

  private Map<String, String> config;

  private Map<String, ServeProxy> proxies = new ConcurrentHashMap<>();

  /** Used only for displaying the route table. Key: route, value: endpoint. */
  private volatile Map<String, EndpointInfo> routeInfo = new HashMap<>();

  @SuppressWarnings("unused")
  private LongPollClient longPollClient;

  private ProxyRouter proxyRouter = new ProxyRouter();

  public ProxyActor(String controllerName, Map<String, String> config) {
    this.config = config;

    // Set the controller name so that serve will connect to the controller instance this proxy is
    // running in.
    Serve.setInternalReplicaContext(null, null, controllerName, null, config);

    Optional<BaseActorHandle> optional = Ray.getActor(controllerName, Constants.SERVE_NAMESPACE);
    Preconditions.checkState(optional.isPresent(), "Controller does not exist");

    Map<KeyType, KeyListener> keyListeners = new HashMap<>();
    keyListeners.put(
        new KeyType(LongPollNamespace.ROUTE_TABLE, null), endpoints -> updateRoutes(endpoints));
    this.longPollClient = new LongPollClient(optional.get(), keyListeners);
    this.run();
  }

  private void run() {
    startupProxy();
    registerServiceDiscovery();
  }

  private void startupProxy() {

    List<ServeProxy> serveProxies = null;

    // Get proxy instances according to class names.
    String proxyClassNames = config != null ? config.get(RayServeConfig.PROXY_CLASS) : null;
    if (StringUtils.isNotBlank(proxyClassNames)) {
      try {
        serveProxies = ReflectUtil.getInstancesByClassNames(proxyClassNames, ServeProxy.class);
      } catch (ClassNotFoundException
          | InstantiationException
          | IllegalAccessException
          | IllegalArgumentException
          | InvocationTargetException
          | NoSuchMethodException
          | SecurityException e) {
        String errorMsg =
            LogUtil.format("Failed to initialize proxies by class names : {}", proxyClassNames);
        LOGGER.error(errorMsg, e);
        throw new RayServeException(errorMsg, e);
      }
    }

    // Get proxy instances through SPI.
    if (CollectionUtil.isEmpty(serveProxies)) {
      List<ServeProxy> spiProxies = new ArrayList<>();
      ServiceLoader<ServeProxy> serviceLoader = ServiceLoader.load(ServeProxy.class);
      serviceLoader.forEach(serveProxy -> spiProxies.add(serveProxy));
      serveProxies = spiProxies;
    }

    // Set the default proxy if proxies still empty.
    if (CollectionUtil.isEmpty(serveProxies)) {
      serveProxies = Lists.newArrayList(new HttpProxy());
    }

    if (!CollectionUtil.isEmpty(serveProxies)) {
      for (ServeProxy serveProxy : serveProxies) {
        if (proxies.containsKey(serveProxy.getName())) {
          String errorMsg =
              LogUtil.format(
                  "Proxy {} name {} is duplicate with proxy {} name {}",
                  serveProxy.getClass().getName(),
                  serveProxy.getName(),
                  proxies.get(serveProxy.getName()).getClass().getName(),
                  proxies.get(serveProxy.getName()).getName());
          LOGGER.error(errorMsg);
          throw new RayServeException(errorMsg);
        }
        proxies.put(serveProxy.getName(), serveProxy);
        serveProxy.init(config, proxyRouter);
        LOGGER.info("Proxy actor initialized proxy: {}", serveProxy.getName());
      }
    }
  }

  public void registerServiceDiscovery() {
    proxies.forEach((key, value) -> value.registerServiceDiscovery());
  }

  public void updateRoutes(Object endpoints) {
    Map<String, EndpointInfo> endpointInfos = ((EndpointSet) endpoints).getEndpointsMap();
    Map<String, EndpointInfo> routeInfo = new HashMap<>();
    if (endpointInfos != null) {
      endpointInfos.forEach(
          (key, value) ->
              routeInfo.put(
                  StringUtils.isNotBlank(value.getRoute()) ? value.getRoute() : key, value));
    }
    this.routeInfo = routeInfo;
    this.proxyRouter.updateRoutes(endpointInfos);
  }

  public boolean ready() {
    return true;
  }

  public boolean blockUntilEndpointExists(String endpoint, double timeoutS) {
    long timeoutMs = (long) (timeoutS * 1000);
    long startTime = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() - startTime > timeoutMs) {
        throw new RayServeException(
            LogUtil.format("Waited {} for {} to propagate.", timeoutS, endpoint));
      }
      for (EndpointInfo endpointInfo : routeInfo.values()) {
        if (StringUtils.equals(endpointInfo.getEndpointName(), endpoint)) {
          return true;
        }
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException e) {
        LOGGER.error(
            "The sleeping was interrupted when waiting for the endpoint {} being existing.",
            endpoint,
            e);
      }
    }
  }

  public ProxyRouter getProxyRouter() {
    return proxyRouter;
  }

  public Map<String, ServeProxy> getProxies() {
    return proxies;
  }
}
