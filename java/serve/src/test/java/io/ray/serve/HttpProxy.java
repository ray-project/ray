package io.ray.serve;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.ray.api.Ray;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.serializer.MessagePackSerializer;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpProxy implements ServeProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(ProxyActor.class);

  private HttpRouter router;

  private int port;

  private HttpServer httpServer;

  private ThreadPoolExecutor threadPoolExecutor;

  private Count requestCounter;

  private HttpHandler httpHandler = new HttpProxyHandler();

  @Override
  public void init(Map<String, String> config, ProxyRouter router) {
    this.router = new HttpRouter();
    this.port =
        Integer.valueOf(
            Optional.ofNullable(config.get("ray.serve.proxy.http.port")).orElse("8341"));
    registerMetrics();
    startupHttpServer(config);
  }

  private void startupHttpServer(Map<String, String> config) {
    try {
      this.httpServer = HttpServer.create(new InetSocketAddress(port), 200);
    } catch (IOException e) {
      throw new RayServeException("Failed to create HTTP server.", e);
    }
    this.threadPoolExecutor =
        new ThreadPoolExecutor(
            Integer.parseInt(
                Optional.ofNullable(config.get("ray.serve.proxy.http.port.core.size"))
                    .orElse("10")),
            Integer.parseInt(
                Optional.ofNullable(config.get("ray.serve.proxy.http.port.max.size")).orElse("10")),
            300L,
            TimeUnit.SECONDS,
            new LinkedBlockingQueue<Runnable>(100));
    httpServer.setExecutor(this.threadPoolExecutor);
    httpServer.start();
  }

  @Override
  public void updateRoutes(Map<String, EndpointInfo> endpoints) {
    Set<String> oldRoutes = new HashSet<>(router.getRouteInfo().keySet());
    router.updateRoutes(endpoints);

    // Add new route into HTTP server.
    if (endpoints != null) {
      endpoints.forEach(
          (key, value) -> {
            String route = StringUtils.isNotBlank(value.getRoute()) ? value.getRoute() : key;

            if (!oldRoutes.contains(route)) {
              oldRoutes.remove(route);
            } else {
              httpServer.createContext(route, httpHandler);
            }
          });
    }

    // Delete the unused route from HTTP server.
    oldRoutes.forEach(route -> httpServer.removeContext(route));
  }

  private class HttpProxyHandler implements HttpHandler {

    @Override
    public void handle(HttpExchange httpExchange) throws IOException {

      int code = HttpURLConnection.HTTP_OK;
      try {
        requestCounter.inc(1.0);

        String route = httpExchange.getRequestURI().getPath();
        String method = httpExchange.getRequestMethod();

        Object[] parameters = null;
        InputStream inputStream = httpExchange.getRequestBody();
        if (null == inputStream || 0 >= inputStream.available()) {
          parameters = new Object[0];
        } else {
          byte[] body = new byte[inputStream.available()];
          inputStream.read(body, 0, inputStream.available());
          inputStream.close();
          parameters = MessagePackSerializer.decode(body, Object[].class);
        }

        Object result = router.matchRoute(route, method).remote(parameters, null).get();

        httpExchange.sendResponseHeaders(code, 0);
        OutputStream outputStream = httpExchange.getResponseBody();
        outputStream.write(MessagePackSerializer.encode(result).getKey());
        outputStream.flush();
        outputStream.close();

      } catch (Throwable e) {
        LOGGER.error("HTTP Proxy failed to process request.", e);
        throw new RayServeException("HTTP Proxy failed to process request.", e);
      } finally {
        httpExchange.close();
      }
    }
  }

  private void registerMetrics() {
    if (!Ray.isInitialized() || Ray.getRuntimeContext().isSingleProcess()) {
      return;
    }

    requestCounter =
        Metrics.count()
            .name("serve_num_http_requests")
            .description("The number of HTTP requests processed.")
            .unit("")
            // .tags(ImmutableMap.of("route", ""))
            .register();
    // TODO set route tag dynamically.
  }
}
