package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import io.ray.runtime.metric.Count;
import io.ray.runtime.metric.Metrics;
import io.ray.runtime.metric.TagKey;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.util.LogUtil;
import io.ray.serve.util.SocketUtil;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.impl.bootstrap.HttpServer;
import org.apache.hc.core5.http.impl.bootstrap.ServerBootstrap;
import org.apache.hc.core5.http.io.HttpRequestHandler;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpProxy implements ServeProxy {

  private static final Logger LOGGER = LoggerFactory.getLogger(HttpProxy.class);

  public static final String PROXY_NAME = "HTTP_PROXY";

  public static final String PROXY_HTTP_PORT = "ray.serve.proxy.http.port";

  public static final String PROXY_HTTP_METHODS = "ray.serve.proxy.http.methods";

  private int port;

  private Count requestCounter;

  private HttpServer httpServer;

  private ProxyRouter proxyRouter;

  @Override
  public void init(Map<String, String> config, ProxyRouter proxyRouter) {
    this.port =
        Optional.ofNullable(config)
            .map(conf -> conf.get(PROXY_HTTP_PORT))
            .map(httpPort -> Integer.valueOf(httpPort))
            .orElse(SocketUtil.findAvailableTcpPort(8000));
    this.proxyRouter = proxyRouter;
    RayServeMetrics.execute(
        () ->
            this.requestCounter =
                Metrics.count()
                    .name("serve_num_http_requests")
                    .description("The number of HTTP requests processed.")
                    .unit("")
                    .tags(new HashMap<>())
                    .register());
    startupHttpServer(port);
    LOGGER.info("Proxy {} has been started with port:{}", getName(), this.port);
  }

  private void startupHttpServer(int port) {
    try {
      this.httpServer =
          ServerBootstrap.bootstrap()
              .setListenerPort(port)
              .register("*", new ServeHttpHandler())
              .registerVirtual(
                  InetAddress.getLocalHost().getHostAddress(), "*", new ServeHttpHandler())
              .create();
      this.httpServer.start();
    } catch (Throwable e) {
      String errMsg =
          LogUtil.format(
              "Proxy {} failed to startup HTTP server on port {}.", getName(), this.port);
      LOGGER.error(errMsg);
      throw new RayServeException(errMsg, e);
    }
  }

  @Override
  public String getName() {
    return PROXY_NAME;
  }

  private class ServeHttpHandler implements HttpRequestHandler {

    @Override
    public void handle(
        ClassicHttpRequest request, ClassicHttpResponse response, HttpContext context)
        throws HttpException, IOException {

      int code = HttpURLConnection.HTTP_OK;
      Object result = null;
      String route = request.getPath();
      try {
        RayServeMetrics.execute(
            () ->
                requestCounter.update(
                    1.0,
                    ImmutableMap.of(
                        new TagKey(RayServeMetrics.TAG_ROUTE),
                        route))); // TODO the old tag will be covered, it may be a bug.

        Object[] parameters = null;
        HttpEntity httpEntity = request.getEntity();
        if (null == httpEntity) {
          parameters = new Object[0];
        } else {
          byte[] body = EntityUtils.toByteArray(httpEntity);
          parameters = MessagePackSerializer.decode(body, Object[].class);
        }

        RayServeHandle rayServeHandle = proxyRouter.matchRoute(route);
        if (rayServeHandle == null) {
          code = HttpURLConnection.HTTP_NOT_FOUND;
        } else {
          result = rayServeHandle.remote(parameters).get();
        }

      } catch (Throwable e) {
        LOGGER.error("HTTP Proxy failed to process request.", e);
        code = HttpURLConnection.HTTP_INTERNAL_ERROR;
      } finally {
        response.setCode(code);
        if (code == HttpURLConnection.HTTP_NOT_FOUND) {
          response.setEntity(
              new StringEntity(
                  LogUtil.format(
                      "Path '{}' not found. Please ping http://.../-/routes for route table.",
                      route),
                  Charset.forName(Constants.UTF8)));
        } else if (result != null) {
          response.setEntity(
              new ByteArrayEntity(MessagePackSerializer.encode(result).getLeft(), null));
        }
      }
    }
  }

  public int getPort() {
    return port;
  }

  public ProxyRouter getProxyRouter() {
    return proxyRouter;
  }
}
