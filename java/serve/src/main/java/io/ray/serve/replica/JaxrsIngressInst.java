package io.ray.serve.replica;

import io.ray.serve.generated.RequestWrapper;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.concurrent.ExecutionException;
import org.apache.commons.lang3.StringUtils;
import org.glassfish.jersey.internal.MapPropertiesDelegate;
import org.glassfish.jersey.server.ApplicationHandler;
import org.glassfish.jersey.server.ContainerRequest;
import org.glassfish.jersey.server.ContainerResponse;
import org.glassfish.jersey.server.ResourceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** use jersey ApplicationHandler to call a class whitch has jax-rs annotation */
public class JaxrsIngressInst {
  private static final Logger LOGGER = LoggerFactory.getLogger(JaxrsIngressInst.class);
  private ApplicationHandler app;

  public JaxrsIngressInst(Class clazz) {
    ResourceConfig resourceConfig = new ResourceConfig(clazz);
    this.app = new ApplicationHandler(resourceConfig);
  }

  public Object call(RequestWrapper httpProxyRequest)
      throws ExecutionException, InterruptedException {
    ContainerRequest jerseyRequest = convertRequestWrap2ContainerRequest(httpProxyRequest);
    ContainerResponse response = app.apply(jerseyRequest).get();
    Object rspBody = response.getEntity();
    return rspBody;
  }

  /**
   * scope data example: { "type": "http", "asgi": { "version": "3.0", "spec_version": "2.1" },
   * "http_version": "1.1", "server": 8000, "client": 54630, "scheme": "http", "method": "GET",
   * "root_path": "/f", "path": "/calc/41", "raw_path": "/f/calc/41", "query_string": "", "headers":
   * [ "localhost:8000", "python-requests/2.25.1", "gzip, deflate", "keep-alive" ] }
   */
  private ContainerRequest convertRequestWrap2ContainerRequest(RequestWrapper httpProxyRequest) {
    LOGGER.info("scope is {}", httpProxyRequest.getScopeMap());
    ContainerRequest containerRequest =
        new ContainerRequest(
            null,
            StringUtils.isBlank(httpProxyRequest.getScopeMap().get("query_string"))
                ? URI.create(httpProxyRequest.getScopeMap().get("path"))
                : URI.create(
                    httpProxyRequest.getScopeMap().get("path")
                        + "?"
                        + httpProxyRequest.getScopeMap().get("query_string")),
            httpProxyRequest.getScopeMap().get("method"),
            null,
            new MapPropertiesDelegate(),
            null);
    if (!httpProxyRequest.getBody().isEmpty()) {
      containerRequest.setEntityStream(
          new ByteArrayInputStream(httpProxyRequest.getBody().toByteArray()));
    }

    return containerRequest;
  }
}
