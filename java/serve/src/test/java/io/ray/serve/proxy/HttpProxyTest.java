package io.ray.serve.proxy;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.util.CommonUtil;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HttpProxyTest {
  @Test
  public void test() throws IOException {

    try {
      BaseServeTest.initRay();

      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      String endpointName = "HTTPProxyTest";
      String route = "/route";
      Map<String, String> config = new HashMap<>();
      config.put(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false");

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new, "").setName(controllerName).remote();

      Map<String, EndpointInfo> endpointInfos = new HashMap<>();
      endpointInfos.put(
          endpointName,
          EndpointInfo.newBuilder().setEndpointName(endpointName).setRoute(route).build());
      EndpointSet endpointSet = EndpointSet.newBuilder().putAllEndpoints(endpointInfos).build();
      controllerHandle
          .task(DummyServeController::setEndpoints, endpointSet.toByteArray())
          .remote()
          .get();

      Serve.setInternalReplicaContext(null, null, controllerName, null, config);

      // ProxyRouter updates routes.
      ProxyRouter proxyRouter = new ProxyRouter();
      proxyRouter.updateRoutes(endpointInfos);

      // HTTP proxy.
      HttpProxy httpProxy = new HttpProxy();
      httpProxy.init(null, proxyRouter);

      // Send request.
      HttpClient httpClient = HttpClientBuilder.create().build();
      HttpPost httpPost = new HttpPost("http://localhost:" + httpProxy.getPort() + route);
      try (CloseableHttpResponse httpResponse =
          (CloseableHttpResponse) httpClient.execute(httpPost)) {
        // No replica, so error is expected.
        int status = httpResponse.getCode();
        Assert.assertEquals(status, HttpURLConnection.HTTP_INTERNAL_ERROR);
      }

    } finally {
      BaseServeTest.clearAndShutdownRay();
    }
  }
}
