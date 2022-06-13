package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.EndpointInfo;
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
    boolean inited = Ray.isInitialized();
    String previous_namespace = System.getProperty("ray.job.namespace");
    System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);

    Ray.init();

    try {
      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      String endpointName = "HTTPProxyTest";
      String route = "/route";

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();

      Map<String, EndpointInfo> endpointInfos = new HashMap<>();
      endpointInfos.put(
          endpointName,
          EndpointInfo.newBuilder().setEndpointName(endpointName).setRoute(route).build());
      controllerHandle.task(DummyServeController::setEndpoints, endpointInfos).remote();

      Serve.setInternalReplicaContext(null, null, controllerName, null);
      Serve.getReplicaContext()
          .setRayServeConfig(
              new RayServeConfig().setConfig(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false"));

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
      if (!inited) {
        Ray.shutdown();
      }
      if (previous_namespace == null) {
        System.clearProperty("ray.job.namespace");
      } else {
        System.setProperty("ray.job.namespace", previous_namespace);
      }
      Serve.setInternalReplicaContext(null);
      Serve.setGlobalClient(null);
    }
  }
}
