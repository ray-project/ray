package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.serve.generated.EndpointInfo;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.HashMap;
import java.util.Map;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.testng.Assert;
import org.testng.annotations.Test;

public class HttpProxyTest {

  @Test
  public void test() throws IOException {

    System.setProperty("ray.run-mode", "SINGLE_PROCESS");
    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String endpointName = "HTTPProxyTest";
      String route = "/route";

      // Controller
      ActorHandle<DummyServeController> controllerHandle =
          Ray.actor(DummyServeController::new).setName(Constants.SERVE_CONTROLLER_NAME).remote();
      Map<String, EndpointInfo> endpointInfos = new HashMap<>();
      endpointInfos.put(
          endpointName,
          EndpointInfo.newBuilder().setEndpointName(endpointName).setRoute(route).build());
      controllerHandle.task(DummyServeController::setEndpoints, endpointInfos).remote();

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

        // No Backend replica, so error is expected.
        int status = httpResponse.getCode();
        Assert.assertEquals(status, HttpURLConnection.HTTP_INTERNAL_ERROR);
      }

    } finally {
      if (!inited) {
        Ray.shutdown();
      }
    }
  }
}
