package io.ray.serve;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.BackendConfig;
import io.ray.serve.generated.BackendVersion;
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
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ProxyActorTest {

  @Test
  public void test() throws IOException {
    boolean inited = Ray.isInitialized();
    Ray.init();

    try {
      String prefix = "ProxyActorTest";
      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      String backendTag = prefix;
      String replicaTag = prefix;
      String endpointName = prefix;
      String route = "/route";
      String version = "v1";

      // Controller
      ActorHandle<DummyServeController> controller =
          Ray.actor(DummyServeController::new).setName(controllerName).remote();
      Map<String, EndpointInfo> endpointInfos = new HashMap<>();
      endpointInfos.put(
          endpointName,
          EndpointInfo.newBuilder().setEndpointName(endpointName).setRoute(route).build());
      controller.task(DummyServeController::setEndpoints, endpointInfos).remote();

      // Replica
      DeploymentInfo deploymentInfo = new DeploymentInfo();
      deploymentInfo.setBackendConfig(BackendConfig.newBuilder().build().toByteArray());
      deploymentInfo.setBackendVersion(
          BackendVersion.newBuilder().setCodeVersion(version).build().toByteArray());
      deploymentInfo.setReplicaConfig(
          new ReplicaConfig(DummyBackendReplica.class.getName(), null, new HashMap<>()));

      ActorHandle<RayServeWrappedReplica> replica =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  backendTag,
                  replicaTag,
                  deploymentInfo,
                  controllerName)
              .setName(replicaTag)
              .remote();
      replica.task(RayServeWrappedReplica::ready).remote();

      // ProxyActor
      ProxyActor proxyActor = new ProxyActor(controllerName, null);
      proxyActor.getProxyRouter().updateRoutes(endpointInfos);
      proxyActor
          .getProxyRouter()
          .getHandles()
          .get(endpointName)
          .getRouter()
          .getReplicaSet()
          .updateWorkerReplicas(ActorSet.newBuilder().addNames(replicaTag).build());

      // Send request.
      HttpClient httpClient = HttpClientBuilder.create().build();
      HttpPost httpPost =
          new HttpPost(
              "http://localhost:"
                  + ((HttpProxy) proxyActor.getProxies().get(HttpProxy.PROXY_NAME)).getPort()
                  + route);
      try (CloseableHttpResponse httpResponse =
          (CloseableHttpResponse) httpClient.execute(httpPost)) {

        int status = httpResponse.getCode();
        Assert.assertEquals(status, HttpURLConnection.HTTP_OK);
        Object result =
            MessagePackSerializer.decode(
                EntityUtils.toByteArray(httpResponse.getEntity()), Object.class);

        Assert.assertNotNull(result);
        Assert.assertEquals("1", result.toString());
      }

    } finally {
      if (!inited) {
        Ray.shutdown();
      }
      Serve.setInternalReplicaContext(null);
      Serve.setGlobalClient(null);
    }
  }
}
