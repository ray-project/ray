package io.ray.serve.proxy;

import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.BaseServeTest;
import io.ray.serve.DummyServeController;
import io.ray.serve.common.Constants;
import io.ray.serve.config.DeploymentConfig;
import io.ray.serve.config.RayServeConfig;
import io.ray.serve.deployment.DeploymentVersion;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.generated.ActorNameList;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.generated.EndpointInfo;
import io.ray.serve.generated.EndpointSet;
import io.ray.serve.replica.DummyReplica;
import io.ray.serve.replica.RayServeWrappedReplica;
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

    try {
      BaseServeTest.initRay();

      String prefix = "ProxyActorTest";
      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      String deploymentName = prefix;
      String replicaTag = prefix;
      String endpointName = prefix;
      String route = "/route";
      String version = "v1";
      Map<String, String> config = new HashMap<>();
      config.put(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false");

      // Controller
      ActorHandle<DummyServeController> controller =
          Ray.actor(DummyServeController::new, "").setName(controllerName).remote();
      Map<String, EndpointInfo> endpointInfos = new HashMap<>();
      endpointInfos.put(
          endpointName,
          EndpointInfo.newBuilder().setEndpointName(endpointName).setRoute(route).build());
      EndpointSet endpointSet = EndpointSet.newBuilder().putAllEndpoints(endpointInfos).build();
      controller.task(DummyServeController::setEndpoints, endpointSet.toByteArray()).remote().get();

      // Replica
      DeploymentWrapper deploymentWrapper =
          new DeploymentWrapper()
              .setName(deploymentName)
              .setDeploymentConfig(
                  new DeploymentConfig().setDeploymentLanguage(DeploymentLanguage.JAVA))
              .setDeploymentVersion(new DeploymentVersion(version))
              .setDeploymentDef(DummyReplica.class.getName())
              .setConfig(config);

      ActorHandle<RayServeWrappedReplica> replica =
          Ray.actor(RayServeWrappedReplica::new, deploymentWrapper, replicaTag, controllerName)
              .setName(replicaTag)
              .remote();
      Assert.assertTrue(replica.task(RayServeWrappedReplica::checkHealth).remote().get());

      // ProxyActor
      ProxyActor proxyActor = new ProxyActor(controllerName, config);
      Assert.assertTrue(proxyActor.ready());

      proxyActor.getProxyRouter().updateRoutes(endpointInfos);
      proxyActor
          .getProxyRouter()
          .getHandles()
          .get(endpointName)
          .getRouter()
          .getReplicaSet()
          .updateWorkerReplicas(ActorNameList.newBuilder().addNames(replicaTag).build());

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
      BaseServeTest.clearAndShutdownRay();
    }
  }
}
