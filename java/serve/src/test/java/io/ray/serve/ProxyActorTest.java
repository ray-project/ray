package io.ray.serve;

import com.google.common.collect.ImmutableMap;
import io.ray.api.ActorHandle;
import io.ray.api.Ray;
import io.ray.runtime.serializer.MessagePackSerializer;
import io.ray.serve.api.Serve;
import io.ray.serve.generated.ActorSet;
import io.ray.serve.generated.DeploymentLanguage;
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
    String previous_namespace = System.getProperty("ray.job.namespace");
    System.setProperty("ray.job.namespace", Constants.SERVE_NAMESPACE);
    Ray.init();

    try {
      String prefix = "ProxyActorTest";
      String controllerName =
          CommonUtil.formatActorName(
              Constants.SERVE_CONTROLLER_NAME, RandomStringUtils.randomAlphabetic(6));
      String deploymentName = prefix;
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
      DeploymentInfo deploymentInfo =
          new DeploymentInfo()
              .setName(deploymentName)
              .setDeploymentConfig(
                  new DeploymentConfig().setDeploymentLanguage(DeploymentLanguage.JAVA.getNumber()))
              .setDeploymentVersion(new DeploymentVersion(version))
              .setDeploymentDef(DummyReplica.class.getName());

      ActorHandle<RayServeWrappedReplica> replica =
          Ray.actor(
                  RayServeWrappedReplica::new,
                  deploymentInfo,
                  replicaTag,
                  controllerName,
                  new RayServeConfig().setConfig(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false"))
              .setName(replicaTag)
              .remote();
      Assert.assertTrue(replica.task(RayServeWrappedReplica::checkHealth).remote().get());

      // ProxyActor
      ProxyActor proxyActor =
          new ProxyActor(
              controllerName, ImmutableMap.of(RayServeConfig.LONG_POOL_CLIENT_ENABLED, "false"));
      Assert.assertTrue(proxyActor.ready());

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
