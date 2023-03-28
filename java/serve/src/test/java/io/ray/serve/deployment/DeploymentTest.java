package io.ray.serve.deployment;

import io.ray.api.Ray;
import io.ray.serve.BaseServeTest;
import io.ray.serve.api.Serve;
import io.ray.serve.config.AutoscalingConfig;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import org.apache.hc.client5.http.classic.HttpClient;
import org.apache.hc.client5.http.classic.methods.HttpGet;
import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.StringEntity;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class DeploymentTest extends BaseServeTest {

  @Test
  public void deployTest() {
    // Deploy deployment.
    String deploymentName = "exampleEcho";

    Deployment deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .setInitArgs(new Object[] {"echo_"})
            .create();

    deployment.deploy(true);
    Assert.assertEquals(Ray.get(deployment.getHandle().method("call").remote("6")), "echo_6_test");
    Assert.assertTrue((boolean) Ray.get(deployment.getHandle().method("checkHealth").remote()));
  }

  @Test
  public void httpExposeDeploymentTest() throws IOException {
    // Deploy deployment.
    String deploymentName = "exampleEcho";

    Deployment deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .setInitArgs(new Object[] {"echo_"})
            .create();
    deployment.deploy(true);
    HttpClient httpClient = HttpClientBuilder.create().build();
    HttpGet httpGet = new HttpGet("http://127.0.0.1:8341/" + deploymentName + "?input=testhttpget");
    try (CloseableHttpResponse httpResponse = (CloseableHttpResponse) httpClient.execute(httpGet)) {
      byte[] body = EntityUtils.toByteArray(httpResponse.getEntity());
      String response = new String(body, StandardCharsets.UTF_8);
      Assert.assertEquals(response, "echo_testhttpget_test");
    }
    HttpPost httpPost = new HttpPost("http://127.0.0.1:8341/" + deploymentName);
    httpPost.setEntity(new StringEntity("testhttppost"));
    try (CloseableHttpResponse httpResponse =
        (CloseableHttpResponse) httpClient.execute(httpPost)) {
      byte[] body = EntityUtils.toByteArray(httpResponse.getEntity());
      String response = new String(body, StandardCharsets.UTF_8);
      Assert.assertEquals(response, "echo_testhttppost_test");
    }
  }

  @Test
  public void updateDeploymentTest() {
    String deploymentName = "exampleEcho";

    Deployment deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .setInitArgs(new Object[] {"echo_"})
            .create();
    deployment.deploy(true);
    Deployment deployed = Serve.getDeployment(deploymentName);
    deployed.options().setNumReplicas(2).create().deploy(true);
    DeploymentRoute deploymentInfo = client.getDeploymentInfo(deploymentName);
    Assert.assertEquals(
        deploymentInfo.getDeploymentInfo().getDeploymentConfig().getNumReplicas().intValue(), 2);
  }

  @Test
  public void autoScaleTest() {
    String deploymentName = "exampleEcho";
    AutoscalingConfig autoscalingConfig = new AutoscalingConfig();
    autoscalingConfig.setMinReplicas(2);
    autoscalingConfig.setMaxReplicas(5);
    autoscalingConfig.setTargetNumOngoingRequestsPerReplica(10);
    Deployment deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setAutoscalingConfig(autoscalingConfig)
            .setUserConfig("_test")
            .setVersion("v1")
            .setInitArgs(new Object[] {"echo_"})
            .create();
    deployment.deploy(true);
    Assert.assertEquals(Ray.get(deployment.getHandle().method("call").remote("6")), "echo_6_test");
  }

  @Test
  public void userConfigTest() {
    String deploymentName = "exampleEcho";
    Deployment deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .setInitArgs(new Object[] {"echo_"})
            .create();
    deployment.deploy(true);
    deployment.options().setUserConfig("_new").create().deploy(true);
    Assert.assertEquals(Ray.get(deployment.getHandle().method("call").remote("6")), "echo_6_new");
  }
}
