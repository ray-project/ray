package io.ray.serve.deployment;

import io.ray.serve.BaseServeTest2;
import io.ray.serve.api.Serve;
import io.ray.serve.config.AutoscalingConfig;
import io.ray.serve.handle.DeploymentHandle;
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
public class DeploymentTest extends BaseServeTest2 {

  @Test
  public void deployTest() {
    // Deploy deployment.
    String deploymentName = "exampleEcho";

    Application deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .bind("echo_");
    DeploymentHandle handle = Serve.run(deployment);
    Assert.assertEquals(handle.method("call").remote("6").result(), "echo_6_test");
    Assert.assertTrue((boolean) handle.method("checkHealth").remote().result());
  }

  @Test
  public void httpExposeDeploymentTest() throws IOException {
    // Deploy deployment.
    String deploymentName = "exampleEcho";

    Application deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .bind("echo_");
    Serve.run(deployment);

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

  @Test(enabled = false)
  public void updateDeploymentTest() {
    String deploymentName = "exampleEcho";

    Application deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .bind("echo_");
    Serve.run(deployment);

    Deployment deployed = Serve.getDeployment(deploymentName);
    Serve.run(deployed.options().setNumReplicas(2).bind("echo_"));
  }

  @Test
  public void autoScaleTest() {
    String deploymentName = "exampleEcho";
    AutoscalingConfig autoscalingConfig = new AutoscalingConfig();
    autoscalingConfig.setMinReplicas(1);
    autoscalingConfig.setMaxReplicas(3);
    autoscalingConfig.setTargetOngoingRequests(10);
    Application deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setAutoscalingConfig(autoscalingConfig)
            .setUserConfig("_test")
            .setVersion("v1")
            .bind("echo_");

    DeploymentHandle handle = Serve.run(deployment);
    Assert.assertEquals(handle.method("call").remote("6").result(), "echo_6_test");
  }

  @Test(enabled = false)
  public void userConfigTest() {
    String deploymentName = "exampleEcho";
    Application deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .bind("echo_");
    Serve.run(deployment);

    Serve.run(Serve.getDeployment(deploymentName).options().setUserConfig("_new").bind());
    Assert.assertEquals(
        Serve.getAppHandle(deploymentName).method("call").remote("6").result(), "echo_6_new");
    // TOOD update user config
  }
}
