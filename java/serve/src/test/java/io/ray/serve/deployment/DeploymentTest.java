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

  @Test
  public void externalScalerEnabledTest() throws IOException {
    // Test deploying with externalScalerEnabled=true, then scaling via the HTTP REST API
    String appName = "externalScalerApp";
    String deploymentName = "exampleEcho";
    Application deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .bind("echo_");

    DeploymentHandle handle = Serve.run(deployment, true, appName, "/", null, true);
    Assert.assertEquals(handle.method("call").remote("5").result(), "echo_5_test");
    Assert.assertTrue((boolean) handle.method("checkHealth").remote().result());

    // Now test scaling using the HTTP REST API endpoint
    // This should succeed because external_scaler_enabled=true
    HttpClient httpClient = HttpClientBuilder.create().build();
    String scaleUrl =
        String.format(
            "http://localhost:8265/api/v1/applications/%s/deployments/%s/scale",
            appName, deploymentName);

    HttpPost scaleRequest = new HttpPost(scaleUrl);
    scaleRequest.setEntity(new StringEntity("{\"target_num_replicas\": 2}"));
    scaleRequest.setHeader("Content-Type", "application/json");

    try (CloseableHttpResponse response =
        (CloseableHttpResponse) httpClient.execute(scaleRequest)) {
      int statusCode = response.getCode();
      String responseBody =
          new String(EntityUtils.toByteArray(response.getEntity()), StandardCharsets.UTF_8);

      // Should succeed with status 200
      Assert.assertEquals(
          statusCode,
          200,
          "Scaling should succeed when external_scaler_enabled=true. Response: " + responseBody);
      Assert.assertTrue(
          responseBody.contains("Scaling request received"),
          "Response should contain success message. Response: " + responseBody);
    }
  }

  @Test
  public void externalScalerDisabledTest() throws IOException {
    // Test deploying with externalScalerEnabled=false, then attempting to scale via HTTP REST API
    // This should fail with status 412 (Precondition Failed) and ExternalScalerNotEnabledError
    String appName = "normalApp";
    String deploymentName = "exampleEcho";
    Application deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .bind("echo_");

    DeploymentHandle handle = Serve.run(deployment, true, appName, "/", null, false);
    Assert.assertEquals(handle.method("call").remote("7").result(), "echo_7_test");
    Assert.assertTrue((boolean) handle.method("checkHealth").remote().result());

    // Now test scaling using the HTTP REST API endpoint
    // This should FAIL because external_scaler_enabled=false
    HttpClient httpClient = HttpClientBuilder.create().build();
    String scaleUrl =
        String.format(
            "http://localhost:8265/api/v1/applications/%s/deployments/%s/scale",
            appName, deploymentName);

    HttpPost scaleRequest = new HttpPost(scaleUrl);
    scaleRequest.setEntity(new StringEntity("{\"target_num_replicas\": 2}"));
    scaleRequest.setHeader("Content-Type", "application/json");

    try (CloseableHttpResponse response =
        (CloseableHttpResponse) httpClient.execute(scaleRequest)) {
      int statusCode = response.getCode();
      String responseBody =
          new String(EntityUtils.toByteArray(response.getEntity()), StandardCharsets.UTF_8);

      // Should fail with status 412 (Precondition Failed)
      Assert.assertEquals(
          statusCode,
          412,
          "Scaling should fail with 412 when external_scaler_enabled=false. Response: "
              + responseBody);
      Assert.assertTrue(
          responseBody.contains("external_scaler_enabled")
              || responseBody.contains("ExternalScalerNotEnabledError"),
          "Response should contain external_scaler_enabled error message. Response: "
              + responseBody);
    }
  }
}
