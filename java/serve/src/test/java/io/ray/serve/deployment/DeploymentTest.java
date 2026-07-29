package io.ray.serve.deployment;

import io.ray.api.Ray;
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
  public void externalScalerEnabledTest() {
    /*
     * This test verifies that the external_scaler_enabled flag is properly passed through
     * the Java Serve API to the Python controller.
     *
     * WHY WE DON'T TEST VIA HTTP DASHBOARD API:
     * The external scaler HTTP REST API endpoint (/api/v1/applications/{app}/deployments/{dep}/scale)
     * is hosted by the Ray dashboard on port 8265. However, in the Java test framework:
     *
     * 1. Each test creates an ephemeral Ray session with a fresh actor registry
     * 2. The Serve controller is created programmatically via Java API (Serve.run)
     * 3. The dashboard runs in a separate process and discovers controllers by querying
     *    Ray's actor registry for "SERVE_CONTROLLER_ACTOR" in the "serve" namespace
     * 4. Due to timing and process isolation issues in test environments, the dashboard's
     *    get_serve_controller() method often fails to find the controller, returning 503
     *    with error "Serve controller is not available"
     *
     * For testing purposes, we verify the flag is correctly passed to the controller by:
     * 1. Deploying an application with external_scaler_enabled=true
     * 2. Verifying the deployment succeeds and is functional
     * 3. Checking that the controller actor exists with the correct configuration
     *
     * The actual HTTP scaling functionality is tested in Python integration tests where
     * the dashboard and controller have proper lifecycle management.
     */
    String appName = "externalScalerApp";
    String deploymentName = "exampleEcho";
    Application deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .bind("echo_");

    // Deploy with external_scaler_enabled=true - this passes the flag through
    DeploymentHandle handle = Serve.run(deployment, true, appName, "/", null, true);

    // Verify the deployment is functional
    Assert.assertEquals(handle.method("call").remote("5").result(), "echo_5_test");
    Assert.assertTrue((boolean) handle.method("checkHealth").remote().result());

    // Verify the controller actor exists in the correct namespace
    java.util.Optional<io.ray.api.BaseActorHandle> controllerOpt =
        Ray.getActor("SERVE_CONTROLLER_ACTOR", "serve");
    Assert.assertTrue(
        controllerOpt.isPresent(), "Serve controller actor should exist in 'serve' namespace");

    // Verify that the external_scaler_enabled flag is actually set to TRUE in the controller
    // by calling the controller's get_external_scaler_enabled method
    io.ray.api.PyActorHandle controller = (io.ray.api.PyActorHandle) controllerOpt.get();
    try {
      // Call the Python controller's get_external_scaler_enabled method
      // This is a helper method added specifically for Java tests that returns a simple boolean
      Object result =
          controller
              .task(io.ray.api.function.PyActorMethod.of("get_external_scaler_enabled"), appName)
              .remote()
              .get();

      // Verify the flag is set to True
      Assert.assertTrue(
          Boolean.TRUE.equals(result),
          "external_scaler_enabled should be True for app '" + appName + "', but was: " + result);

      // Also verify application is running
      io.ray.serve.api.ServeControllerClient client = io.ray.serve.api.Serve.getGlobalClient();
      io.ray.serve.generated.StatusOverview status = client.getServeStatus(appName);
      Assert.assertEquals(
          status.getAppStatus().getStatus(),
          io.ray.serve.generated.ApplicationStatus.APPLICATION_STATUS_RUNNING,
          "Application should be in RUNNING status");
    } catch (Exception e) {
      throw new RuntimeException("Failed to verify external_scaler_enabled flag", e);
    }
  }

  @Test
  public void externalScalerDisabledTest() {
    /*
     * This test verifies that the external_scaler_enabled flag defaults to false and
     * applications can be deployed with external scaling explicitly disabled.
     *
     * This is the complement to externalScalerEnabledTest - verifying that the flag
     * can be set to false (which is also the default behavior).
     *
     * In production, when external_scaler_enabled=false, attempts to scale via the
     * HTTP dashboard API would return 412 (Precondition Failed) with an
     * ExternalScalerDisabledError. However, as explained in externalScalerEnabledTest,
     * we cannot reliably test the HTTP API in this test environment due to dashboard
     * and controller lifecycle management issues.
     *
     * For testing purposes, we verify:
     * 1. Applications can be deployed with external_scaler_enabled=false
     * 2. The deployment succeeds and is functional
     * 3. Checking that the controller actor exists with the correct configuration
     */
    String appName = "normalApp";
    String deploymentName = "exampleEcho";
    Application deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleEchoDeployment.class.getName())
            .setNumReplicas(1)
            .setUserConfig("_test")
            .bind("echo_");

    // Deploy with external_scaler_enabled=false (explicit)
    DeploymentHandle handle = Serve.run(deployment, true, appName, "/", null, false);

    // Verify the deployment is functional
    Assert.assertEquals(handle.method("call").remote("7").result(), "echo_7_test");
    Assert.assertTrue((boolean) handle.method("checkHealth").remote().result());

    // Verify the controller actor exists in the correct namespace
    java.util.Optional<io.ray.api.BaseActorHandle> controllerOpt =
        Ray.getActor("SERVE_CONTROLLER_ACTOR", "serve");
    Assert.assertTrue(
        controllerOpt.isPresent(), "Serve controller actor should exist in 'serve' namespace");

    // Verify that the external_scaler_enabled flag is actually set to FALSE in the controller
    // by calling the controller's get_external_scaler_enabled method
    io.ray.api.PyActorHandle controller = (io.ray.api.PyActorHandle) controllerOpt.get();
    try {
      // Call the Python controller's get_external_scaler_enabled method
      // This is a helper method added specifically for Java tests that returns a simple boolean
      Object result =
          controller
              .task(io.ray.api.function.PyActorMethod.of("get_external_scaler_enabled"), appName)
              .remote()
              .get();

      // Verify the flag is set to False
      Assert.assertFalse(
          Boolean.TRUE.equals(result),
          "external_scaler_enabled should be False for app '" + appName + "', but was: " + result);

      // Also verify application is running
      io.ray.serve.api.ServeControllerClient client = io.ray.serve.api.Serve.getGlobalClient();
      io.ray.serve.generated.StatusOverview status = client.getServeStatus(appName);
      Assert.assertEquals(
          status.getAppStatus().getStatus(),
          io.ray.serve.generated.ApplicationStatus.APPLICATION_STATUS_RUNNING,
          "Application should be in RUNNING status");
    } catch (Exception e) {
      throw new RuntimeException("Failed to verify external_scaler_enabled flag", e);
    }
  }
}
