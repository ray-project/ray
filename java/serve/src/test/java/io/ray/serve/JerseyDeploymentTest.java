package io.ray.serve;

import io.ray.api.Ray;
import io.ray.serve.api.Serve;
import io.ray.serve.common.Constants;
import io.ray.serve.deployment.Deployment;
import io.ray.serve.util.ExampleJaxrsDeployment;
import java.io.IOException;
import org.apache.hc.client5.http.fluent.Request;
import org.apache.hc.core5.http.ContentType;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class JerseyDeploymentTest extends BaseServeTest {

  @Test
  public void createDepolymentTest() throws IOException {
    // Deploy deployment.
    String deploymentName = "jerseyTest";

    Deployment deployment =
        Serve.deployment()
            .setName(deploymentName)
            .setDeploymentDef(ExampleJaxrsDeployment.class.getName())
            .setNumReplicas(1)
            .setIngress(Constants.CALLABLE_PROVIDER_JERSEY)
            .create();
    deployment.deploy(true);
    String result =
        Request.get("http://127.0.0.1:8341/jerseyTest/example/helloWorld?name=ray")
            .execute()
            .returnContent()
            .asString();
    Assert.assertEquals(result, "hello world, ray");
    result =
        Request.post("http://127.0.0.1:8341/jerseyTest/example/helloWorld2")
            .bodyString("rayByPost", ContentType.APPLICATION_JSON)
            .execute()
            .returnContent()
            .asString();
    Assert.assertEquals(result, "hello world, i am from body:rayByPost");
    result =
        Request.get("http://127.0.0.1:8341/jerseyTest/example/paramPathTest/ray")
            .execute()
            .returnContent()
            .asString();
    Assert.assertEquals(result, "paramPathTest, ray");
    Assert.assertEquals(
        Ray.get(deployment.getHandle().method("helloWorld").remote("ray")), "hello world, ray");
  }
}
