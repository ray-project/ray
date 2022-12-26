package io.ray.serve.replica;

import io.ray.serve.common.Constants;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.util.ExampleEchoDeployment;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

/** DefaultCallable Tester. */
public class DefaultCallableTest {
  /** Method: type() */
  @Test
  public void testType() throws Exception {
    ServeCallableProvider defaultProvider =
        CallableUtils.getProvider(Constants.CALLABLE_PROVIDER_DEFAULT);
    Assert.assertEquals(defaultProvider.type(), Constants.CALLABLE_PROVIDER_DEFAULT);
  }

  /** Method: buildCallable(DeploymentWrapper deploymentWrapper) */
  @Test
  public void testBuildCallable() throws Exception {
    ServeCallableProvider defaultProvider =
        CallableUtils.getProvider(Constants.CALLABLE_PROVIDER_DEFAULT);
    DeploymentWrapper deploymentWrapper = new DeploymentWrapper();
    deploymentWrapper.setDeploymentDef(ExampleEchoDeployment.class.getName());
    deploymentWrapper.setName("echo server");
    deploymentWrapper.setInitArgs(new Object[] {"test_"});
    ExampleEchoDeployment obj =
        (ExampleEchoDeployment) defaultProvider.buildCallable(deploymentWrapper);
    Assert.assertNotNull(obj);
    Assert.assertEquals(obj.call("hello world"), "test_hello world");
  }

  /** Method: getSignatures(Object callable) */
  @Test
  public void testGetSignatures() throws Exception {
    ServeCallableProvider defaultProvider =
        CallableUtils.getProvider(Constants.CALLABLE_PROVIDER_DEFAULT);
    DeploymentWrapper deploymentWrapper = new DeploymentWrapper();
    deploymentWrapper.setDeploymentDef(ExampleEchoDeployment.class.getName());
    deploymentWrapper.setName("echo server");
    deploymentWrapper.setInitArgs(new Object[] {"test_"});
    Object obj = defaultProvider.buildCallable(deploymentWrapper);
    Map<String, Pair<Method, Object>> signatures = defaultProvider.getSignatures(obj);
    Assert.assertTrue(signatures.containsKey("call#(Ljava/lang/Object;)Ljava/lang/String;"));
    Assert.assertTrue(signatures.containsKey("reconfigure#(Ljava/lang/Object;)Ljava/lang/Object;"));
    Assert.assertTrue(signatures.containsKey("checkHealth#()Z"));
  }
}
