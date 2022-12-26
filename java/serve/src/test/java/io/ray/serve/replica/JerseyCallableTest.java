package io.ray.serve.replica;

import io.ray.serve.common.Constants;
import io.ray.serve.deployment.DeploymentWrapper;
import io.ray.serve.util.ExampleEchoDeployment;
import java.lang.reflect.Method;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JerseyCallableTest {
  /** Method: type() */
  @Test
  public void testType() throws Exception {
    ServeCallableProvider jerseyProvider =
        CallableUtils.getProvider(Constants.CALLABLE_PROVIDER_JERSEY);
    Assert.assertEquals(jerseyProvider.type(), Constants.CALLABLE_PROVIDER_JERSEY);
  }

  /** Method: getSignatures(Object callable) */
  @Test
  public void testGetSignatures() throws Exception {
    ServeCallableProvider jerseyProvider =
        CallableUtils.getProvider(Constants.CALLABLE_PROVIDER_JERSEY);
    DeploymentWrapper deploymentWrapper = new DeploymentWrapper();
    deploymentWrapper.setDeploymentDef(ExampleEchoDeployment.class.getName());
    deploymentWrapper.setName("echo server");
    deploymentWrapper.setInitArgs(new Object[] {"test_"});
    Object obj = jerseyProvider.buildCallable(deploymentWrapper);
    Map<String, Pair<Method, Object>> signatures = jerseyProvider.getSignatures(obj);
    Assert.assertTrue(signatures.containsKey("call#(Ljava/lang/Object;)Ljava/lang/String;"));
    Assert.assertTrue(signatures.containsKey("reconfigure#(Ljava/lang/Object;)Ljava/lang/Object;"));
    Assert.assertTrue(signatures.containsKey("checkHealth#()Z"));
    Assert.assertTrue(signatures.containsKey(Constants.HTTP_PROXY_SIGNATURE));
  }
}
