package io.ray.serve;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import io.ray.api.Ray;
import io.ray.serve.api.Deployment;
import io.ray.serve.api.Serve;
import io.ray.serve.api.ServeControllerClient;
import io.ray.serve.model.DeploymentInfo;

public class ServeDemo {

  public static class Counter {

    private AtomicInteger count;
≠
    public Counter(Integer value) {
      this.count = new AtomicInteger(value);
    }

    public Integer call(Integer delta) {
      return this.count.addAndGet(delta);
    }
  }

  public static void main(String[] args) throws IOException {
  	
  	Ray.init();
  	
  	// Start serve.
    ServeControllerClient client = Serve.start(true, false, null, null, null);
    
    // Deploy deployment.
    String deploymentName = "counter";

    Deployment deployment =
    		
        Serve.deployment() // TODO return Creator
            .setName(deploymentName)
            .setDeploymentDef(Counter.class.getName())
            .setNumReplicas(2)//.spring(SpringOptions)
            .setInitArgs(new Object[] {10}); // TODO.create();
    
    //SpringOptions;
    
    deployment.deploy(true);

    Deployment result = Serve.getDeployment(deploymentName);
    DeploymentInfo deploymentInfo = client.getDeploymentInfo(deploymentName);

    // Call deployment by handle.
    Assert.assertEquals(16, Ray.get(deployment.getHandle()
    		//.method("f") // TODO
    		.remote(6)));
    Assert.assertEquals(16, Ray.get(deployment.getHandle()
    		//.method("f", "signature") // TODO
    		.remote(6)));
    Assert.assertEquals(26, Ray.get(client.getHandle(deploymentName, false).remote(10)));

    Serve.shutdown();
    client.shutdown();
    Ray.shutdown();
  }
}
