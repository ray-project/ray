package io.ray.serve.docdemo;

import io.ray.serve.api.Serve;
import io.ray.serve.deployment.Application;
import io.ray.serve.generated.DeploymentLanguage;
import io.ray.serve.handle.DeploymentHandle;
import java.io.File;

public class ManagePythonDeployment {

  public static void main(String[] args) {

    System.setProperty(
        "ray.job.code-search-path",
        System.getProperty("java.class.path") + File.pathSeparator + "/path/to/code/");

    Serve.start(null);

    Application deployment =
        Serve.deployment()
            .setLanguage(DeploymentLanguage.PYTHON)
            .setName("counter")
            .setDeploymentDef("counter.Counter")
            .setNumReplicas(1)
            .bind("1");
    DeploymentHandle handle = Serve.run(deployment);

    System.out.println(handle.method("increase").remote("2").result());
  }
}
