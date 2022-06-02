package io.ray;

import java.util.Arrays;
import static org.junit.Assert.assertTrue;
import org.junit.Test;

public class RuntimeEnvTest
{
    @Test
    public void testRuntimeEnv()
    {
      RuntimeEnv runtimeEnv = new RuntimeEnv();
      // Add pip runtime env
      Pip pip = new Pip();
      String[] packages = new String[] {"requests"};
      pip.packages = packages;
      pip.pip_check = true;
      runtimeEnv.set("pip", pip);
      // Add working dir runtime env
      String workingDir = "https://path/to/working_dir.zip";
      runtimeEnv.set("working_dir", workingDir);
      // Serialize
      String serializedRuntimeEnv = runtimeEnv.serialize();
      System.out.println("serializedRuntimeEnv " + serializedRuntimeEnv);

      // Deserialize
      RuntimeEnv runtimeEnv2 = RuntimeEnv.deserialize(serializedRuntimeEnv);
      Pip pip2 = runtimeEnv2.get("pip", Pip.class);
      assertTrue(Arrays.equals(pip2.packages, packages));
      String workingDir2 = runtimeEnv2.get("working_dir", String.class);
      assertTrue(workingDir2.equals(workingDir));
    }
}
