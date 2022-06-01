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
      PipRuntimeEnv pipRuntimeEnv = new PipRuntimeEnv();
      String[] packages = new String[] {"requests"};
      pipRuntimeEnv.packages = packages;
      pipRuntimeEnv.pip_check = true;
      runtimeEnv.set("pip", pipRuntimeEnv);
      // Serialize
      String serializedRuntimeEnv = runtimeEnv.serialize();

      // Deserialize
      RuntimeEnv runtimeEnv2 = RuntimeEnv.deserialize(serializedRuntimeEnv);
      PipRuntimeEnv pipRuntimeEnv2 = runtimeEnv2.get("pip", PipRuntimeEnv.class);
      assertTrue(Arrays.equals(pipRuntimeEnv2.packages, packages));
      assertTrue(pipRuntimeEnv2.pip_check);
    }
}
