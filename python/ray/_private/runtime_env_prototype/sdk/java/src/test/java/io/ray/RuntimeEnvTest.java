package io.ray;

import java.util.Arrays;
import static org.junit.Assert.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import org.junit.Test;

public class RuntimeEnvTest
{
    @Test
    public void testRuntimeEnv() throws Exception {
      // Load plugin schemas
      final String currentDir = System.getProperty("user.dir");
      String[] schema_paths={currentDir + "/../../pip/pip_schema.json",
        currentDir + "/../../working_dir/working_dir_schema.json"};
      PluginSchemaManager.getInstance().loadSchemas(schema_paths);
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

      // Construct runtime env with raw json
      RuntimeEnv runtimeEnv3 = new RuntimeEnv();
      String pipString = "    "
        + "{\n"
        + "      \"packages\": [\"requests\", \"tensorflow\"],\n"
        + "      \"pip_check\": false\n"
        + "}";
      JsonNode pipRawJson = JsonLoader.fromString(pipString);
      runtimeEnv3.set("pip", pipRawJson);
      System.out.println("serializedRuntimeEnv 3: " + runtimeEnv3.serialize());

      // Construct runtime env with raw json string
      RuntimeEnv runtimeEnv4 = new RuntimeEnv();
      runtimeEnv4.set("pip", pipString);
      System.out.println("serializedRuntimeEnv 4: " + runtimeEnv4.serialize());
    }
}
