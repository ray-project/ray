package io.ray;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;


import com.fasterxml.jackson.databind.JsonNode;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import com.github.fge.jsonschema.main.JsonValidator;

public class PluginSchemaManager {
  private final JsonValidator VALIDATOR = JsonSchemaFactory.byDefault().getValidator();
  private Map<String, JsonNode> schemas = new HashMap<>();
  private PluginSchemaManager(){ }

  private static class holder {
    private static final PluginSchemaManager INSTANCE = new PluginSchemaManager();
  }

  public static PluginSchemaManager getInstance() {
    return holder.INSTANCE;
  }

  public void loadSchemas(String[] schemaPaths) throws IOException {
    for (String path : schemaPaths) {
      JsonNode schema = JsonLoader.fromPath(path);
      JsonNode title = schema.get("title");
      if (title == null) {
        System.err.println("No valid title in " + path);
        continue;
      }
      String name = title.asText();
      if (schemas.containsKey(name)) {
        System.err.println("The schema " + path + "'title' already exists");
        continue;
      }
      schemas.put(name, schema);
    }
  }

    public void validate(String name, JsonNode jsonNode) throws Exception {
      if (!schemas.containsKey(name)) {
        throw new Exception("Invalid plugin name " + name);
      }
      ProcessingReport result = VALIDATOR.validate(schemas.get(name), jsonNode, true);
      if (!result.isSuccess()) {
        throw new Exception("Validate failed: " + result.toString());
      }
  }

}
