package org.ray.util.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.ray.util.FileUtil;

/**
 * A util class that generates Rpc.java
 */
public class RpcGenerator extends BaseGenerator {

  private String build() {
    sb = new StringBuilder();

    newLine("// generated automatically, do not modify.");
    newLine("");
    newLine("package org.ray.api;");
    newLine("");
    newLine("import org.ray.api.funcs.*;");
    newLine("");

    newLine("@SuppressWarnings({\"rawtypes\", \"unchecked\"})");
    newLine("class Rpc {");
    for (int i = 0; i <= 6; i++) {
      buildCalls(i);
    }
    newLine("}");
    return sb.toString();
  }

  private void buildCalls(int numParameters) {
    String funcClass = "RayFunc" + numParameters;
    String genericTypes = "";
    String callList = "";
    for (int i = 1; i <= numParameters; i++) {
      genericTypes += "T" + i + ", ";
      callList += ", t" + i;
    }
    String body = String.format(
        "return Ray.internal().call(null, %s.class, f, 1%s).objs[0];",
        funcClass, callList
    );
    for (String param : generateParameters(numParameters)) {
      indents(1);
      newLine(String.format(
          "public static <%sR> RayObject<R> call(%s<%sR> f%s) {",
          genericTypes, funcClass, genericTypes, param
      ));
      indents(2);
      newLine(body);
      indents(1);
      newLine("}");
    }
  }

  private List<String> generateParameters(int numParameters) {
    List<String> res = new ArrayList<>();
    dfs(1, numParameters, "", res);
    return res;
  }

  private void dfs(int pos, int max, String cur, List<String> res) {
    if (pos > max) {
      res.add(cur);
      return;
    }
    cur += ", ";
    String nextParameter = String.format("T%d t%d", pos, pos);
    dfs(pos + 1, max, cur + nextParameter, res);
    nextParameter = String.format("RayObject<T%d> t%d", pos, pos);
    dfs(pos + 1, max, cur + nextParameter, res);
  }

  public static void main(String[] args) throws IOException {
    String path = System.getProperty("user.dir") + "/api/src/main/java";
    path += "/org/ray/api/Rpc.java";
    FileUtil.overrideFile(path, new RpcGenerator().build());
  }
}
