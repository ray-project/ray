package org.ray.util.generator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.ray.util.FileUtil;

/**
 * A util class that generates `RayCall.java`
 */
public class RayCallGenerator extends BaseGenerator {

  /**
   * @return Whole file content of `RayCall.java`.
   */
  private String build() {
    sb = new StringBuilder();

    newLine("// generated automatically, do not modify.");
    newLine("");
    newLine("package org.ray.api;");
    newLine("");
    newLine("import org.ray.api.function.*;");
    newLine("");

    newLine("/**");
    newLine(" * This class provides type-safe interfaces for Ray.call.");
    newLine(" **/");
    newLine("@SuppressWarnings({\"rawtypes\", \"unchecked\"})");
    newLine("class RayCall {");
    for (int i = 0; i <= 6; i++) {
      if (i > 0) {
        buildCalls(i, true);
      }
      buildCalls(i, false);
    }
    newLine("}");
    return sb.toString();
  }

  /**
   * Build the `Ray.call` methods for given number of parameters.
   * @param numParameters the number of parameters, including the actor parameter.
   * @param forActor build actor api when true, otherwise build task api.
   */
  private void buildCalls(int numParameters, boolean forActor) {
    String genericTypes = "";
    String argList = "";
    for (int i = 0; i < numParameters; i++) {
      genericTypes += "T" + i + ", ";
      if (!forActor || i > 0) {
        argList += "t" + i + ", ";
      }
    }
    if (argList.endsWith(", ")) {
      argList = argList.substring(0, argList.length() - 2);
    }

    String funcParam = String.format("RayFunc%d<%sR> f%s",
        numParameters,
        genericTypes,
        numParameters > 0 ? ", " : "");
    String actorParam = "";
    if (forActor) {
      actorParam = "RayActor<T0> actor";
      if (numParameters > 1) {
        actorParam += ", ";
      }
    }

    for (String param : generateParameters(forActor ? 1 : 0, numParameters)) {
      // method signature
      indents(1);
      newLine(String.format(
          "public static <%sR> RayObject<R> call(%s%s%s) {",
          genericTypes, funcParam, actorParam, param
      ));
      // method body
      indents(2);
      newLine(String.format("Object[] args = new Object[]{%s};", argList));
      indents(2);
      newLine(String.format("return Ray.internal().call(f%s, args);", forActor ? ", actor" : ""));
      indents(1);
      newLine("}");
    }
  }

  private List<String> generateParameters(int from, int to) {
    List<String> res = new ArrayList<>();
    dfs(from, from, to, "", res);
    return res;
  }

  private void dfs(int pos, int from, int to, String cur, List<String> res) {
    if (pos >= to) {
      res.add(cur);
      return;
    }
    if (pos > from) {
      cur += ", ";
    }
    String nextParameter = String.format("T%d t%d", pos, pos);
    dfs(pos + 1, from, to, cur + nextParameter, res);
    nextParameter = String.format("RayObject<T%d> t%d", pos, pos);
    dfs(pos + 1, from, to, cur + nextParameter, res);
  }

  public static void main(String[] args) throws IOException {
    String path = System.getProperty("user.dir")
        + "/api/src/main/java/org/ray/api/RayCall.java";
    FileUtil.overrideFile(path, new RayCallGenerator().build());
  }
}
