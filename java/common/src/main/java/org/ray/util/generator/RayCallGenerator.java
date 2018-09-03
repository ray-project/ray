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
    newLine(1, "// =======================================");
    newLine(1, "// Methods for remote function invocation.");
    newLine(1, "// =======================================");
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      buildCalls(i, false, false);
    }
    newLine(1, "// ===========================================");
    newLine(1, "// Methods for remote actor method invocation.");
    newLine(1, "// ===========================================");
    for (int i = 0; i <= MAX_PARAMETERS - 1; i++) {
      buildCalls(i, true, false);
    }
    newLine(1, "// ===========================");
    newLine(1, "// Methods for actor creation.");
    newLine(1, "// ===========================");
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      buildCalls(i, false, true);
    }
    newLine("}");
    return sb.toString();
  }

  /**
   * Build the `Ray.call` or `Ray.createActor` methods with the given number of parameters.
   * @param numParameters the number of parameters
   * @param forActor build actor api when true, otherwise build task api.
   * @param forActorCreation build `Ray.createActor` when true, otherwise build `Ray.call`.
   */
  private void buildCalls(int numParameters, boolean forActor, boolean forActorCreation) {
    String genericTypes = "";
    String argList = "";
    for (int i = 0; i < numParameters; i++) {
      genericTypes += "T" + i + ", ";
      argList += "t" + i + ", ";
    }
    if (forActor) {
      genericTypes = "A, " + genericTypes;
    }
    genericTypes += forActorCreation ? "A" : "R";
    if (argList.endsWith(", ")) {
      argList = argList.substring(0, argList.length() - 2);
    }

    String paramPrefix = String.format("RayFunc%d<%s> f",
        !forActor ? numParameters : numParameters + 1,
        genericTypes);
    if (forActor) {
      paramPrefix += ", RayActor<A> actor";
    }
    if (numParameters > 0) {
      paramPrefix += ", ";
    }

    String returnType = !forActorCreation ? "RayObject<R>" : "RayActor<A>";
    String funcName = !forActorCreation ? "call" : "createActor";
    String funcArgs = !forActor ? "f, args" : "f, actor, args";
    for (String param : generateParameters(0, numParameters)) {
      // method signature
      newLine(1, String.format(
          "public static <%s> %s %s(%s) {",
          genericTypes, returnType, funcName, paramPrefix + param
      ));
      // method body
      newLine(2, String.format("Object[] args = new Object[]{%s};", argList));
      newLine(2, String.format("return Ray.internal().%s(%s);", funcName, funcArgs));
      newLine(1, "}");
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
