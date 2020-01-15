package org.ray.runtime.util.generator;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.io.FileUtils;

/**
 * A util class that generates `RayCall.java`, which provides type-safe interfaces for `Ray.call`
 * and `Ray.createActor`.
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
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      newLine("import org.ray.api.function.RayFunc" + i + ";");
    }
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      newLine("import org.ray.api.function.RayFuncVoid" + i + ";");
    }
    newLine("import org.ray.api.options.ActorCreationOptions;");
    newLine("import org.ray.api.options.CallOptions;");
    newLine("");

    newLine("/**");
    newLine(" * This class provides type-safe interfaces for `Ray.call` and `Ray.createActor`.");
    newLine(" **/");
    newLine("@SuppressWarnings({\"rawtypes\", \"unchecked\"})");
    newLine("class RayCall {");
    newLine(1, "// =======================================");
    newLine(1, "// Methods for remote function invocation.");
    newLine(1, "// =======================================");
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      buildCalls(i, false, false, true, false);
      buildCalls(i, false, false, true, true);
      buildCalls(i, false, false, false, false);
      buildCalls(i, false, false, false, true);
    }

    newLine(1, "// ===========================================");
    newLine(1, "// Methods for remote actor method invocation.");
    newLine(1, "// ===========================================");
    for (int i = 0; i <= MAX_PARAMETERS - 1; i++) {
      buildCalls(i, true, false, true, false);
      buildCalls(i, true, false, false, false);
    }
    newLine(1, "// ===========================");
    newLine(1, "// Methods for actor creation.");
    newLine(1, "// ===========================");
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      buildCalls(i, false, true, true, false);
      buildCalls(i, false, true, true, true);
    }

    newLine(1, "// ===========================");
    newLine(1, "// Cross-language methods.");
    newLine(1, "// ===========================");
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      buildPyCalls(i, false, false, false);
      buildPyCalls(i, false, false, true);
    }
    for (int i = 0; i <= MAX_PARAMETERS - 1; i++) {
      buildPyCalls(i, true, false, false);
    }
    for (int i = 0; i <= MAX_PARAMETERS; i++) {
      buildPyCalls(i, false, true, false);
      buildPyCalls(i, false, true, true);
    }
    newLine("}");
    return sb.toString();
  }

  /**
   * Build the `Ray.call` or `Ray.createActor` methods with the given number of parameters.
   *
   * @param numParameters the number of parameters
   * @param forActor build actor api when true, otherwise build task api.
   * @param hasReturn if true, build api for functions with return.
   * @param forActorCreation build `Ray.createActor` when true, otherwise build `Ray.call`.
   */
  private void buildCalls(int numParameters, boolean forActor,
      boolean forActorCreation, boolean hasReturn, boolean hasOptionsParam) {
    // Template of the generated function:
    // public static [genericTypes] [returnType] [callFunc]([argsDeclaration]) {
    //   Objects[] args = new Object[]{[args]};
    //   return Ray.internal().[callFunc](f[, actor], args[, options]);
    // }

    // 1) Construct the `genericTypes` part, e.g. `<T0, T1, T2, R>`.
    String genericTypes = "";
    for (int i = 0; i < numParameters; i++) {
      genericTypes += "T" + i + ", ";
    }
    if (forActor) {
      // Actor generic type.
      genericTypes = "A, " + genericTypes;
    }
    // Return generic type.
    if (forActorCreation) {
      genericTypes += "A, ";
    } else {
      if (hasReturn) {
        genericTypes += "R, ";
      }
    }
    if (!genericTypes.isEmpty()) {
      // Trim trailing ", ";
      genericTypes = genericTypes.substring(0, genericTypes.length() - 2);
      genericTypes = "<" + genericTypes + ">";
    }

    // 2) Construct the `returnType` part.
    String returnType;
    if (forActorCreation) {
      returnType = "RayActor<A>";
    } else {
      returnType = hasReturn ? "RayObject<R>" : "void";
    }

    // 3) Construct the `argsDeclaration` part.
    String argsDeclarationPrefix = String.format("RayFunc%s%d%s f, ",
        hasReturn ? "" : "Void",
        !forActor ? numParameters : numParameters + 1,
        genericTypes);
    if (forActor) {
      argsDeclarationPrefix += "RayActor<A> actor, ";
    }

    String callFunc = forActorCreation ? "createActor" : "call";

    // Enumerate all combinations of the parameters.
    for (String param : generateParameters(numParameters)) {
      String argsDeclaration = argsDeclarationPrefix + param;
      if (hasOptionsParam) {
        argsDeclaration +=
            forActorCreation ? "ActorCreationOptions options, " : "CallOptions options, ";
      }
      // Trim trailing ", ";
      argsDeclaration = argsDeclaration.substring(0, argsDeclaration.length() - 2);
      // Print the first line (method signature).
      newLine(1, String.format(
          "public static%s %s %s(%s) {",
          genericTypes.isEmpty() ? "" : " " + genericTypes, returnType, callFunc, argsDeclaration
      ));

      // 4) Construct the `args` part.
      String args = "";
      for (int i = 0; i < numParameters; i++) {
        args += "t" + i + ", ";
      }
      // Trim trailing ", ";
      if (!args.isEmpty()) {
        args = args.substring(0, args.length() - 2);
      }
      // Print the second line (local args declaration).
      newLine(2, String.format("Object[] args = new Object[]{%s};", args));

      // 5) Construct the third line.
      String callFuncArgs = "f, ";
      if (forActor) {
        callFuncArgs += "actor, ";
      }
      callFuncArgs += "args, ";
      callFuncArgs += forActor ? "" : hasOptionsParam ? "options, " : "null, ";
      callFuncArgs = callFuncArgs.substring(0, callFuncArgs.length() - 2);
      newLine(2, String.format("%sRay.internal().%s(%s);",
          hasReturn ? "return " : "", callFunc, callFuncArgs));
      newLine(1, "}");
    }
  }

  /**
   * Build the `Ray.callPy` or `Ray.createPyActor` methods.
   *
   * @param forActor build actor api when true, otherwise build task api.
   * @param forActorCreation build `Ray.createPyActor` when true, otherwise build `Ray.callPy`.
   */
  private void buildPyCalls(int numParameters, boolean forActor,
      boolean forActorCreation, boolean hasOptionsParam) {
    String argList = "";
    String paramList = "";
    for (int i = 0; i < numParameters; i++) {
      paramList += "Object obj" + i + ", ";
      argList += "obj" + i + ", ";
    }
    if (argList.endsWith(", ")) {
      argList = argList.substring(0, argList.length() - 2);
    }
    if (paramList.endsWith(", ")) {
      paramList = paramList.substring(0, paramList.length() - 2);
    }

    String paramPrefix = "";
    String funcArgs = "";
    if (forActorCreation) {
      paramPrefix += "String moduleName, String className";
      funcArgs += "moduleName, className";
    } else if (forActor) {
      paramPrefix += "RayPyActor pyActor, String functionName";
      funcArgs += "pyActor, functionName";
    } else {
      paramPrefix += "String moduleName, String functionName";
      funcArgs += "moduleName, functionName";
    }
    if (numParameters > 0) {
      paramPrefix += ", ";
    }

    String optionsParam;
    if (hasOptionsParam) {
      optionsParam = forActorCreation ? ", ActorCreationOptions options" : ", CallOptions options";
    } else {
      optionsParam = "";
    }

    String optionsArg;
    if (forActor) {
      optionsArg = "";
    } else {
      if (hasOptionsParam) {
        optionsArg = ", options";
      } else {
        optionsArg = ", null";
      }
    }

    String returnType = !forActorCreation ? "RayObject" : "RayPyActor";
    String funcName = !forActorCreation ? "callPy" : "createPyActor";
    funcArgs += ", args";
    // Method signature.
    newLine(1, String.format(
        "public static %s %s(%s%s) {",
        returnType, funcName, paramPrefix + paramList, optionsParam
    ));
    // Method body.
    newLine(2, String.format("Object[] args = new Object[]{%s};", argList));
    newLine(2, String.format("return Ray.internal().%s(%s%s);", funcName, funcArgs, optionsArg));
    newLine(1, "}");
  }

  private List<String> generateParameters(int numParams) {
    List<String> res = new ArrayList<>();
    dfs(0, numParams, "", res);
    return res;
  }

  private void dfs(int pos, int numParams, String cur, List<String> res) {
    if (pos >= numParams) {
      res.add(cur);
      return;
    }
    String nextParameter = String.format("T%d t%d, ", pos, pos);
    dfs(pos + 1, numParams, cur + nextParameter, res);
    nextParameter = String.format("RayObject<T%d> t%d, ", pos, pos);
    dfs(pos + 1, numParams, cur + nextParameter, res);
  }

  public static void main(String[] args) throws IOException {
    String path = System.getProperty("user.dir")
        + "/api/src/main/java/org/ray/api/RayCall.java";
    FileUtils.write(new File(path), new RayCallGenerator().build(), Charset.defaultCharset());
  }
}

