package io.ray.serialization.codegen;

import static io.ray.serialization.codegen.Code.ExprCode;
import static io.ray.serialization.codegen.CodeGenerator.alignIndent;
import static io.ray.serialization.codegen.CodeGenerator.indent;
import static io.ray.serialization.codegen.CodeGenerator.spaces;
import static io.ray.serialization.codegen.TypeUtils.getArrayType;
import static io.ray.serialization.util.StringUtils.format;
import static io.ray.serialization.util.StringUtils.uncapitalize;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import io.ray.serialization.util.Tuple2;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * CodegenContext can be a any scope in a class, such as class, method, local and so on.
 *
 * <p>All constructor of generated class will call {@code initialize()} to initialize object. We
 * don't use instance initialize, so user can add init code which depends on used-passed
 * constructor's args.
 */
public class CodegenContext {
  public static Set<String> JAVA_RESERVED_WORDS;

  static {
    JAVA_RESERVED_WORDS = new HashSet<>();
    JAVA_RESERVED_WORDS.addAll(
        Arrays.asList(
            "abstract",
            "assert",
            "boolean",
            "break",
            "byte",
            "case",
            "catch",
            "char",
            "class",
            "const",
            "continue",
            "default",
            "do",
            "double",
            "else",
            "enum",
            "extends",
            "final",
            "finally",
            "float",
            "for",
            "goto",
            "if",
            "implements",
            "import",
            "instanceof",
            "int",
            "interface",
            "long",
            "native",
            "new",
            "package",
            "private",
            "protected",
            "public",
            "return",
            "short",
            "static",
            "strictfp",
            "super",
            "switch",
            "synchronized",
            "this",
            "throw",
            "throws",
            "transient",
            "try",
            "void",
            "volatile",
            "while",
            "true",
            "false",
            "null"));
    JAVA_RESERVED_WORDS = ImmutableSet.copyOf(JAVA_RESERVED_WORDS);
  }

  private static final String INITIALIZE_METHOD_NAME = "initialize";

  private static LinkedHashSet<String> defaultImports = new LinkedHashSet<>();

  static {
    defaultImports.add("java.util.*");
  }

  Map<String, Long> freshNameIds = new HashMap<>();

  /**
   * State used for expression elimination/reuse.
   *
   * <p>Takes the first expression and requests it to generate a Java source code for the expression
   * tree
   *
   * <p>The exprCode's code of subsequent same expression will be null, because the code is already
   * added to current context
   */
  Map<Expression, ExprCode> exprState = new HashMap<>();

  String pkg;
  LinkedHashSet<String> imports = new LinkedHashSet<>();
  String className;
  String[] superClasses;
  String[] interfaces;
  List<Tuple2<String, String>> fields = new ArrayList<>();
  /**
   * all initCodes would be placed into a method called initialize(), which will be called by
   * constructor.
   */
  List<String> initCodes = new ArrayList<>();

  List<String> constructors = new ArrayList<>();
  LinkedHashMap<String, String> methods = new LinkedHashMap<>();

  private CodegenContext instanceInitCtx;

  public CodegenContext() {
    defaultImports.forEach(this::addImport);
  }

  /**
   * Reserve name to avoid name collision for name that not created with {@link
   * CodegenContext#freshName(String)}
   */
  public void reserveName(String name) {
    freshName(name);
  }

  /**
   * If name is a java reserved word, return as if called with name "value".
   *
   * <p>Since we don't pass in TypeToken, no need to consider generics
   */
  public String freshName(Class<?> clz) {
    if (clz.isArray()) {
      return freshName("arr");
    } else {
      String type = type(clz);
      int index = type.lastIndexOf(".");
      String name;
      if (index >= 0) {
        name = uncapitalize(type.substring(index + 1));
      } else {
        name = uncapitalize(type);
      }

      if (JAVA_RESERVED_WORDS.contains(name)) {
        return freshName("value");
      } else {
        return freshName(name);
      }
    }
  }

  /** Returns a term name that is unique within this instance of a `CodegenContext`. */
  public String freshName(String name) {
    freshNameIds.putIfAbsent(name, 0L);
    long id = freshNameIds.get(name);
    freshNameIds.put(name, id + 1);
    if (id == 0) {
      return name;
    } else {
      return String.format("%s%s", name, id);
    }
  }

  /** Returns two term names that have same suffix to get more readability for generated code */
  public String[] freshNames(Class<?> clz1, String name2) {
    if (clz1.isArray()) {
      return freshNames("arr", name2);
    } else {
      String type = type(clz1);
      int index = type.lastIndexOf(".");
      String name;
      if (index >= 0) {
        name = uncapitalize(type.substring(index + 1));
      } else {
        name = uncapitalize(type);
      }
      if (JAVA_RESERVED_WORDS.contains(name)) {
        return freshNames("value", name2);
      } else {
        return freshNames(name, name2);
      }
    }
  }

  /** Returns term names that have same suffix to get more readability for generated code */
  public String[] freshNames(String... names) {
    long id = 0;
    for (int i = 0; i < names.length; i++) {
      id = Math.max(id, freshNameIds.getOrDefault(names[i], 0L));
    }
    for (int i = 0; i < names.length; i++) {
      freshNameIds.put(names[i], id + 1);
    }
    if (id == 0) {
      return names;
    } else {
      String[] newNames = new String[names.length];
      for (int i = 0; i < names.length; i++) {
        newNames[i] = String.format("%s%s", names[i], id);
      }
      return newNames;
    }
  }

  /**
   * @param clz type
   * @return simple name for class if type's canonical name starts with java.lang or is imported,
   *     return canonical name otherwise.
   */
  public String type(Class<?> clz) {
    if (clz.isArray()) {
      return getArrayType(clz);
    }
    String type = clz.getCanonicalName();
    if (type.startsWith("java.lang")) {
      if (!type.substring("java.lang.".length()).contains(".")) {
        return clz.getSimpleName();
      }
    }
    if (imports.contains(type)) {
      return clz.getSimpleName();
    } else {
      int index = type.lastIndexOf(".");
      if (index > 0) {
        // This might be package name or qualified name of outer class
        String pkgOrClassName = type.substring(0, index);
        if (imports.contains(pkgOrClassName + ".*")) {
          return clz.getSimpleName();
        }
      }
      return type;
    }
  }

  /** return type name. since janino doesn't generics, we ignore type parameters in typeToken. */
  public String type(TypeToken<?> typeToken) {
    return type(typeToken.getRawType());
  }

  /**
   * Set the generated class's package
   *
   * @param pkg java package
   */
  public void setPackage(String pkg) {
    this.pkg = pkg;
  }

  /**
   * Import classes
   *
   * @param classes classes to be imported
   */
  public void addImports(Class<?>... classes) {
    for (Class<?> aClass : classes) {
      imports.add(aClass.getCanonicalName());
    }
  }

  /**
   * Add imports
   *
   * @param imports import statements
   */
  public void addImports(String... imports) {
    this.imports.addAll(Arrays.asList(imports));
  }

  /**
   * Import class
   *
   * @param cls class to be imported
   */
  public void addImport(Class<?> cls) {
    this.imports.add(cls.getCanonicalName());
  }

  /**
   * Add import
   *
   * @param im import statement
   */
  public void addImport(String im) {
    this.imports.add(im);
  }

  /**
   * Set class name of class to be generated
   *
   * @param className class name of class to be generated
   */
  public void setClassName(String className) {
    this.className = className;
  }

  /**
   * Set super classes
   *
   * @param superClasses super classes
   */
  public void extendsClasses(String... superClasses) {
    this.superClasses = superClasses;
  }

  /**
   * Set implemented interfaces
   *
   * @param interfaces implemented interfaces
   */
  public void implementsInterfaces(String... interfaces) {
    this.interfaces = interfaces;
  }

  public void addConstructor(String codeBody, Object... params) {
    List<Tuple2<String, String>> parameters = getParameters(params);
    String paramsStr =
        parameters.stream().map(t -> t.f0 + " " + t.f1).collect(Collectors.joining(", "));
    String constructor =
        format(
            ""
                + "public ${className}(${paramsStr}) {\n"
                + "    ${codeBody}\n"
                + "    ${initMethod}();\n"
                + "}",
            "className",
            className,
            "paramsStr",
            paramsStr,
            "codeBody",
            alignIndent(codeBody),
            "initMethod",
            INITIALIZE_METHOD_NAME);
    constructors.add(constructor);
  }

  public void addStaticMethod(
      String methodName, String codeBody, Class<?> returnType, Object... params) {
    addMethod("public static", methodName, codeBody, returnType, params);
  }

  public void addMethod(String methodName, String codeBody, Class<?> returnType, Object... params) {
    addMethod("public", methodName, codeBody, returnType, params);
  }

  public void addMethod(
      String modifier, String methodName, String codeBody, Class<?> returnType, Object... params) {
    List<Tuple2<String, String>> parameters = getParameters(params);
    String paramsStr =
        parameters.stream().map(t -> t.f0 + " " + t.f1).collect(Collectors.joining(", "));
    String method =
        format(
            ""
                + "${modifier} ${returnType} ${methodName}(${paramsStr}) {\n"
                + "    ${codeBody}\n"
                + "}\n",
            "modifier",
            modifier,
            "returnType",
            type(returnType),
            "methodName",
            methodName,
            "paramsStr",
            paramsStr,
            "codeBody",
            alignIndent(codeBody));
    String signature = String.format("%s(%s)", methodName, paramsStr);
    if (methods.containsKey(signature)) {
      throw new IllegalStateException(String.format("Duplicated method signature: %s", signature));
    }
    methods.put(signature, method);
  }

  public void overrideMethod(
      String methodName, String codeBody, Class<?> returnType, Object... params) {
    addMethod("@Override public final", methodName, codeBody, returnType, params);
  }

  /** @param args type, value; type, value; type, value; ...... */
  private List<Tuple2<String, String>> getParameters(Object... args) {
    Preconditions.checkArgument(args.length % 2 == 0);
    List<Tuple2<String, String>> params = new ArrayList<>(args.length % 2);
    for (int i = 0; i < args.length; i += 2) {
      String type;
      if (args[i] instanceof Class) {
        type = type(((Class<?>) args[i]));
      } else {
        type = args[i].toString();
      }
      params.add(Tuple2.of(type, args[i + 1].toString()));
    }
    return params;
  }

  /**
   * Add a field to class
   *
   * @param type type
   * @param fieldName field name
   * @param initExpr field init expression
   */
  public void addField(Class<?> type, String fieldName, Expression initExpr) {
    addField(type.getCanonicalName(), fieldName, initExpr);
  }

  /**
   * Add a field to class
   *
   * @param type type
   * @param fieldName field name
   * @param initExpr field init expression
   */
  public void addField(String type, String fieldName, Expression initExpr) {
    if (instanceInitCtx == null) {
      instanceInitCtx = new CodegenContext();
      instanceInitCtx.freshNameIds = freshNameIds;
      instanceInitCtx.imports = imports;
    }
    fields.add(Tuple2.of(type, fieldName));
    ExprCode exprCode = initExpr.genCode(instanceInitCtx);
    initCodes.add(exprCode.code());
    initCodes.add(String.format("%s = %s;", fieldName, exprCode.value()));
  }

  /**
   * Add a field to class
   *
   * @param type type
   * @param fieldName field name
   * @param initCode field init code
   */
  public void addField(String type, String fieldName, String initCode) {
    fields.add(Tuple2.of(type, fieldName));
    initCodes.add(initCode);
  }

  /**
   * Add a field to class. The init code should be placed in constructor's code
   *
   * @param type type
   * @param fieldName field name
   */
  public void addField(String type, String fieldName) {
    fields.add(Tuple2.of(type, fieldName));
  }

  private void genInitializeMethod(StringBuilder codeBuilder) {
    codeBuilder
        .append(spaces(4))
        .append("private void ")
        .append(INITIALIZE_METHOD_NAME)
        .append("() {\n");
    for (String init : initCodes) {
      codeBuilder.append(indent(init, 8)).append('\n');
    }
    codeBuilder.append(spaces(4)).append("}\n");
  }

  /** Generate code for class */
  public String genCode() {
    StringBuilder codeBuilder = new StringBuilder();

    codeBuilder.append("package ").append(pkg).append(";\n\n");

    if (!imports.isEmpty()) {
      imports.forEach(clz -> codeBuilder.append("import ").append(clz).append(";\n"));
      codeBuilder.append('\n');
    }

    codeBuilder.append(String.format("public final class %s ", className));
    if (superClasses != null) {
      codeBuilder.append(String.format("extends %s ", String.join(", ", superClasses)));
    }
    if (interfaces != null) {
      codeBuilder.append(String.format("implements %s ", String.join(", ", interfaces)));
    }
    codeBuilder.append("{\n");

    // fields
    if (!fields.isEmpty()) {
      codeBuilder.append('\n');
      for (Tuple2<String, String> field : fields) {
        String declare = String.format("private %s %s;\n", field.f0, field.f1);
        codeBuilder.append(indent(declare));
      }
    }

    // constructors
    if (!constructors.isEmpty()) {
      codeBuilder.append('\n');
      constructors.forEach(constructor -> codeBuilder.append(indent(constructor)).append('\n'));
    }

    // instance initialize
    codeBuilder.append('\n');
    genInitializeMethod(codeBuilder);

    // methods
    codeBuilder.append('\n');
    methods.values().forEach(method -> codeBuilder.append(indent(method)).append('\n'));

    codeBuilder.append('}');
    return codeBuilder.toString();
  }
}
