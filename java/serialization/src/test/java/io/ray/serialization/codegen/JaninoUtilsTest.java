package io.ray.serialization.codegen;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.function.Function;
import org.codehaus.commons.compiler.CompileException;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.ScriptEvaluator;
import org.codehaus.janino.SimpleCompiler;
import org.testng.Assert;
import org.testng.annotations.Test;

public class JaninoUtilsTest {

  @Test
  public void compile() throws Exception {
    CompileUnit unit1 =
        new CompileUnit(
            "demo.pkg1",
            "A",
            (""
                + "package demo.pkg1;\n"
                + "import demo.pkg2.*;\n"
                + "public class A {\n"
                + "  public static String main() { return B.hello(); }\n"
                + "  public static String hello() { return \"HELLO\"; }\n"
                + "}"));
    CompileUnit unit2 =
        new CompileUnit(
            "demo.pkg2",
            "B",
            (""
                + "package demo.pkg2;\n"
                + "import demo.pkg1.*;\n"
                + "public class B {\n"
                + "  public static String hello() { return A.hello(); }\n"
                + "}"));
    ClassLoader classLoader =
        JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), unit1, unit2);
    Assert.assertEquals(
        "HELLO", classLoader.loadClass("demo.pkg1.A").getMethod("main").invoke(null));
  }

  // For 3.0.11: Total cost 98.523273 ms, average time is 1.970465 ms
  // For 3.1.2: Total cost 15863.328650 ms, average time is 317.266573 ms
  @Test
  public void benchmark() {
    CompileUnit unit =
        new CompileUnit(
            "demo.pkg1",
            "A",
            (""
                + "package demo.pkg1;\n"
                + "public class A {\n"
                + "  public static String hello() { return \"HELLO\"; }\n"
                + "}"));
    // Since janino is not called frequently, we test only 50 times.
    int iterNums = 50;
    for (int i = 0; i < iterNums; i++) {
      JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), unit);
    }
    long startTime = System.nanoTime();
    for (int i = 0; i < iterNums; i++) {
      JaninoUtils.compile(Thread.currentThread().getContextClassLoader(), unit);
    }
    long duration = System.nanoTime() - startTime;
    System.out.printf(
        "Total cost %f ms, average time is %f ms",
        (double) duration / 1000_000, (double) duration / iterNums / 1000_000);
  }

  @Test
  public void testJaninoScript() throws CompileException, InvocationTargetException {
    ScriptEvaluator se = new ScriptEvaluator();
    se.cook(
        ""
            + "static void method1() {\n"
            + "  int i = 2;\n"
            + "  int j = i * 3;\n"
            + "}\n"
            + "\n"
            + "method1();\n"
            + "method2();\n"
            + "\n"
            + "static void method2() {\n"
            + "  int x = 3;\n"
            + "  int y = x * 3;\n"
            + "}\n");
    se.evaluate(new Object[0]);
  }

  @Test
  public void testJaninoClassBody()
      throws CompileException, IllegalAccessException, InstantiationException {
    ClassBodyEvaluator evaluator = new ClassBodyEvaluator();
    // default to context class loader. set if only use another class loader
    evaluator.setParentClassLoader(Thread.currentThread().getContextClassLoader());
    evaluator.setDefaultImports(List.class.getName());
    evaluator.setImplementedInterfaces(new Class[] {Function.class});
    String code =
        ""
            + "@Override\n"
            + "public Object apply(Object str) {\n"
            + "  return ((String)str).length();\n"
            + "}";
    evaluator.cook(code);
    Class<?> clazz = evaluator.getClazz();
    @SuppressWarnings("unchecked")
    Function<Object, Object> function = (Function<Object, Object>) clazz.newInstance();
    Assert.assertEquals(function.apply("test class body"), "test class body".length());
  }

  @Test
  public void testJaninoClass()
      throws CompileException, ClassNotFoundException, IllegalAccessException,
          InstantiationException {
    SimpleCompiler compiler = new SimpleCompiler();
    String code =
        ""
            + "import java.util.function.Function;\n"
            + "public class A implements Function {\n"
            + "  @Override\n"
            + "  public Object apply(Object o) {\n"
            + "    return o;\n"
            + "  }\n"
            + "}";
    // default to context class loader. set if only use another class loader
    compiler.setParentClassLoader(Thread.currentThread().getContextClassLoader());
    compiler.cook(code);
    Class<? extends Function> aCLass =
        compiler.getClassLoader().loadClass("A").asSubclass(Function.class);
    @SuppressWarnings("unchecked")
    Function<Object, Object> function = aCLass.newInstance();
    Assert.assertEquals(function.apply("test class"), "test class");
  }
}
