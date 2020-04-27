package io.ray.runtime.functionmanager;

import io.ray.api.function.RayFunc0;
import io.ray.api.function.RayFunc1;
import io.ray.api.id.JobId;
import io.ray.runtime.functionmanager.FunctionManager.JobFunctionTable;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * Tests for {@link FunctionManager}
 */
public class FunctionManagerTest {

  public static Object foo() {
    return null;
  }

  public static class Bar {

    public Bar() {
    }

    public Object bar() {
      return null;
    }

    public Object overloadFunction(int i) {
      return null;
    }

    public Object overloadFunction(double d) {
      return null;
    }
  }

  private static RayFunc0<Object> fooFunc;
  private static RayFunc1<Bar, Object> barFunc;
  private static RayFunc0<Bar> barConstructor;
  private static JavaFunctionDescriptor fooDescriptor;
  private static JavaFunctionDescriptor barDescriptor;
  private static JavaFunctionDescriptor barConstructorDescriptor;
  private static JavaFunctionDescriptor overloadFunctionDescriptorInt;
  private static JavaFunctionDescriptor overloadFunctionDescriptorDouble;

  @BeforeClass
  public static void beforeClass() {
    fooFunc = FunctionManagerTest::foo;
    barConstructor = Bar::new;
    barFunc = Bar::bar;
    fooDescriptor = new JavaFunctionDescriptor(FunctionManagerTest.class.getName(), "foo",
        "()Ljava/lang/Object;");
    barDescriptor = new JavaFunctionDescriptor(Bar.class.getName(), "bar",
        "()Ljava/lang/Object;");
    barConstructorDescriptor = new JavaFunctionDescriptor(Bar.class.getName(),
        FunctionManager.CONSTRUCTOR_NAME,
        "()V");
    overloadFunctionDescriptorInt = new JavaFunctionDescriptor(FunctionManagerTest.class.getName(),
        "overloadFunction", "(I)Ljava/lang/Object;");
    overloadFunctionDescriptorDouble = new JavaFunctionDescriptor(FunctionManagerTest.class.getName(),
        "overloadFunction", "(D)Ljava/lang/Object;");
  }

  @Test
  public void testGetFunctionFromRayFunc() {
    final FunctionManager functionManager = new FunctionManager(null);
    // Test normal function.
    RayFunction func = functionManager.getFunction(JobId.NIL, fooFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);

    // Test actor method
    func = functionManager.getFunction(JobId.NIL, barFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barDescriptor);

    // Test actor constructor
    func = functionManager.getFunction(JobId.NIL, barConstructor);
    Assert.assertTrue(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barConstructorDescriptor);
  }

  @Test
  public void testGetFunctionFromFunctionDescriptor() {
    final FunctionManager functionManager = new FunctionManager(null);
    // Test normal function.
    RayFunction func = functionManager.getFunction(JobId.NIL, fooDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);

    // Test actor method
    func = functionManager.getFunction(JobId.NIL, barDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barDescriptor);

    // Test actor constructor
    func = functionManager.getFunction(JobId.NIL, barConstructorDescriptor);
    Assert.assertTrue(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barConstructorDescriptor);

    // Test raise overload exception
    Assert.expectThrows(RuntimeException.class, () -> {
      functionManager.getFunction(JobId.NIL,
          new JavaFunctionDescriptor(FunctionManagerTest.class.getName(),
              "overloadFunction", ""));
    });
  }

  @Test
  public void testLoadFunctionTableForClass() {
    JobFunctionTable functionTable = new JobFunctionTable(getClass().getClassLoader());
    Map<Pair<String, String>, RayFunction> res = functionTable
        .loadFunctionsForClass(Bar.class.getName());
    // The result should be 4 entries:
    //   1, the constructor with signature
    //   2, the constructor without signature
    //   3, bar with signature
    //   4, bar without signature
    Assert.assertEquals(res.size(), 7);
    Assert.assertTrue(res.containsKey(
        ImmutablePair.of(barDescriptor.name, barDescriptor.signature)));
    Assert.assertTrue(res.containsKey(
        ImmutablePair.of(barConstructorDescriptor.name, barConstructorDescriptor.signature)));
    Assert.assertTrue(res.containsKey(
            ImmutablePair.of(barDescriptor.name, "")));
    Assert.assertTrue(res.containsKey(
            ImmutablePair.of(barConstructorDescriptor.name, "")));
    Assert.assertTrue(res.containsKey(
            ImmutablePair.of(overloadFunctionDescriptorInt.name, overloadFunctionDescriptorInt.signature)));
    Assert.assertTrue(res.containsKey(
            ImmutablePair.of(overloadFunctionDescriptorDouble.name, overloadFunctionDescriptorDouble.signature)));
    Assert.assertTrue(res.containsKey(
            ImmutablePair.of(overloadFunctionDescriptorInt.name, "")));
    Pair<String, String> overloadKey = ImmutablePair.of(overloadFunctionDescriptorInt.name, "");
    RayFunction func = res.get(overloadKey);
    // The function is overloaded.
    Assert.assertTrue(res.containsKey(overloadKey));
    Assert.assertNull(func);
  }

  @Test
  public void testGetFunctionFromLocalResource() throws Exception {
    JobId jobId = JobId.fromInt(1);
    final String resourcePath = FileUtils.getTempDirectoryPath() + "/ray_test_resources";
    final String jobResourcePath = resourcePath + "/" + jobId.toString();
    File jobResourceDir = new File(jobResourcePath);
    FileUtils.deleteQuietly(jobResourceDir);
    jobResourceDir.mkdirs();
    jobResourceDir.deleteOnExit();

    String demoJavaFile = "";
    demoJavaFile += "public class DemoApp {\n";
    demoJavaFile += "  public static String hello() {\n";
    demoJavaFile += "    return \"hello\";\n";
    demoJavaFile += "  }\n";
    demoJavaFile += "}";

    // Write the demo java file to the job resource path.
    String javaFilePath = jobResourcePath + "/DemoApp.java";
    Files.write(Paths.get(javaFilePath), demoJavaFile.getBytes());

    // Compile the java file.
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    int result = compiler.run(null, null, null, "-d", jobResourcePath, javaFilePath);
    if (result != 0) {
      throw new RuntimeException("Couldn't compile Demo.java.");
    }

    // Test loading the function.
    JavaFunctionDescriptor descriptor = new JavaFunctionDescriptor(
        "DemoApp", "hello", "()Ljava/lang/String;");
    final FunctionManager functionManager = new FunctionManager(resourcePath);
    RayFunction func = functionManager.getFunction(jobId, descriptor);
    Assert.assertEquals(func.getFunctionDescriptor(), descriptor);
  }

}
