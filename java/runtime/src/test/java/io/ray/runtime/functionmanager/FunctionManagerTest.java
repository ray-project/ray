package io.ray.runtime.functionmanager;

import io.ray.api.function.RayFunc0;
import io.ray.api.function.RayFunc1;
import io.ray.api.id.JobId;
import io.ray.runtime.functionmanager.FunctionManager.JobFunctionTable;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/** Tests for {@link FunctionManager} */
public class FunctionManagerTest {

  public static Object foo() {
    return null;
  }

  public static class ParentClass {

    public Object foo() {
      return null;
    }

    public Object bar() {
      return null;
    }
  }

  public interface ChildClassInterface {

    default String interfaceName() {
      return getClass().getName();
    }
  }

  public static class ChildClass extends ParentClass implements ChildClassInterface {

    public ChildClass() {}

    @Override
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

  private static final JobId JOB_ID = JobId.fromInt(1);

  private static RayFunc0<Object> fooFunc;
  private static RayFunc1<ChildClass, Object> childClassBarFunc;
  private static RayFunc0<ChildClass> childClassConstructor;
  private static JavaFunctionDescriptor fooDescriptor;
  private static JavaFunctionDescriptor childClassBarDescriptor;
  private static JavaFunctionDescriptor childClassConstructorDescriptor;
  private static JavaFunctionDescriptor overloadFunctionDescriptorInt;
  private static JavaFunctionDescriptor overloadFunctionDescriptorDouble;

  @BeforeClass
  public static void beforeClass() {
    fooFunc = FunctionManagerTest::foo;
    childClassConstructor = ChildClass::new;
    childClassBarFunc = ChildClass::bar;
    fooDescriptor =
        new JavaFunctionDescriptor(
            FunctionManagerTest.class.getName(), "foo", "()Ljava/lang/Object;");
    childClassBarDescriptor =
        new JavaFunctionDescriptor(ChildClass.class.getName(), "bar", "()Ljava/lang/Object;");
    childClassConstructorDescriptor =
        new JavaFunctionDescriptor(
            ChildClass.class.getName(), FunctionManager.CONSTRUCTOR_NAME, "()V");
    overloadFunctionDescriptorInt =
        new JavaFunctionDescriptor(
            FunctionManagerTest.class.getName(), "overloadFunction", "(I)Ljava/lang/Object;");
    overloadFunctionDescriptorDouble =
        new JavaFunctionDescriptor(
            FunctionManagerTest.class.getName(), "overloadFunction", "(D)Ljava/lang/Object;");
  }

  @Test
  public void testGetFunctionFromRayFunc() {
    final FunctionManager functionManager = new FunctionManager(null);
    // Test normal function.
    RayFunction func = functionManager.getFunction(fooFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);

    // Test actor method
    func = functionManager.getFunction(childClassBarFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), childClassBarDescriptor);

    // Test actor constructor
    func = functionManager.getFunction(childClassConstructor);
    Assert.assertTrue(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), childClassConstructorDescriptor);
  }

  @Test
  public void testGetFunctionFromFunctionDescriptor() {
    final FunctionManager functionManager = new FunctionManager(null);
    // Test normal function.
    RayFunction func = functionManager.getFunction(fooDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);

    // Test actor method
    func = functionManager.getFunction(childClassBarDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), childClassBarDescriptor);

    // Test actor constructor
    func = functionManager.getFunction(childClassConstructorDescriptor);
    Assert.assertTrue(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), childClassConstructorDescriptor);

    // Test raise overload exception
    Assert.expectThrows(
        RuntimeException.class,
        () -> {
          functionManager.getFunction(
              new JavaFunctionDescriptor(
                  FunctionManagerTest.class.getName(), "overloadFunction", ""));
        });
  }

  @Test
  public void testInheritance() {
    final FunctionManager functionManager = new FunctionManager(null);
    // Check inheritance can work and FunctionManager can find method in parent class.
    fooDescriptor =
        new JavaFunctionDescriptor(ParentClass.class.getName(), "foo", "()Ljava/lang/Object;");
    Assert.assertEquals(
        functionManager.getFunction(fooDescriptor).executable.getDeclaringClass(),
        ParentClass.class);
    RayFunction fooFunc =
        functionManager.getFunction(
            new JavaFunctionDescriptor(ChildClass.class.getName(), "foo", "()Ljava/lang/Object;"));
    Assert.assertEquals(fooFunc.executable.getDeclaringClass(), ParentClass.class);

    // Check FunctionManager can use method in child class if child class methods overrides methods
    // in parent class.
    childClassBarDescriptor =
        new JavaFunctionDescriptor(ParentClass.class.getName(), "bar", "()Ljava/lang/Object;");
    Assert.assertEquals(
        functionManager.getFunction(childClassBarDescriptor).executable.getDeclaringClass(),
        ParentClass.class);
    RayFunction barFunc =
        functionManager.getFunction(
            new JavaFunctionDescriptor(ChildClass.class.getName(), "bar", "()Ljava/lang/Object;"));
    Assert.assertEquals(barFunc.executable.getDeclaringClass(), ChildClass.class);

    // Check interface default methods.
    RayFunction interfaceNameFunc =
        functionManager.getFunction(
            new JavaFunctionDescriptor(
                ChildClass.class.getName(), "interfaceName", "()Ljava/lang/String;"));
    Assert.assertEquals(
        interfaceNameFunc.executable.getDeclaringClass(), ChildClassInterface.class);
  }

  @Test
  public void testLoadFunctionTableForClass() {
    JobFunctionTable functionTable = new JobFunctionTable(getClass().getClassLoader());
    Map<Pair<String, String>, Pair<RayFunction, Boolean>> res =
        functionTable.loadFunctionsForClass(ChildClass.class.getName());
    // The result should be 5 entries:
    //   1, the constructor with signature
    //   2, the constructor without signature
    //   3, bar with signature
    //   4, bar without signature
    //   5, bar with the number of signature acting as signature field (xlang)
    Assert.assertEquals(res.size(), 16);
    Assert.assertTrue(
        res.containsKey(
            ImmutablePair.of(childClassBarDescriptor.name, childClassBarDescriptor.signature)));
    Assert.assertTrue(
        res.containsKey(
            ImmutablePair.of(
                childClassConstructorDescriptor.name, childClassConstructorDescriptor.signature)));
    Assert.assertTrue(res.containsKey(ImmutablePair.of(childClassBarDescriptor.name, "")));
    Assert.assertTrue(res.containsKey(ImmutablePair.of(childClassConstructorDescriptor.name, "")));
    Assert.assertTrue(
        res.containsKey(
            ImmutablePair.of(
                overloadFunctionDescriptorInt.name, overloadFunctionDescriptorInt.signature)));
    Assert.assertTrue(
        res.containsKey(
            ImmutablePair.of(
                overloadFunctionDescriptorDouble.name,
                overloadFunctionDescriptorDouble.signature)));
    Assert.assertTrue(res.containsKey(ImmutablePair.of(overloadFunctionDescriptorInt.name, "")));
    Pair<String, String> overloadKey = ImmutablePair.of(overloadFunctionDescriptorInt.name, "");
    RayFunction func = res.get(overloadKey).getLeft();
    // The function is overloaded.
    Assert.assertTrue(res.containsKey(overloadKey));
    Assert.assertNull(func);
  }

  @Test
  public void testGetFunctionFromLocalResource() throws Exception {
    final String codeSearchPath = FileUtils.getTempDirectoryPath() + "/ray_test_resources/";
    File jobResourceDir = new File(codeSearchPath);
    FileUtils.deleteQuietly(jobResourceDir);
    jobResourceDir.mkdirs();
    jobResourceDir.deleteOnExit();

    String demoJavaFile = "";
    demoJavaFile += "public class DemoApp {\n";
    demoJavaFile += "  public static String hello() {\n";
    demoJavaFile += "    return \"hello\";\n";
    demoJavaFile += "  }\n";
    demoJavaFile += "}";

    // Write the demo java file to the job code search path.
    String javaFilePath = codeSearchPath + "/DemoApp.java";
    Files.write(Paths.get(javaFilePath), demoJavaFile.getBytes());

    // Compile the java file.
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    int result = compiler.run(null, null, null, "-d", codeSearchPath, javaFilePath);
    if (result != 0) {
      throw new RuntimeException("Couldn't compile Demo.java.");
    }

    // Test loading the function.
    JavaFunctionDescriptor descriptor =
        new JavaFunctionDescriptor("DemoApp", "hello", "()Ljava/lang/String;");
    final FunctionManager functionManager =
        new FunctionManager(Collections.singletonList(codeSearchPath));
    RayFunction func = functionManager.getFunction(descriptor);
    Assert.assertEquals(func.getFunctionDescriptor(), descriptor);
  }
}
