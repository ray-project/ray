package org.ray.runtime.functionmanager;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.ray.api.annotation.RayRemote;
import org.ray.api.function.RayFunc0;
import org.ray.api.function.RayFunc1;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.FunctionManager.DriverFunctionTable;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;

/**
 * Tests for {@link FunctionManager}
 */
public class FunctionManagerTest {

  @RayRemote
  public static Object foo() {
    return null;
  }

  @RayRemote
  public static class Bar {

    public Bar() {
    }

    public Object bar() {
      return null;
    }
  }

  private static RayFunc0<Object> fooFunc;
  private static RayFunc1<Bar, Object> barFunc;
  private static RayFunc0<Bar> barConstructor;
  private static FunctionDescriptor fooDescriptor;
  private static FunctionDescriptor barDescriptor;
  private static FunctionDescriptor barConstructorDescriptor;

  @BeforeClass
  public static void beforeClass() {
    fooFunc = FunctionManagerTest::foo;
    barConstructor = Bar::new;
    barFunc = Bar::bar;
    fooDescriptor = new FunctionDescriptor(FunctionManagerTest.class.getName(), "foo",
        "()Ljava/lang/Object;");
    barDescriptor = new FunctionDescriptor(Bar.class.getName(), "bar",
        "()Ljava/lang/Object;");
    barConstructorDescriptor = new FunctionDescriptor(Bar.class.getName(),
        FunctionManager.CONSTRUCTOR_NAME,
        "()V");
  }

  @Test
  public void testGetFunctionFromRayFunc() {
    final FunctionManager functionManager = new FunctionManager(null);
    // Test normal function.
    RayFunction func = functionManager.getFunction(UniqueId.NIL, fooFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());

    // Test actor method
    func = functionManager.getFunction(UniqueId.NIL, barFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barDescriptor);
    Assert.assertNull(func.getRayRemoteAnnotation());

    // Test actor constructor
    func = functionManager.getFunction(UniqueId.NIL, barConstructor);
    Assert.assertTrue(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barConstructorDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());
  }

  @Test
  public void testGetFunctionFromFunctionDescriptor() {
    final FunctionManager functionManager = new FunctionManager(null);
    // Test normal function.
    RayFunction func = functionManager.getFunction(UniqueId.NIL, fooDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());

    // Test actor method
    func = functionManager.getFunction(UniqueId.NIL, barDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barDescriptor);
    Assert.assertNull(func.getRayRemoteAnnotation());

    // Test actor constructor
    func = functionManager.getFunction(UniqueId.NIL, barConstructorDescriptor);
    Assert.assertTrue(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barConstructorDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());
  }

  @Test
  public void testLoadFunctionTableForClass() {
    DriverFunctionTable functionTable = new DriverFunctionTable(getClass().getClassLoader());
    Map<Pair<String, String>, RayFunction> res = functionTable
        .loadFunctionsForClass(Bar.class.getName());
    // The result should 2 entries, one for the constructor, the other for bar.
    Assert.assertEquals(res.size(), 2);
    Assert.assertTrue(res.containsKey(
        ImmutablePair.of(barDescriptor.name, barDescriptor.typeDescriptor)));
    Assert.assertTrue(res.containsKey(
        ImmutablePair.of(barConstructorDescriptor.name, barConstructorDescriptor.typeDescriptor)));
  }

  //TODO(qwang): This is an integration test case, and we should move it to test folder in the future.
  @Test
  public void testGetFunctionFromLocalResource() throws Exception {
    final String resourcePath = "/tmp/ray/java_test/resource";
    UniqueId driverId = UniqueId.randomId();
    final String driverResourcePath = resourcePath + "/" + driverId.toString();
    final String destDir = System.getProperty("user.dir") + "/org/ray/demo";
    compileAndPack("../runtime/src/main/resources/DemoApp.java", destDir);

    final String sourcePath = destDir + "/DemoApp.jar";
    final String destPath = driverResourcePath + "/DemoApp.jar";
    File file = new File(destPath);
    file.mkdirs();
    Files.move(Paths.get(sourcePath), Paths.get(destPath),
      StandardCopyOption.REPLACE_EXISTING);

    FunctionDescriptor sayHelloDescriptor = new FunctionDescriptor(
        "org.ray.demo.DemoApp", "sayHello", "()Ljava/lang/String;");
    final FunctionManager functionManager = new FunctionManager(resourcePath);

    RayFunction func = functionManager.getFunction(driverId, sayHelloDescriptor);
    Assert.assertEquals(func.getFunctionDescriptor(), sayHelloDescriptor);
  }

  private static void compileAndPack(String sourceFilePath, String destDir) throws Exception {
    JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
    int result = compiler.run(null, null, null, sourceFilePath);

    if (result != 0) {
      throw new RuntimeException(String.format("Failed to compile file : %s", sourceFilePath));
    }


    String[] packJarCommand = new String[]{
      "jar",
      "-cvf",
      destDir + "/DemoApp.jar",
      "org/ray/demo/DemoApp.class"
    };

    Runtime.getRuntime().exec(packJarCommand);

    // Move to destination directory.
    final String sourceFileDir = (new File(sourceFilePath)).getParent();
    final String classFileSourcePath = sourceFileDir + "/DemoApp.class";
    final String classFileDestPath = destDir + "/DemoApp.class";
    File file = new File(classFileDestPath);
    file.mkdirs();
    Files.move(Paths.get(classFileSourcePath), Paths.get(classFileDestPath),
      StandardCopyOption.REPLACE_EXISTING);

    // Sleep for waiting for jar.
    TimeUnit.SECONDS.sleep(1);
  }

}
