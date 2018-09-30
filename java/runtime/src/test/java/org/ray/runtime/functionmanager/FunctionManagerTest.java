package org.ray.runtime.functionmanager;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.ray.api.annotation.RayRemote;
import org.ray.api.function.RayFunc0;
import org.ray.api.function.RayFunc1;
import org.ray.api.id.UniqueId;
import org.ray.runtime.functionmanager.FunctionManager.DriverFunctionTable;

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

  private final static String resourcePath = "/tmp/ray/test/resource";

  private FunctionManager functionManager;

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

  @Before
  public void before() {
    functionManager = new FunctionManager(FunctionManagerTest.resourcePath);
  }

  @Test
  public void testGetFunctionFromRayFunc() {
    // Test normal function.
    RayFunction func = functionManager.getFunction(UniqueId.NIL, fooFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());

    // Test actor method
    func = functionManager.getFunction(UniqueId.NIL, barFunc);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());

    // Test actor constructor
    func = functionManager.getFunction(UniqueId.NIL, barConstructor);
    Assert.assertTrue(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barConstructorDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());
  }

  @Test
  public void testGetFunctionFromFunctionDescriptor() {
    // Test normal function.
    RayFunction func = functionManager.getFunction(UniqueId.NIL, fooDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), fooDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());

    // Test actor method
    func = functionManager.getFunction(UniqueId.NIL, barDescriptor);
    Assert.assertFalse(func.isConstructor());
    Assert.assertEquals(func.getFunctionDescriptor(), barDescriptor);
    Assert.assertNotNull(func.getRayRemoteAnnotation());

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
  public void testGetFunctionFromLocalResource() throws Exception{
    UniqueId driverId = UniqueId.fromHexString("0123456789012345678901234567890123456789");

    //TODO(qwang): We should use a independent app demo instead of `tutorial`.
    final String srcJarPath = System.getProperty("user.dir") +
                                  "/../tutorial/target/ray-tutorial-0.1-SNAPSHOT.jar";
    final String destJarPath = resourcePath + "/" + driverId.toString() +
                                   "/ray-tutorial-0.1-SNAPSHOT.jar";

    File file = new File(resourcePath + "/" + driverId.toString());
    file.mkdirs();

    Files.copy(Paths.get(srcJarPath), Paths.get(destJarPath), StandardCopyOption.REPLACE_EXISTING);

    FunctionDescriptor sayHelloDescriptor = new FunctionDescriptor("org.ray.exercise.Exercise02",
        "sayHello", "()Ljava/lang/String;");
    RayFunction func = functionManager.getFunction(driverId, sayHelloDescriptor);
    Assert.assertEquals(func.getFunctionDescriptor(), sayHelloDescriptor);
  }

}
