package org.ray.runtime.functionmanager;

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
    functionManager = new FunctionManager();
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
}
