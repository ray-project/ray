package io.ray.test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.function.PyFunction;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.actor.NativePyActorHandle;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CrossLanguageInvocationTest extends BaseMultiLanguageTest {

  private static final Logger LOGGER = LoggerFactory.getLogger(CrossLanguageInvocationTest.class);
  private static final String PYTHON_MODULE = "test_cross_language_invocation";

  @Override
  protected Map<String, String> getRayStartEnv() {
    // Delete and re-create the temp dir.
    File tempDir = new File(
        System.getProperty("java.io.tmpdir") + File.separator + "ray_cross_language_test");
    FileUtils.deleteQuietly(tempDir);
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    // Write the test Python file to the temp dir.
    InputStream in = CrossLanguageInvocationTest.class
        .getResourceAsStream("/" + PYTHON_MODULE + ".py");
    File pythonFile = new File(
        tempDir.getAbsolutePath() + File.separator + PYTHON_MODULE + ".py");
    try {
      FileUtils.copyInputStreamToFile(in, pythonFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return ImmutableMap.of("PYTHONPATH", tempDir.getAbsolutePath());
  }

  @Test
  public void testCallingPythonFunction() {
    Object[] inputs = new Object[]{
        true,  // Boolean
        Byte.MAX_VALUE,  // Byte
        Short.MAX_VALUE,  // Short
        Integer.MAX_VALUE,  // Integer
        Long.MAX_VALUE,  // Long
        // BigInteger can support max value of 2^64-1, please refer to:
        // https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
        // If BigInteger larger than 2^64-1, the value can only be transferred among Java workers.
        BigInteger.valueOf(Long.MAX_VALUE),  // BigInteger
        "Hello World!",  // String
        1.234f,  // Float
        1.234,  // Double
        "example binary".getBytes()};  // byte[]
    for (Object o : inputs) {
      ObjectRef res = Ray.task(
          PyFunction.of(PYTHON_MODULE, "py_return_input", o.getClass()),
          o).remote();
      Assert.assertEquals(res.get(), o);
    }
    // null
    {
      Object input = null;
      ObjectRef<Object> res = Ray.task(
          PyFunction.of(PYTHON_MODULE, "py_return_input", Object.class), input).remote();
      Object r = res.get();
      Assert.assertEquals(r, input);
    }
    // array
    {
      int[] input = new int[]{1, 2};
      ObjectRef<int[]> res = Ray.task(
          PyFunction.of(PYTHON_MODULE, "py_return_input", int[].class), input).remote();
      int[] r = res.get();
      Assert.assertEquals(r, input);
    }
    // array of Object
    {
      Object[] input = new Object[]{1, 2.3f, 4.56, "789", "10".getBytes(), null, true,
          new int[]{1, 2}};
      ObjectRef<Object[]> res = Ray.task(
          PyFunction.of(PYTHON_MODULE, "py_return_input", Object[].class), input).remote();
      Object[] r = res.get();
      // If we tell the value type is Object, then all numbers will be Number type.
      Assert.assertEquals(((Number) r[0]).intValue(), input[0]);
      Assert.assertEquals(((Number) r[1]).floatValue(), input[1]);
      Assert.assertEquals(((Number) r[2]).doubleValue(), input[2]);
      // String cast
      Assert.assertEquals((String) r[3], input[3]);
      // binary cast
      Assert.assertEquals((byte[]) r[4], input[4]);
      // null
      Assert.assertEquals(r[5], input[5]);
      // Boolean cast
      Assert.assertEquals((Boolean) r[6], input[6]);
      // array cast
      Object[] r7array = (Object[]) r[7];
      int[] input7array = (int[]) input[7];
      Assert.assertEquals(((Number) r7array[0]).intValue(), input7array[0]);
      Assert.assertEquals(((Number) r7array[1]).intValue(), input7array[1]);
    }
    // Unsupported types, all Java specific types, e.g. List / Map...
    {
      Assert.expectThrows(Exception.class, () -> {
        List<Integer> input = Arrays.asList(1, 2);
        ObjectRef<List<Integer>> res = Ray.task(
            PyFunction.of(PYTHON_MODULE, "py_return_input",
                (Class<List<Integer>>) input.getClass()), input).remote();
        List<Integer> r = res.get();
        Assert.assertEquals(r, input);
      });
    }
  }

  @Test
  public void testPythonCallJavaFunction() {
    ObjectRef<String> res = Ray.task(PyFunction.of(
        PYTHON_MODULE, "py_func_call_java_function", String.class)).remote();
    Assert.assertEquals(res.get(), "success");
  }

  @Test
  public void testCallingPythonActor() {
    PyActorHandle actor = Ray.actor(
        PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes()).remote();
    ObjectRef<byte[]> res = actor.task(
        PyActorMethod.of("increase", byte[].class),
        "1".getBytes()).remote();
    Assert.assertEquals(res.get(), "2".getBytes());
  }

  @Test
  public void testPythonCallJavaActor() {
    ObjectRef<byte[]> res = Ray.task(
        PyFunction.of(PYTHON_MODULE, "py_func_call_java_actor", byte[].class),
        "1".getBytes()).remote();
    Assert.assertEquals(res.get(), "Counter1".getBytes());

  }

  @Test
  public void testPassActorHandleFromPythonToJava() {
    // Call a python function which creates a python actor
    // and pass the actor handle to callPythonActorHandle.
    ObjectRef<byte[]> res = Ray.task(PyFunction.of(
        PYTHON_MODULE, "py_func_pass_python_actor_handle", byte[].class)).remote();
    Assert.assertEquals(res.get(), "3".getBytes());
  }

  @Test
  public void testPassActorHandleFromJavaToPython() {
    // Create a java actor, and pass actor handle to python.
    ActorHandle<TestActor> javaActor = Ray.actor(TestActor::new, "1".getBytes()).remote();
    Preconditions.checkState(javaActor instanceof NativeActorHandle);
    byte[] actorHandleBytes = ((NativeActorHandle) javaActor).toBytes();
    ObjectRef<byte[]> res = Ray.task(
        PyFunction.of(PYTHON_MODULE,
            "py_func_call_java_actor_from_handle",
            byte[].class),
        actorHandleBytes).remote();
    Assert.assertEquals(res.get(), "12".getBytes());
    // Create a python actor, and pass actor handle to python.
    PyActorHandle pyActor = Ray.actor(
        PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes()).remote();
    Preconditions.checkState(pyActor instanceof NativeActorHandle);
    actorHandleBytes = ((NativeActorHandle) pyActor).toBytes();
    res = Ray.task(
        PyFunction.of(PYTHON_MODULE,
            "py_func_call_python_actor_from_handle",
            byte[].class),
        actorHandleBytes).remote();
    Assert.assertEquals(res.get(), "3".getBytes());
  }

  public static Object[] pack(int i, String s, double f, Object[] o) {
    // This function will be called from test_cross_language_invocation.py
    return new Object[]{i, s, f, o};
  }

  public static Object returnInput(Object o) {
    return o;
  }

  public static boolean returnInputBoolean(boolean b) {
    return b;
  }

  public static int returnInputInt(int i) {
    return i;
  }

  public static double returnInputDouble(double d) {
    return d;
  }

  public static String returnInputString(String s) {
    return s;
  }

  public static int[] returnInputIntArray(int[] l) {
    return l;
  }

  public static byte[] callPythonActorHandle(byte[] value) {
    // This function will be called from test_cross_language_invocation.py
    NativePyActorHandle actor = (NativePyActorHandle) NativeActorHandle.fromBytes(value);
    ObjectRef<byte[]> res = actor.task(
        PyActorMethod.of("increase", byte[].class),
        "1".getBytes()).remote();
    Assert.assertEquals(res.get(), "3".getBytes());
    return (byte[]) res.get();
  }

  public static class TestActor {

    public TestActor(byte[] v) {
      value = v;
    }

    public byte[] concat(byte[] v) {
      byte[] c = new byte[value.length + v.length];
      System.arraycopy(value, 0, c, 0, value.length);
      System.arraycopy(v, 0, c, value.length, v.length);
      return c;
    }

    private byte[] value;
  }
}
