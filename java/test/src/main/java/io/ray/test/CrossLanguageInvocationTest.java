package io.ray.test;

import com.google.common.base.Preconditions;
import io.ray.api.ActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.exception.CrossLanguageException;
import io.ray.api.exception.RayException;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.api.function.PyFunction;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.runtime.serializer.RayExceptionSerializer;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import org.apache.commons.io.FileUtils;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

@Test(groups = {"cluster"})
public class CrossLanguageInvocationTest extends BaseTest {

  private static final String PYTHON_MODULE = "test_cross_language_invocation";

  @BeforeClass
  public void beforeClass() {
    // Delete and re-create the temp dir.
    File tempDir =
        new File(System.getProperty("java.io.tmpdir") + File.separator + "ray_cross_language_test");
    FileUtils.deleteQuietly(tempDir);
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    // Write the test Python file to the temp dir.
    InputStream in =
        CrossLanguageInvocationTest.class.getResourceAsStream("/" + PYTHON_MODULE + ".py");
    File pythonFile = new File(tempDir.getAbsolutePath() + File.separator + PYTHON_MODULE + ".py");
    try {
      FileUtils.copyInputStreamToFile(in, pythonFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    System.setProperty(
        "ray.job.code-search-path",
        System.getProperty("java.class.path") + File.pathSeparator + tempDir.getAbsolutePath());
  }

  @Test
  public void testCallingPythonFunction() {
    Object[] inputs =
        new Object[] {
          true, // Boolean
          Byte.MAX_VALUE, // Byte
          Short.MAX_VALUE, // Short
          Integer.MAX_VALUE, // Integer
          Long.MAX_VALUE, // Long
          // BigInteger can support max value of 2^64-1, please refer to:
          // https://github.com/msgpack/msgpack/blob/master/spec.md#int-format-family
          // If BigInteger larger than 2^64-1, the value can only be transferred among Java workers.
          BigInteger.valueOf(Long.MAX_VALUE), // BigInteger
          "Hello World!", // String
          1.234f, // Float
          1.234, // Double
          "example binary".getBytes()
        }; // byte[]
    for (Object o : inputs) {
      ObjectRef res =
          Ray.task(PyFunction.of(PYTHON_MODULE, "py_return_input", o.getClass()), o).remote();
      Assert.assertEquals(res.get(), o);
    }
    // null
    {
      Object input = null;
      ObjectRef<Object> res =
          Ray.task(PyFunction.of(PYTHON_MODULE, "py_return_input", Object.class), input).remote();
      Object r = res.get();
      Assert.assertEquals(r, input);
    }
    // array
    {
      int[] input = new int[] {1, 2};
      ObjectRef<int[]> res =
          Ray.task(PyFunction.of(PYTHON_MODULE, "py_return_input", int[].class), input).remote();
      int[] r = res.get();
      Assert.assertEquals(r, input);
    }
    // array of Object
    {
      Object[] input =
          new Object[] {1, 2.3f, 4.56, "789", "10".getBytes(), null, true, new int[] {1, 2}};
      ObjectRef<Object[]> res =
          Ray.task(PyFunction.of(PYTHON_MODULE, "py_return_input", Object[].class), input).remote();
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
      Assert.expectThrows(
          Exception.class,
          () -> {
            List<Integer> input = Arrays.asList(1, 2);
            ObjectRef<List<Integer>> res =
                Ray.task(
                        PyFunction.of(
                            PYTHON_MODULE,
                            "py_return_input",
                            (Class<List<Integer>>) input.getClass()),
                        input)
                    .remote();
            List<Integer> r = res.get();
            Assert.assertEquals(r, input);
          });
    }
  }

  @Test
  public void testPythonCallJavaFunction() {
    ObjectRef<String> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_call_java_function", String.class)).remote();
    Assert.assertEquals(res.get(), "success");
  }

  @Test
  public void testCallingPythonActor() {
    PyActorHandle actor =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes()).remote();
    ObjectRef<byte[]> res =
        actor.task(PyActorMethod.of("increase", byte[].class), "1".getBytes()).remote();
    Assert.assertEquals(res.get(), "2".getBytes());
  }

  @Test
  public void testPythonCallJavaActor() {
    ObjectRef<byte[]> res =
        Ray.task(
                PyFunction.of(PYTHON_MODULE, "py_func_call_java_actor", byte[].class),
                "1".getBytes())
            .remote();
    Assert.assertEquals(res.get(), "Counter1".getBytes());
  }

  @Test
  public void testPassActorHandleFromPythonToJava() {
    // Call a python function which creates a python actor
    // and pass the actor handle to callPythonActorHandle.
    ObjectRef<byte[]> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_pass_python_actor_handle", byte[].class))
            .remote();
    Assert.assertEquals(res.get(), "3".getBytes());
  }

  @Test
  public void testPassActorHandleFromJavaToPython() {
    // Create a java actor, and pass actor handle to python.
    ActorHandle<TestActor> javaActor = Ray.actor(TestActor::new, "1".getBytes()).remote();
    Preconditions.checkState(javaActor instanceof NativeActorHandle);
    ObjectRef<byte[]> res =
        Ray.task(
                PyFunction.of(PYTHON_MODULE, "py_func_call_java_actor_from_handle", byte[].class),
                javaActor)
            .remote();
    Assert.assertEquals(res.get(), "12".getBytes());
    // Create a python actor, and pass actor handle to python.
    PyActorHandle pyActor =
        Ray.actor(PyActorClass.of(PYTHON_MODULE, "Counter"), "1".getBytes()).remote();
    Preconditions.checkState(pyActor instanceof NativeActorHandle);
    res =
        Ray.task(
                PyFunction.of(PYTHON_MODULE, "py_func_call_python_actor_from_handle", byte[].class),
                pyActor)
            .remote();
    Assert.assertEquals(res.get(), "3".getBytes());
  }

  @Test
  public void testExceptionSerialization() throws IOException {
    try {
      throw new RayException("Test Exception");
    } catch (RayException e) {
      String formattedException =
          org.apache.commons.lang3.exception.ExceptionUtils.getStackTrace(e);
      io.ray.runtime.generated.Common.RayException exception =
          io.ray.runtime.generated.Common.RayException.parseFrom(RayExceptionSerializer.toBytes(e));
      Assert.assertEquals(exception.getFormattedExceptionString(), formattedException);
    }
  }

  @Test
  public void testRaiseExceptionFromPython() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_python_raise_exception", Object.class))
            .remote();
    try {
      res.get();
    } catch (RuntimeException ex) {
      // ex is a Python exception(py_func_python_raise_exception) with no cause.
      Assert.assertTrue(ex instanceof CrossLanguageException);
      CrossLanguageException e = (CrossLanguageException) ex;
      // ex.cause is null.
      Assert.assertNull(ex.getCause());
      Assert.assertTrue(
          ex.getMessage().contains("ZeroDivisionError: division by zero"), ex.getMessage());
      return;
    }
    Assert.fail();
  }

  @Test
  public void testThrowExceptionFromJava() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_java_throw_exception", Object.class))
            .remote();
    try {
      res.get();
    } catch (RuntimeException ex) {
      final String message = ex.getMessage();
      Assert.assertTrue(message.contains("py_func_java_throw_exception"), message);
      Assert.assertTrue(
          message.contains("io.ray.test.CrossLanguageInvocationTest.throwException"), message);
      Assert.assertTrue(message.contains("java.lang.ArithmeticException: / by zero"), message);
      return;
    }
    Assert.fail();
  }

  @Test
  public void testRaiseExceptionFromNestPython() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_nest_python_raise_exception", Object.class))
            .remote();
    try {
      res.get();
    } catch (RuntimeException ex) {
      final String message = ex.getMessage();
      Assert.assertTrue(message.contains("py_func_nest_python_raise_exception"), message);
      Assert.assertTrue(message.contains("io.ray.runtime.task.TaskExecutor.execute"), message);
      Assert.assertTrue(message.contains("py_func_python_raise_exception"), message);
      Assert.assertTrue(message.contains("ZeroDivisionError: division by zero"), message);
      return;
    }
    Assert.fail();
  }

  @Test
  public void testThrowExceptionFromNestJava() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_nest_java_throw_exception", Object.class))
            .remote();
    try {
      res.get();
    } catch (RuntimeException ex) {
      final String message = ex.getMessage();
      Assert.assertTrue(message.contains("py_func_nest_java_throw_exception"), message);
      Assert.assertEquals(
          org.apache.commons.lang3.StringUtils.countMatches(
              message, "io.ray.api.exception.RayTaskException"),
          2);
      Assert.assertTrue(message.contains("py_func_java_throw_exception"), message);
      Assert.assertTrue(message.contains("java.lang.ArithmeticException: / by zero"), message);
      return;
    }
    Assert.fail();
  }

  @Test
  public void testCallingPythonNamedActor() {
    /// 1. create Python named actor.
    /// 2. get and invoke it in Java.
    byte[] res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_create_named_actor", byte[].class))
            .remote()
            .get();
    Assert.assertEquals(res, "true".getBytes(StandardCharsets.UTF_8));
    PyActorHandle pyActor = (PyActorHandle) Ray.getActor("py_named_actor").get();

    ObjectRef<byte[]> obj =
        pyActor.task(PyActorMethod.of("increase", byte[].class), "1".getBytes()).remote();
    Assert.assertEquals(obj.get(), "102".getBytes());
  }

  @Test
  public void testCallingJavaNamedActor() {
    /// 1. create Java named actor.
    /// 2. get and invoke it in Python.
    ActorHandle<TestActor> actor =
        Ray.actor(TestActor::new, "hello".getBytes(StandardCharsets.UTF_8))
            .setName("java_named_actor")
            .remote();
    Assert.assertEquals(
        actor.task(TestActor::getValue).remote().get(), "hello".getBytes(StandardCharsets.UTF_8));

    byte[] res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_get_and_invoke_named_actor", byte[].class))
            .remote()
            .get();
    Assert.assertEquals(res, "true".getBytes(StandardCharsets.UTF_8));
  }

  public static Object[] pack(int i, String s, double f, Object[] o) {
    // This function will be called from test_cross_language_invocation.py
    return new Object[] {i, s, f, o};
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

  public static byte[] callPythonActorHandle(PyActorHandle actor) {
    // This function will be called from test_cross_language_invocation.py
    ObjectRef<byte[]> res =
        actor.task(PyActorMethod.of("increase", byte[].class), "1".getBytes()).remote();
    Assert.assertEquals(res.get(), "3".getBytes());
    return (byte[]) res.get();
  }

  @SuppressWarnings("ConstantOverflow")
  public static Object throwException() {
    return 1 / 0;
  }

  public static Object throwJavaException() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_java_throw_exception", Object.class))
            .remote();
    return res.get();
  }

  public static Object raisePythonException() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_python_raise_exception", Object.class))
            .remote();
    return res.get();
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

    public byte[] getValue() {
      return value;
    }

    private byte[] value;
  }

  public void testPyCallJavaOeveridedMethodWithDefault() {
    ObjectRef<Object> res =
        Ray.task(
                PyFunction.of(
                    PYTHON_MODULE,
                    "py_func_call_java_overrided_method_with_default_keyword",
                    Object.class))
            .remote();
    Assert.assertEquals("hi", res.get());
  }

  public void testPyCallJavaOverloadedMethodByParameterSize() {
    ObjectRef<Object> res =
        Ray.task(PyFunction.of(PYTHON_MODULE, "py_func_call_java_overloaded_method", Object.class))
            .remote();
    Assert.assertEquals(true, res.get());
  }
}
