package org.ray.runtime.functionmanager;

import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.objectweb.asm.Type;
import org.ray.api.function.RayFunc;
import org.ray.api.id.UniqueId;
import org.ray.runtime.util.LambdaUtils;

/**
 * Manages functions by driver id.
 */
public class FunctionManager {

  static final String CONSTRUCTOR_NAME = "<init>";

  /**
   * Cache from a RayFunc object to its corresponding FunctionDescriptor. Because
   * `LambdaUtils.getSerializedLambda` is expensive.
   */
  private static final ThreadLocal<WeakHashMap<Class<RayFunc>, FunctionDescriptor>>
      RAY_FUNC_CACHE = ThreadLocal.withInitial(WeakHashMap::new);

  /**
   * Mapping from the driver id to the functions that belong to this driver.
   */
  private Map<UniqueId, DriverFunctionTable> driverFunctionTables = new HashMap<>();

  /**
   * Get the RayFunction from a RayFunc instance (a lambda).
   *
   * @param driverId current driver id.
   * @param func The lambda.
   * @return A RayFunction object.
   */
  public RayFunction getFunction(UniqueId driverId, RayFunc func) {
    FunctionDescriptor functionDescriptor = RAY_FUNC_CACHE.get().get(func.getClass());
    if (functionDescriptor == null) {
      SerializedLambda serializedLambda = LambdaUtils.getSerializedLambda(func);
      final String className = serializedLambda.getImplClass().replace('/', '.');
      final String methodName = serializedLambda.getImplMethodName();
      final String typeDescriptor = serializedLambda.getImplMethodSignature();
      functionDescriptor = new FunctionDescriptor(className, methodName, typeDescriptor);
    }
    return getFunction(driverId, functionDescriptor);
  }

  /**
   * Get the RayFunction from a function descriptor.
   *
   * @param driverId Current driver id.
   * @param functionDescriptor The function descriptor.
   * @return A RayFunction object.
   */
  public RayFunction getFunction(UniqueId driverId, FunctionDescriptor functionDescriptor) {
    DriverFunctionTable driverFunctionTable = driverFunctionTables.get(driverId);
    if (driverFunctionTable == null) {
      //TODO(hchen): distinguish class loader by driver id.
      ClassLoader classLoader = getClass().getClassLoader();
      driverFunctionTable = new DriverFunctionTable(classLoader);
      driverFunctionTables.put(driverId, driverFunctionTable);
    }
    return driverFunctionTable.getFunction(functionDescriptor);
  }

  /**
   * Manages all functions that belong to one driver.
   */
  static class DriverFunctionTable {

    /**
     * The driver's corresponding class loader.
     */
    ClassLoader classLoader;
    /**
     * Functions per class, per function name + type descriptor.
     */
    Map<String, Map<Pair<String, String>, RayFunction>> functions;

    DriverFunctionTable(ClassLoader classLoader) {
      this.classLoader = classLoader;
      this.functions = new HashMap<>();
    }

    RayFunction getFunction(FunctionDescriptor descriptor) {
      Map<Pair<String, String>, RayFunction> classFunctions = functions.get(descriptor.className);
      if (classFunctions == null) {
        classFunctions = loadFunctionsForClass(descriptor.className);
        functions.put(descriptor.className, classFunctions);
      }
      return classFunctions.get(ImmutablePair.of(descriptor.name, descriptor.typeDescriptor));
    }

    /**
     * Load all functions from a class.
     */
    Map<Pair<String, String>, RayFunction> loadFunctionsForClass(String className) {
      Map<Pair<String, String>, RayFunction> map = new HashMap<>();
      try {
        Class clazz = Class.forName(className, true, classLoader);

        List<Executable> executables = new ArrayList<>();
        executables.addAll(Arrays.asList(clazz.getDeclaredMethods()));
        executables.addAll(Arrays.asList(clazz.getConstructors()));

        for (Executable e : executables) {
          e.setAccessible(true);
          final String methodName = e instanceof Method ? e.getName() : CONSTRUCTOR_NAME;
          final Type type =
              e instanceof Method ? Type.getType((Method) e) : Type.getType((Constructor) e);
          final String typeDescriptor = type.getDescriptor();
          RayFunction rayFunction = new RayFunction(e, classLoader,
              new FunctionDescriptor(className, methodName, typeDescriptor));
          map.put(ImmutablePair.of(methodName, typeDescriptor), rayFunction);
        }
      } catch (Exception e) {
        throw new RuntimeException("Failed to load functions from class " + className, e);
      }
      return map;
    }
  }
}
