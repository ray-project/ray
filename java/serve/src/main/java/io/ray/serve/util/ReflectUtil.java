package io.ray.serve.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Executable;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;

/** Tool class for reflection. */
public class ReflectUtil {

  /**
   * Get types of the parameters in input array, and make the types into a new array.
   *
   * @param parameters The input parameter array
   * @return Type array corresponding to the input parameter array
   */
  @SuppressWarnings("rawtypes")
  private static Class[] getParameterTypes(Object[] parameters) {
    Class[] parameterTypes = null;
    if (ArrayUtils.isEmpty(parameters)) {
      return null;
    }
    parameterTypes = new Class[parameters.length];
    for (int i = 0; i < parameters.length; i++) {
      parameterTypes[i] = parameters[i].getClass();
    }
    return parameterTypes;
  }

  /**
   * Get a constructor whose each parameter's type is the most closed to the corresponding input
   * parameter in terms of Java inheritance system, while {@link Class#getConstructor(Class...)}
   * returns the one based on class's equal.
   *
   * @param targetClass the constructor's class
   * @param parameters the input parameters
   * @return a matching constructor of the target class
   * @throws NoSuchMethodException if a matching method is not found
   */
  @SuppressWarnings("rawtypes")
  public static Constructor getConstructor(Class targetClass, Object... parameters)
      throws NoSuchMethodException {
    return reflect(
        targetClass.getConstructors(), (candidate) -> true, ".<init>", targetClass, parameters);
  }

  /**
   * Get a method whose each parameter's type is the most closed to the corresponding input
   * parameter in terms of Java inheritance system, while {@link Class#getMethod(String, Class...)}
   * returns the one based on class's equal.
   *
   * @param targetClass the constructor's class
   * @param name the specified method's name
   * @param parameters the input parameters
   * @return a matching method of the target class
   * @throws NoSuchMethodException if a matching method is not found
   */
  @SuppressWarnings("rawtypes")
  public static Method getMethod(Class targetClass, String name, Object... parameters)
      throws NoSuchMethodException {
    return reflect(
        targetClass.getMethods(),
        (candidate) -> StringUtils.equals(name, candidate.getName()),
        "." + name,
        targetClass,
        parameters);
  }

  /**
   * Get an object of {@link Executable} of the specified class according to initialization
   * parameters. The result's every parameter's type is the same as, or is a subclass or
   * subinterface of, the type of the corresponding input parameter. This method returns the result
   * whose each parameter's type is the most closed to the corresponding input parameter in terms of
   * Java inheritance system.
   *
   * @param <T> the type of result which extends {@link Executable}
   * @param candidates a set of candidates
   * @param filter the filter deciding whether to select the input candidate
   * @param message a message representing the target executable object
   * @param targetClass the constructor's class
   * @param parameters the input parameters
   * @return a matching executable of the target class
   * @throws NoSuchMethodException if a matching method is not found
   */
  @SuppressWarnings("rawtypes")
  private static <T extends Executable> T reflect(
      T[] candidates,
      Function<T, Boolean> filter,
      String message,
      Class targetClass,
      Object... parameters)
      throws NoSuchMethodException {
    Class[] parameterTypes = getParameterTypes(parameters);
    T result = null;
    for (int i = 0; i < candidates.length; i++) {
      T candidate = candidates[i];
      if (filter.apply(candidate)
          && assignable(parameterTypes, candidate.getParameterTypes())
          && (result == null
              || assignable(candidate.getParameterTypes(), result.getParameterTypes()))) {
        result = candidate;
      }
    }
    if (result == null) {
      throw new NoSuchMethodException(
          targetClass.getName() + message + argumentTypesToString(parameterTypes));
    }
    return result;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private static boolean assignable(Class[] from, Class[] to) {
    if (from == null) {
      return to == null || to.length == 0;
    }

    if (to == null) {
      return from.length == 0;
    }

    if (from.length != to.length) {
      return false;
    }

    for (int i = 0; i < from.length; i++) {
      if (!to[i].isAssignableFrom(from[i])) {
        return false;
      }
    }

    return true;
  }

  /**
   * It is copied from {@link Class#argumentTypesToString(Class[])}.
   *
   * @param argTypes array of Class object
   * @return Formatted string of the input Class array.
   */
  private static String argumentTypesToString(Class<?>[] argTypes) {
    StringBuilder buf = new StringBuilder();
    buf.append("(");
    if (argTypes != null) {
      for (int i = 0; i < argTypes.length; i++) {
        if (i > 0) {
          buf.append(", ");
        }
        Class<?> c = argTypes[i];
        buf.append((c == null) ? "null" : c.getName());
      }
    }
    buf.append(")");
    return buf.toString();
  }

  /**
   * Get a string representing the specified class's all methods.
   *
   * @param targetClass the input class
   * @return the formatted string of the specified class's all methods.
   */
  @SuppressWarnings("rawtypes")
  public static List<String> getMethodStrings(Class targetClass) {
    if (targetClass == null) {
      return null;
    }
    Method[] methods = targetClass.getMethods();
    if (methods == null || methods.length == 0) {
      return null;
    }
    List<String> methodStrings = new ArrayList<>();
    for (int i = 0; i < methods.length; i++) {
      methodStrings.add(methods[i].toString());
    }
    return methodStrings;
  }
}
