package org.ray.hook;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.codec.digest.DigestUtils;
import org.objectweb.asm.Type;
import org.ray.util.logger.RayLog;

/**
 * Represent a Method in a Class.
 */
public class MethodId {

  static final String getFunctionIdPostfix = "_function_id";
  String className;
  String methodName;
  String methodDesc;
  boolean isStatic;
  ClassLoader loader;

  public MethodId(String cls, String method, String mdesc, boolean isstatic, ClassLoader loader) {
    className = cls;
    methodName = method;
    methodDesc = mdesc;
    isStatic = isstatic;
    this.loader = loader;
  }

  public MethodId(String encodedString, ClassLoader loader) {
    // className + "." + methodName + "::" + methodDesc + "&&" + isStatic;
    int lastPos3 = encodedString.lastIndexOf("&&");
    int lastPos2 = encodedString.lastIndexOf("::");
    int lastPos1 = encodedString.lastIndexOf(".");
    if (lastPos1 == -1 || lastPos2 == -1 || lastPos3 == -1) {
      throw new RuntimeException("invalid given method id " + encodedString
          + " - it must be className.methodName::methodDesc&&isStatic");
    }

    className = encodedString.substring(0, lastPos1);
    methodName = encodedString.substring(lastPos1 + ".".length(), lastPos2);
    methodDesc = encodedString.substring(lastPos2 + "::".length(), lastPos3);
    isStatic = Boolean.parseBoolean(encodedString.substring(lastPos3 + "&&".length()));
    this.loader = loader;
  }

  public static String toHexHashString(byte[] id) {
    String s = "";
    String hex = "0123456789abcdef";
    assert (id.length == 20);
    for (int i = 0; i < 20; i++) {
      int val = id[i] & 0xff;
      s += hex.charAt(val >> 4);
      s += hex.charAt(val & 0xf);
    }
    return s;
  }

  private String toHexHashString() {
    byte[] id = this.getSha1Hash();
    return toHexHashString(id);
  }

  public String getClassName() {
    return className;
  }

  public String getMethodName() {
    return methodName;
  }

  public String getMethodDesc() {
    return methodDesc;
  }

  public ClassLoader getLoader() {
    return loader;
  }

  public Boolean isStaticMethod() {
    return isStatic;
  }

  public String getIdMethodName() {
    return this.methodName + getFunctionIdPostfix;
  }

  public String getIdMethodDesc() {
    return "(L" + this.className + ";" + this.methodDesc.substring(1);
  }

  public Method load() {
    String loadClsName = className.replace('/', '.');
    Class<?> cls;
    try {
      RayLog.core.debug(
          "load class " + loadClsName + " from class loader " + (loader == null ? this.getClass()
              .getClassLoader() : loader)
              + " for method " + toString() + " with ID = " + toHexHashString()
      );
      cls = Class
          .forName(loadClsName, true, loader == null ? this.getClass().getClassLoader() : loader);
    } catch (Throwable e) {
      RayLog.core.error("Cannot load class " + loadClsName, e);
      return null;
    }

    Method[] ms = cls.getDeclaredMethods();
    ArrayList<Method> methods = new ArrayList<>();
    Type t = Type.getMethodType(this.methodDesc);
    Type[] params = t.getArgumentTypes();
    String rt = t.getReturnType().getDescriptor();

    for (Method m : ms) {
      if (m.getName().equals(methodName)) {
        if (!Arrays.equals(params, Type.getArgumentTypes(m))) {
          continue;
        }

        String mrt = Type.getDescriptor(m.getReturnType());
        if (!rt.equals(mrt)) {
          continue;
        }

        methods.add(m);
      }
    }

    if (methods.size() != 1) {
      RayLog.core.error(
          "Load method " + toString() + " failed as there are " + methods.size() + " definitions");
      return null;
    }

    Method m = methods.get(0);
    try {
      Field fld = cls.getField(getStaticHashValueFieldName());
      Object hashValue = fld.get(null);
      if (hashValue instanceof byte[] && Arrays.equals((byte[]) hashValue, this.getSha1Hash())) {
        RayLog.core.debug("Method " + toString() + " hash: " + toHexHashString((byte[]) hashValue));
      } else {
        if (hashValue instanceof byte[]) {
          RayLog.core.error(
              "Method " + toString() + " hash-field: " + toHexHashString((byte[]) hashValue)
                  + " vs id-hash: " + toHexHashString());
        } else {
          RayLog.core.error(
              "Method " + toString() + " hash-field: " + (hashValue != null ? hashValue.toString()
                  : "<nil>") + " vs id-hash: " + toHexHashString());
        }
      }
    } catch (NoSuchFieldException | SecurityException | IllegalArgumentException
        | IllegalAccessException e) {
      RayLog.core.error("load method hash field failed for " + toString(), e);
    }
    return m;
  }

  public String toEncodingString() {
    return className + "." + methodName + "::" + methodDesc + "&&" + isStatic;
  }

  public byte[] getSha1Hash() {
    byte[] digests = DigestUtils.sha(toEncodingString());
    ByteBuffer bb = ByteBuffer.wrap(digests);
    bb.order(ByteOrder.LITTLE_ENDIAN);
    if (methodName.contains("createActorStage1")) {
      bb.putLong(Long.BYTES, 1);
    } else {
      bb.putLong(Long.BYTES, 0);
    }
    return digests;
  }

  public String getStaticHashValueFieldName() {
    // _hashOf<init>_([Ljava/lang/String;)V
    String r = "_hashOf" + methodName + "_" + methodDesc;
    r = r.replace("<", "_")
        .replace(">", "_")
        .replace("(", "_")
        .replace("[", "_")
        .replace("/", "_")
        .replace(";", "_")
        .replace(")", "_")
    ;
    // System.err.println(r);
    return r;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + className.hashCode();
    result = prime * result + methodName.hashCode();
    result = prime * result + methodDesc.hashCode();
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null) {
      return false;
    }
    if (getClass() != obj.getClass()) {
      return false;
    }
    MethodId other = (MethodId) obj;
    return className.equals(other.className)
        && methodName.equals(other.methodName)
        && methodDesc.equals(other.methodDesc)
        && isStatic == other.isStatic
        ;
  }

  @Override
  public String toString() {
    return toEncodingString();
  }
}
