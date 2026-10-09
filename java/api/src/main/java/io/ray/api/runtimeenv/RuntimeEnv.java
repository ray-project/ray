package io.ray.api.runtimeenv;

import io.ray.api.Ray;
import io.ray.api.exception.RuntimeEnvException;
import io.ray.api.runtimeenv.types.RuntimeEnvName;

/** This class provides interfaces of setting runtime environments for job/actor/task. */
public interface RuntimeEnv {

  /**
   * Set a runtime env field by name and Object.
   *
   * @param name The build-in names or a runtime env plugin name.
   * @see RuntimeEnvName
   * @param value An object with primitive data type or plain old java object(POJO).
   * @throws RuntimeEnvException
   */
  void set(String name, Object value) throws RuntimeEnvException;

  /**
   * Set a runtime env field by name and json string.
   *
   * @param name The build-in names or a runtime env plugin name.
   * @see RuntimeEnvName
   * @param jsonStr A json string represents the runtime env field.
   * @throws RuntimeEnvException
   */
  public void setJsonStr(String name, String jsonStr) throws RuntimeEnvException;

  /**
   * Get the object of a runtime env field.
   *
   * @param name The build-in names or a runtime env plugin name.
   * @param classOfT The class of a primitive data type or plain old java object(POJO) type.
   * @return
   * @param <T> A primitive data type or plain old java object(POJO) type.
   * @throws RuntimeEnvException
   */
  public <T> T get(String name, Class<T> classOfT) throws RuntimeEnvException;

  /**
   * Get the json string of a runtime env field.
   *
   * @param name The build-in names or a runtime env plugin name.
   * @return A json string represents the runtime env field.
   * @throws RuntimeEnvException
   */
  public String getJsonStr(String name) throws RuntimeEnvException;

  /**
   * Whether a field is contained.
   *
   * @param name The runtime env plugin name.
   * @return
   */
  boolean contains(String name);

  /**
   * Remove a runtime env field by name.
   *
   * @param name The build-in names or a runtime env plugin name.
   * @return true if remove an existing field, otherwise false.
   * @throws RuntimeEnvException
   */
  public boolean remove(String name) throws RuntimeEnvException;

  /**
   * Serialize the runtime env to string.
   *
   * @return The serialized runtime env string.
   * @throws RuntimeEnvException
   */
  public String serialize() throws RuntimeEnvException;

  /**
   * Whether the runtime env is empty.
   *
   * @return
   */
  boolean isEmpty();

  /**
   * Serialize the runtime env to string of RuntimeEnvInfo.
   *
   * @return The serialized runtime env info string.
   * @throws RuntimeEnvException
   */
  public String serializeToRuntimeEnvInfo() throws RuntimeEnvException;

  /**
   * Deserialize the runtime env from string.
   *
   * @param serializedRuntimeEnv The serialized runtime env string.
   * @return The deserialized RuntimeEnv instance.
   * @throws RuntimeEnvException
   */
  public static RuntimeEnv deserialize(String serializedRuntimeEnv) throws RuntimeEnvException {
    return Ray.internal().deserializeRuntimeEnv(serializedRuntimeEnv);
  }

  /**
   * Set runtime env config.
   *
   * @param runtimeEnvConfig
   */
  public void setConfig(RuntimeEnvConfig runtimeEnvConfig);

  /**
   * Get runtime env config.
   *
   * @return The runtime env config.
   */
  public RuntimeEnvConfig getConfig();

  /** The builder which is used to generate a RuntimeEnv instance. */
  public static class Builder {
    public RuntimeEnv build() {
      return Ray.internal().createRuntimeEnv();
    }
  }
}
