package io.ray.streaming.runtime.python;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Primitives;
import io.ray.streaming.api.context.StreamingContext;
import io.ray.streaming.api.stream.DataStream;
import io.ray.streaming.api.stream.Stream;
import io.ray.streaming.python.PythonFunction;
import io.ray.streaming.python.PythonPartition;
import io.ray.streaming.python.stream.PythonDataStream;
import io.ray.streaming.python.stream.PythonStreamSource;
import io.ray.streaming.runtime.serialization.MsgPackSerializer;
import io.ray.streaming.runtime.util.ReflectionUtils;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Gateway for streaming python api.
 * All calls on DataStream in python will be mapped to DataStream call in java by this
 * PythonGateway using ray calls.
 * <p>
 * Note: this class needs to be in sync with `GatewayClient` in
 * `streaming/python/runtime/gateway_client.py`
 */
@SuppressWarnings("unchecked")
public class PythonGateway {
  private static final Logger LOG = LoggerFactory.getLogger(PythonGateway.class);
  private static final String REFERENCE_ID_PREFIX = "__gateway_reference_id__";
  private static MsgPackSerializer serializer = new MsgPackSerializer();

  private Map<String, Object> referenceMap;
  private StreamingContext streamingContext;

  public PythonGateway() {
    referenceMap = new HashMap<>();
    LOG.info("PythonGateway created");
  }

  public byte[] createStreamingContext() {
    streamingContext = StreamingContext.buildContext();
    LOG.info("StreamingContext created");
    referenceMap.put(getReferenceId(streamingContext), streamingContext);
    return serializer.serialize(getReferenceId(streamingContext));
  }

  public StreamingContext getStreamingContext() {
    return streamingContext;
  }

  public byte[] withConfig(byte[] confBytes) {
    Preconditions.checkNotNull(streamingContext);
    try {
      Map<String, String> config = (Map<String, String>) serializer.deserialize(confBytes);
      LOG.info("Set config {}", config);
      streamingContext.withConfig(config);
      // We can't use `return void`, that will make `ray.get()` hang forever.
      // We can't using `return new byte[0]`, that will make `ray::CoreWorker::ExecuteTask` crash.
      // So we `return new byte[1]` for method execution success.
      // Same for other methods in this class which return new byte[1].
      return new byte[1];
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] createPythonStreamSource(byte[] pySourceFunc) {
    Preconditions.checkNotNull(streamingContext);
    try {
      PythonStreamSource pythonStreamSource = PythonStreamSource.from(
          streamingContext, new PythonFunction(pySourceFunc));
      referenceMap.put(getReferenceId(pythonStreamSource), pythonStreamSource);
      return serializer.serialize(getReferenceId(pythonStreamSource));
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] execute(byte[] jobNameBytes) {
    LOG.info("Starting executing");
    streamingContext.execute((String) serializer.deserialize(jobNameBytes));
    // see `withConfig` method.
    return new byte[1];
  }

  public byte[] createPyFunc(byte[] pyFunc) {
    PythonFunction function = new PythonFunction(pyFunc);
    referenceMap.put(getReferenceId(function), function);
    return serializer.serialize(getReferenceId(function));
  }

  public byte[] createPyPartition(byte[] pyPartition) {
    PythonPartition partition = new PythonPartition(pyPartition);
    referenceMap.put(getReferenceId(partition), partition);
    return serializer.serialize(getReferenceId(partition));
  }

  public byte[] union(byte[] paramsBytes) {
    List<Object> streams = (List<Object>) serializer.deserialize(paramsBytes);
    streams = processParameters(streams);
    LOG.info("Call union with streams {}", streams);
    Preconditions.checkArgument(streams.size() >= 2,
        "Union needs at least two streams");
    Stream unionStream;
    Stream stream1 = (Stream) streams.get(0);
    List otherStreams = streams.subList(1, streams.size());
    if (stream1 instanceof DataStream) {
      DataStream dataStream = (DataStream) stream1;
      unionStream = dataStream.union(otherStreams);
    } else {
      Preconditions.checkArgument(stream1 instanceof PythonDataStream);
      PythonDataStream pythonDataStream = (PythonDataStream) stream1;
      unionStream = pythonDataStream.union(otherStreams);
    }
    return serialize(unionStream);
  }

  public byte[] callFunction(byte[] paramsBytes) {
    try {
      List<Object> params = (List<Object>) serializer.deserialize(paramsBytes);
      params = processParameters(params);
      LOG.info("callFunction params {}", params);
      String className = (String) params.get(0);
      String funcName = (String) params.get(1);
      Class<?> clz = Class.forName(className, true, this.getClass().getClassLoader());
      Class[] paramsTypes = params.subList(2, params.size()).stream()
          .map(Object::getClass).toArray(Class[]::new);
      Method method = findMethod(clz, funcName, paramsTypes);
      Object result = method.invoke(null, params.subList(2, params.size()).toArray());
      return serialize(result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] callMethod(byte[] paramsBytes) {
    try {
      List<Object> params = (List<Object>) serializer.deserialize(paramsBytes);
      params = processParameters(params);
      LOG.info("callMethod params {}", params);
      Object obj = params.get(0);
      String methodName = (String) params.get(1);
      Class<?> clz = obj.getClass();
      Class[] paramsTypes = params.subList(2, params.size()).stream()
          .map(Object::getClass).toArray(Class[]::new);
      Method method = findMethod(clz, methodName, paramsTypes);
      Object result = method.invoke(obj, params.subList(2, params.size()).toArray());
      return serialize(result);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static Method findMethod(Class<?> cls, String methodName, Class[] paramsTypes) {
    List<Method> methods = ReflectionUtils.findMethods(cls, methodName);
    if (methods.size() == 1) {
      return methods.get(0);
    }
    // Convert all params types to primitive types if it's boxed type
    Class[] unwrappedTypes = Arrays.stream(paramsTypes)
        .map((Function<Class, Class>) Primitives::unwrap)
        .toArray(Class[]::new);
    Optional<Method> any = methods.stream()
        .filter(m -> {
          boolean exactMatch = Arrays.equals(m.getParameterTypes(), paramsTypes) ||
              Arrays.equals(m.getParameterTypes(), unwrappedTypes);
          if (exactMatch) {
            return true;
          } else if (paramsTypes.length == m.getParameterTypes().length) {
            for (int i = 0; i < m.getParameterTypes().length; i++) {
              Class<?> parameterType = m.getParameterTypes()[i];
              if (!parameterType.isAssignableFrom(paramsTypes[i])) {
                return false;
              }
            }
            return true;
          } else {
            return false;
          }
        })
        .findAny();
    Preconditions.checkArgument(any.isPresent(),
        String.format("Method %s with type %s doesn't exist on class %s",
            methodName, Arrays.toString(paramsTypes), cls));
    return any.get();
  }

  private byte[] serialize(Object value) {
    if (returnReference(value)) {
      referenceMap.put(getReferenceId(value), value);
      return serializer.serialize(getReferenceId(value));
    } else {
      return serializer.serialize(value);
    }
  }

  private static boolean returnReference(Object value) {
    if (isBasic(value)) {
      return false;
    } else {
      try {
        serializer.serialize(value);
        return false;
      } catch (Exception e) {
        return true;
      }
    }
  }

  private static boolean isBasic(Object value) {
    return value == null || (value instanceof Boolean) || (value instanceof Number) ||
        (value instanceof String) || (value instanceof byte[]);
  }

  public byte[] newInstance(byte[] classNameBytes) {
    String className = (String) serializer.deserialize(classNameBytes);
    try {
      Class<?> clz = Class.forName(className, true, this.getClass().getClassLoader());
      Object instance = clz.newInstance();
      referenceMap.put(getReferenceId(instance), instance);
      return serializer.serialize(getReferenceId(instance));
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new IllegalArgumentException(
          String.format("Create instance for class %s failed", className), e);
    }
  }

  private List<Object> processParameters(List<Object> params) {
    return params.stream().map(this::processParameter)
        .collect(Collectors.toList());
  }

  private Object processParameter(Object o) {
    if (o instanceof String) {
      Object value = referenceMap.get(o);
      if (value != null) {
        return value;
      }
    }
    // Since python can't represent byte/short, we convert all Byte/Short to Integer
    if (o instanceof Byte || o instanceof Short) {
      return ((Number) o).intValue();
    }
    return o;
  }

  private String getReferenceId(Object o) {
    return REFERENCE_ID_PREFIX + System.identityHashCode(o);
  }

}
