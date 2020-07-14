package io.ray.streaming.runtime.transfer;

import com.google.common.base.Preconditions;
import io.ray.api.BaseActorHandle;
import io.ray.api.id.ActorId;
import io.ray.runtime.actor.LocalModeActorHandle;
import io.ray.runtime.actor.NativeJavaActorHandle;
import io.ray.runtime.actor.NativePyActorHandle;
import io.ray.runtime.functionmanager.FunctionDescriptor;
import io.ray.runtime.functionmanager.JavaFunctionDescriptor;
import io.ray.runtime.functionmanager.PyFunctionDescriptor;
import io.ray.streaming.runtime.worker.JobWorker;
import java.util.ArrayList;
import java.util.List;

/**
 * Save channel initial parameters needed by DataWriter/DataReader.
 */
public class ChannelCreationParametersBuilder {

  public static class Parameter {

    private ActorId actorId;
    private FunctionDescriptor asyncFunctionDescriptor;
    private FunctionDescriptor syncFunctionDescriptor;

    public void setActorId(ActorId actorId) {
      this.actorId = actorId;
    }

    public void setAsyncFunctionDescriptor(
        FunctionDescriptor asyncFunctionDescriptor) {
      this.asyncFunctionDescriptor = asyncFunctionDescriptor;
    }

    public void setSyncFunctionDescriptor(
        FunctionDescriptor syncFunctionDescriptor) {
      this.syncFunctionDescriptor = syncFunctionDescriptor;
    }

    public String toString() {
      String language =
          asyncFunctionDescriptor instanceof JavaFunctionDescriptor ? "Java" : "Python";
      return "Language: " + language + " Desc: " + asyncFunctionDescriptor.toList() + " "
        + syncFunctionDescriptor.toList();
    }

    // Get actor id in bytes, called from jni.
    public byte[] getActorIdBytes() {
      return actorId.getBytes();
    }

    // Get async function descriptor, called from jni.
    public FunctionDescriptor getAsyncFunctionDescriptor() {
      return asyncFunctionDescriptor;
    }

    // Get sync function descriptor, called from jni.
    public FunctionDescriptor getSyncFunctionDescriptor() {
      return syncFunctionDescriptor;
    }
  }

  private List<Parameter> parameters;

  // function descriptors of direct call entry point for Java workers
  private static JavaFunctionDescriptor javaReaderAsyncFuncDesc = new JavaFunctionDescriptor(
      JobWorker.class.getName(),
      "onReaderMessage", "([B)V");
  private static JavaFunctionDescriptor javaReaderSyncFuncDesc = new JavaFunctionDescriptor(
      JobWorker.class.getName(),
      "onReaderMessageSync", "([B)[B");
  private static JavaFunctionDescriptor javaWriterAsyncFuncDesc = new JavaFunctionDescriptor(
      JobWorker.class.getName(),
      "onWriterMessage", "([B)V");
  private static JavaFunctionDescriptor javaWriterSyncFuncDesc = new JavaFunctionDescriptor(
      JobWorker.class.getName(),
      "onWriterMessageSync", "([B)[B");
  // function descriptors of direct call entry point for Python workers
  private static PyFunctionDescriptor pyReaderAsyncFunctionDesc = new PyFunctionDescriptor(
      "ray.streaming.runtime.worker",
      "JobWorker", "on_reader_message");
  private static PyFunctionDescriptor pyReaderSyncFunctionDesc = new PyFunctionDescriptor(
      "ray.streaming.runtime.worker",
      "JobWorker", "on_reader_message_sync");
  private static PyFunctionDescriptor pyWriterAsyncFunctionDesc = new PyFunctionDescriptor(
      "ray.streaming.runtime.worker",
      "JobWorker", "on_writer_message");
  private static PyFunctionDescriptor pyWriterSyncFunctionDesc = new PyFunctionDescriptor(
      "ray.streaming.runtime.worker",
      "JobWorker", "on_writer_message_sync");

  public ChannelCreationParametersBuilder() {
  }

  public static void setJavaReaderFunctionDesc(JavaFunctionDescriptor asyncFunc,
                                               JavaFunctionDescriptor syncFunc) {
    javaReaderAsyncFuncDesc = asyncFunc;
    javaReaderSyncFuncDesc = syncFunc;
  }

  public static void setJavaWriterFunctionDesc(JavaFunctionDescriptor asyncFunc,
                                               JavaFunctionDescriptor syncFunc) {
    javaWriterAsyncFuncDesc = asyncFunc;
    javaWriterSyncFuncDesc = syncFunc;
  }

  public ChannelCreationParametersBuilder buildInputQueueParameters(
      List<String> queues,
      List<BaseActorHandle> actors) {
    return buildParameters(queues, actors, javaWriterAsyncFuncDesc, javaWriterSyncFuncDesc,
      pyWriterAsyncFunctionDesc, pyWriterSyncFunctionDesc);
  }

  public ChannelCreationParametersBuilder buildOutputQueueParameters(List<String> queues,
                                                                     List<BaseActorHandle> actors) {
    return buildParameters(queues, actors, javaReaderAsyncFuncDesc, javaReaderSyncFuncDesc,
      pyReaderAsyncFunctionDesc, pyReaderSyncFunctionDesc);
  }

  private ChannelCreationParametersBuilder buildParameters(List<String> queues,
      List<BaseActorHandle> actors,
      JavaFunctionDescriptor javaAsyncFunctionDesc, JavaFunctionDescriptor javaSyncFunctionDesc,
      PyFunctionDescriptor pyAsyncFunctionDesc, PyFunctionDescriptor pySyncFunctionDesc
  ) {
    parameters = new ArrayList<>(queues.size());

    for (int i = 0; i < queues.size(); ++i) {
      String queue = queues.get(i);
      BaseActorHandle actor = actors.get(i);
      Parameter parameter = new Parameter();
      Preconditions.checkArgument(actor != null);
      parameter.setActorId(actor.getId());
      /// LocalModeRayActor used in single-process mode.
      if (actor instanceof NativeJavaActorHandle || actor instanceof LocalModeActorHandle) {
        parameter.setAsyncFunctionDescriptor(javaAsyncFunctionDesc);
        parameter.setSyncFunctionDescriptor(javaSyncFunctionDesc);
      } else if (actor instanceof NativePyActorHandle) {
        parameter.setAsyncFunctionDescriptor(pyAsyncFunctionDesc);
        parameter.setSyncFunctionDescriptor(pySyncFunctionDesc);
      } else {
        throw new IllegalArgumentException("Invalid actor type");
      }
      parameters.add(parameter);
    }

    return this;
  }

  // Called from jni
  public List<Parameter> getParameters() {
    return parameters;
  }

  public String toString() {
    StringBuilder str = new StringBuilder();
    for (Parameter param : parameters) {
      str.append(param.toString());
    }
    return str.toString();
  }
}
