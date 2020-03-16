package org.ray.streaming.runtime.transfer;

import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.ray.api.RayActor;
import org.ray.api.id.ActorId;
import org.ray.runtime.actor.LocalModeRayActor;
import org.ray.runtime.actor.NativeRayJavaActor;
import org.ray.runtime.actor.NativeRayPyActor;
import org.ray.runtime.functionmanager.FunctionDescriptor;
import org.ray.runtime.functionmanager.JavaFunctionDescriptor;
import org.ray.runtime.functionmanager.PyFunctionDescriptor;
import org.ray.streaming.runtime.worker.JobWorker;

/**
 * Save initial parameters needed by a streaming channel.
 */
public class ChannelInitialParameters {

  public class Parameter {

    private ActorId actorId;
    private FunctionDescriptor asyncFunctionDescriptor;
    private FunctionDescriptor syncFunctionDescriptor;

    // Called from jni
    public byte[] getActorIdBytes() {
      return actorId.getBytes();
    }

    // Called from jni
    public FunctionDescriptor getAsyncFunctionDescriptor() {
      return asyncFunctionDescriptor;
    }

    // Called from jni
    public FunctionDescriptor getSyncFunctionDescriptor() {
      return syncFunctionDescriptor;
    }

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
  }

  private List<Parameter> parameters;

  // async function descriptor for a java DataReader, used by upstream queues.
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

  private static PyFunctionDescriptor pyReaderAsyncFunctionDesc = new PyFunctionDescriptor(
      "streaming.runtime.worker",
      "JobWorker", "on_reader_message");
  private static PyFunctionDescriptor pyReaderSyncFunctionDesc = new PyFunctionDescriptor(
      "streaming.runtime.worker",
      "JobWorker", "on_reader_message_sync");
  private static PyFunctionDescriptor pyWriterAsyncFunctionDesc = new PyFunctionDescriptor(
      "streaming.runtime.worker",
      "JobWorker", "on_writer_message");
  private static PyFunctionDescriptor pyWriterSyncFunctionDesc = new PyFunctionDescriptor(
      "streaming.runtime.worker",
      "JobWorker", "on_writer_message_sync");

  public ChannelInitialParameters() {
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

  public ChannelInitialParameters buildInputQueueParameters(List<String> queues,
      Map<String, RayActor> actors) {
    return buildParameters(queues, actors, javaWriterAsyncFuncDesc, javaWriterSyncFuncDesc,
        pyWriterAsyncFunctionDesc, pyWriterSyncFunctionDesc);
  }

  public ChannelInitialParameters buildOutputQueueParameters(List<String> queues,
      Map<String, RayActor> actors) {
    return buildParameters(queues, actors, javaReaderAsyncFuncDesc, javaReaderSyncFuncDesc,
        pyReaderAsyncFunctionDesc, pyReaderSyncFunctionDesc);
  }

  private ChannelInitialParameters buildParameters(List<String> queues,
      Map<String, RayActor> actors,
      JavaFunctionDescriptor javaAsyncFunctionDesc, JavaFunctionDescriptor javaSyncFunctionDesc,
      PyFunctionDescriptor pyAsyncFunctionDesc, PyFunctionDescriptor pySyncFunctionDesc
  ) {
    parameters = new ArrayList<>(queues.size());
    for (String queue : queues) {
      Parameter parameter = new Parameter();
      RayActor actor = actors.get(queue);
      parameter.setActorId(actor.getId());
      if (actor == null || actor instanceof NativeRayJavaActor
          || actor instanceof LocalModeRayActor) {
        parameter.setAsyncFunctionDescriptor(javaAsyncFunctionDesc);
        parameter.setSyncFunctionDescriptor(javaSyncFunctionDesc);
      } else if (actor instanceof NativeRayPyActor) {
        parameter.setAsyncFunctionDescriptor(pyAsyncFunctionDesc);
        parameter.setSyncFunctionDescriptor(pySyncFunctionDesc);
      } else {
        Preconditions.checkArgument(false, "Invalid actor type");
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
    String str = "";
    for (Parameter param : parameters) {
      str += param.toString();
    }
    return str;
  }
}
