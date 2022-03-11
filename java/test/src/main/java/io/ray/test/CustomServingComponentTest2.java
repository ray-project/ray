package io.ray.test;

import com.google.protobuf.InvalidProtocolBufferException;
import io.ray.api.PyActorHandle;
import io.ray.api.Ray;
import io.ray.api.function.PyActorClass;
import io.ray.api.function.PyActorMethod;
import io.ray.runtime.actor.NativeActorHandle;
import io.ray.serve.generated.ActorNameList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Auto testing ray job driver that test the alibilities of customizing serving's components.
 */
public class CustomServingComponentTest2 {

  /** LOGGER */
  private static final Logger LOGGER = LoggerFactory.getLogger(CustomServingComponentTest2.class);

  public static void main(String[] args) throws InvalidProtocolBufferException {

    // System.setProperty("ray.run-mode", "SINGLE_PROCESS");
    Ray.init();

    PyActorHandle pyActor =
        Ray.actor(PyActorClass.of("test_cross_language_invocation", "Proxy")).remote();

    byte[] actorNameListBytes = (byte[])
      pyActor
        .task(PyActorMethod.of("read"))
        .remote()
        .get();

    System.out.println(actorNameListBytes);

    ActorNameList actorNameList = ActorNameList.parseFrom(actorNameListBytes);
    System.out.println(actorNameList);
    System.out.println(actorNameList.getNamesList());


    //ActorHandleList actorHandleList = ActorHandleList.parseFrom(actorHandleListBytes);

    //System.out.println(actorHandleList.getHandlesList().size());

    PyActorHandle proxyDeserializeActor =
      Ray.actor(PyActorClass.of("test_cross_language_invocation", "ProxyDeserialize")).remote();

    //String count = (String)proxyDeserializeActor.task(PyActorMethod.of("read"), actorHandleListBytes).remote().get();

    //NativeActorHandle counter = NativeActorHandle.fromBytes(actorHandleListBytes);

    /*System.out.println(counter);

    byte[] result = (byte[])
      ((PyActorHandle)counter)
        .task(PyActorMethod.of("increase"), 10)
        .remote()
        .get();

    System.out.println(result);
    System.out.println(new String(result));*/
  }

}
