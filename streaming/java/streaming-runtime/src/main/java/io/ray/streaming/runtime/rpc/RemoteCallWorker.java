package io.ray.streaming.runtime.rpc;

import io.ray.api.ActorHandle;
import io.ray.api.BaseActorHandle;
import io.ray.api.ObjectRef;
import io.ray.api.PyActorHandle;
import io.ray.api.function.PyActorMethod;
import io.ray.streaming.runtime.master.JobMaster;
import io.ray.streaming.runtime.worker.JobWorker;
import io.ray.streaming.runtime.worker.context.JobWorkerContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ray call worker.
 * It takes the communication job from {@link JobMaster} to {@link JobWorker}.
 */
public class RemoteCallWorker {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteCallWorker.class);

  /**
   * Call JobWorker actor to init.
   *
   * @param actor target JobWorker actor
   * @param context JobWorker's context
   * @return init result
   */
  public static ObjectRef<Boolean> initWorker(BaseActorHandle actor, JobWorkerContext context) {
    LOG.info("Call worker to initiate, actor: {}, context: {}.", actor.getId(), context);
    ObjectRef<Boolean> result;

    // python
    if (actor instanceof PyActorHandle) {
      result = ((PyActorHandle) actor).task(PyActorMethod.of("init", Boolean.class),
          context.getPythonWorkerContextBytes()).remote();
    } else {
      // java
      result = ((ActorHandle<JobWorker>) actor).task(JobWorker::init, context).remote();
    }

    LOG.info("Finished calling worker to initiate.");
    return result;
  }

  /**
   * Call JobWorker actor to start.
   *
   * @param actor target JobWorker actor
   * @return start result
   */
  public static ObjectRef<Boolean> startWorker(BaseActorHandle actor) {
    LOG.info("Call worker to start, actor: {}.", actor.getId());
    ObjectRef<Boolean> result = null;

    // python
    if (actor instanceof PyActorHandle) {
      result = ((PyActorHandle) actor)
          .task(PyActorMethod.of("start", Boolean.class)).remote();
    } else {
      // java
      result = ((ActorHandle<JobWorker>) actor).task(JobWorker::start).remote();
    }

    LOG.info("Finished calling worker to start.");
    return result;
  }

  /**
   * Call JobWorker actor to destroy without reconstruction.
   *
   * @param actor target JobWorker actor
   * @return destroy result
   */
  public static Boolean shutdownWithoutReconstruction(BaseActorHandle actor) {
    LOG.info("Call worker to shutdown without reconstruction, actor is {}.",
        actor.getId());
    Boolean result = false;

    // TODO (datayjz): ray call worker to destroy

    LOG.info("Finished calling wk shutdownWithoutReconstruction, result is {}.", result);
    return result;
  }

}
