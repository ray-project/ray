import os
import ray


def run():
    ray.init()

    @ray.remote(runtime_env={"env_vars": {"FOO": "bar"}})
    def get_task_working_dir():
        # Check behavior of working_dir: The cwd should contain the
        # current file, which is being used as a job entrypoint script.
        assert os.path.exists("per_task_runtime_env.py")

        return ray.get_runtime_context().runtime_env.working_dir()

    driver_working_dir = ray.get_runtime_context().runtime_env.working_dir()
    task_working_dir = ray.get(get_task_working_dir.remote())
    assert driver_working_dir == task_working_dir, (
        driver_working_dir,
        task_working_dir,
    )


if __name__ == "__main__":
    run()
