job:
    entrypoint: python main.py

detached_job:
    entrypoint: my_module.TrainingCoordinator

class TrainingCoordinator(ray.DetachedJobDriver)
    def step(self) -> Checkpoint:
        pass
