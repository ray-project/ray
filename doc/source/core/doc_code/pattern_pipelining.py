import ray


@ray.remote
class WorkQueue:
    def __init__(self):
        self.queue = list(range(10))

    def get_work_item(self):
        if self.queue:
            return self.queue.pop(0)
        else:
            return None


@ray.remote
class WorkerWithoutPipelining:
    def __init__(self, work_queue):
        self.work_queue = work_queue

    def process(self, work_item):
        print(work_item)

    def run(self):
        while True:
            # Get work from the remote queue.
            work_item = ray.get(self.work_queue.get_work_item.remote())

            if work_item is None:
                break

            # Do work.
            self.process(work_item)


@ray.remote
class WorkerWithPipelining:
    def __init__(self, work_queue):
        self.work_queue = work_queue

    def process(self, work_item):
        print(work_item)

    def run(self):
        self.work_item_ref = self.work_queue.get_work_item.remote()

        while True:
            # Get work from the remote queue.
            work_item = ray.get(self.work_item_ref)

            if work_item is None:
                break

            self.work_item_ref = self.work_queue.get_work_item.remote()

            # Do work while we are fetching the next work item.
            self.process(work_item)


work_queue = WorkQueue.remote()
worker_without_pipelining = WorkerWithoutPipelining.remote(work_queue)
ray.get(worker_without_pipelining.run.remote())

work_queue = WorkQueue.remote()
worker_with_pipelining = WorkerWithPipelining.remote(work_queue)
ray.get(worker_with_pipelining.run.remote())
