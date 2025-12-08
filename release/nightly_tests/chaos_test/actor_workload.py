import ray
from ray._common.test_utils import wait_for_condition
from ray.data._internal.progress_bar import ProgressBar
from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy


def run_actor_workload(total_num_cpus, smoke):
    """Run actor-based workload.

    The test checks if actor restart -1 and task_retries -1 works
    as expected. It basically requires many actors to report the
    seqno to the centralized DB actor while there are failures.
    If at least once is guaranteed upon failures, this test
    shouldn't fail.
    """

    @ray.remote(num_cpus=0, max_task_retries=-1)
    class DBActor:
        def __init__(self):
            self.letter_dict = set()

        def add(self, letter):
            self.letter_dict.add(letter)

        def get(self):
            return self.letter_dict

    @ray.remote(num_cpus=1, max_restarts=-1, max_task_retries=-1)
    class ReportActor:
        def __init__(self, db_actor):
            self.db_actor = db_actor

        def add(self, letter):
            ray.get(self.db_actor.add.remote(letter))

    NUM_CPUS = int(total_num_cpus)
    multiplier = 2
    # For smoke mode, run fewer tasks
    if smoke:
        multiplier = 1
    TOTAL_TASKS = int(300 * multiplier)
    head_node_id = ray.get_runtime_context().get_node_id()
    db_actors = [
        DBActor.options(
            scheduling_strategy=NodeAffinitySchedulingStrategy(
                node_id=head_node_id, soft=False
            )
        ).remote()
        for _ in range(NUM_CPUS)
    ]

    pb = ProgressBar("Chaos test", TOTAL_TASKS * NUM_CPUS, "task")
    actors = []
    for db_actor in db_actors:
        actors.append(ReportActor.remote(db_actor))
    results = []
    highest_reported_num = 0
    for a in actors:
        for _ in range(TOTAL_TASKS):
            results.append(a.add.remote(str(highest_reported_num)))
            highest_reported_num += 1
    pb.fetch_until_complete(results)
    pb.close()
    for actor in actors:
        ray.kill(actor)

    # Consistency check
    wait_for_condition(
        lambda: (
            ray.cluster_resources().get("CPU", 0)
            == ray.available_resources().get("CPU", 0)
        ),
        timeout=60,
    )
    letter_set = set()
    for db_actor in db_actors:
        letter_set.update(ray.get(db_actor.get.remote()))
    # Make sure the DB actor didn't lose any report.
    # If this assert fails, that means at least once actor task semantic
    # wasn't guaranteed.
    for i in range(highest_reported_num):
        assert str(i) in letter_set, i
