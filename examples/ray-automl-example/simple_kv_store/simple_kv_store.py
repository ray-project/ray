import ray
from ray.util.placement_group import placement_group
from ray.util.scheduling_strategies import PlacementGroupSchedulingStrategy
import time

MAIN_ACTOR_NAME = "main_actor"
BACKUP_ACTOR_NAME = "backup_actor"


@ray.remote(max_restarts=-1, num_cpus=2)
class MainServer:
    def __init__(self):
        self.data_ = {}
        self.backup_actor_ = ray.get_actor(BACKUP_ACTOR_NAME)
        self.data_ = ray.get(self.backup_actor_.get_all_data.remote())

    def put(self, key: str, value: str) -> None:
        self.data_[key] = value
        self.backup_actor_.backup_data.remote(key, value)

    def get(self, key: str) -> str:
        return self.data_[key]


@ray.remote(max_restarts=-1, num_cpus=1)
class BackupServer:
    def __init__(self):
        self.data_ = {}

    def get_all_data(self) -> dict:
        return self.data_

    def backup_data(self, key: str, value: str) -> None:
        self.data_[key] = value


def always_retry(func):
    while True:
        try:
            return func()
            break
        except Exception as e:
            print(f"Catching exception {e}. Retry in 2 seconds.")
            time.sleep(2)
            continue


class Client:
    def __init__(self):
        self.main_actor_ = ray.get_actor(MAIN_ACTOR_NAME)
        return

    def put(self, key: str, value: str):
        always_retry(lambda k=key, v=value: ray.get(self.main_actor_.put.remote(k, v)))

    def get(self, key: str) -> str:
        return always_retry(lambda k=key: ray.get(self.main_actor_.get.remote(k)))


def start_servers():
    bundle1 = {"CPU": 2}
    bundle2 = {"CPU": 1}
    pg = placement_group([bundle1, bundle2], strategy="SPREAD")
    ray.get(pg.ready())
    main_actor = MainServer.options(name=MAIN_ACTOR_NAME, scheduling_strategy=PlacementGroupSchedulingStrategy(
                           placement_group=pg,
                           placement_group_bundle_index=0),

    ).remote()
    backup_actor = BackupServer.options(name=BACKUP_ACTOR_NAME, scheduling_strategy=PlacementGroupSchedulingStrategy(
                           placement_group=pg,
                           placement_group_bundle_index=1)
    ).remote()
    return main_actor, backup_actor


def kill_main_server(main_server):
    ray.kill(main_server, no_restart=False)


# Initialize
ray.init(include_dashboard=True, _node_ip_address="0.0.0.0")
# Start servers
main_actor, backup_actor = start_servers()
# Use client to put and get data
client = Client()
client.put("hello", "ray")
assert client.get("hello") == "ray"
# Kill server, it will be restarted automatically.
kill_main_server(main_actor)
# The key-value service will keep high availability.
client.put("world", "ray")
assert client.get("world") == "ray"
print("Run the simple kv store successfully!")
exit(0)
