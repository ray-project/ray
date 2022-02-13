from typing import Dict, Union, List, Optional

import ray.util as ray_util
import ray
import time
import random

class PendingPlacementGroup:

    '''
    Stores a placement group that 
    '''

    def __init__(self, 
        bundles: List[Dict[str, float]],
        strategy: str = "PACK",
        name: str = "",
        lifetime=None):
        self.bundles = bundles
        self.strategy = strategy
        self.name = name
        self.lifetime = lifetime
    
    def start(self):
        pg = ray_util.placement_group(bundles=self.bundles, strategy=self.strategy, name = self.name, lifetime = self.lifetime)
        return pg

@ray.remote
class PlacementGroupQueue:

    '''
    A remote object to store a shared queue of pending placement groups.

    Right now rank is implemented so that it is FIFO queue, but it can be modified
    for any use.
    '''
    
    def __init__(self):
        self.pending_list = {}

    def add(self, pendingPlacementGroup, rank = None):
        if rank == None:
            rank = int(time.time() * 10000)
        pg_id = random.getrandbits(64)
        self.pending_list[pg_id] = (rank, pendingPlacementGroup)
        return pg_id

    def min_rank_id(self):
        criteria = lambda t: self.pending_list[t][0]
        return min(self.pending_list, key = criteria)
    
    def get(self, pg_id):
        return self.pending_list[pg_id][1]

    def remove(self, pg_id):
        self.pending_list.pop(pg_id)

    
class PlacementGroupScheduler:

    '''
    A client library to handle staging and acquiring resources from a placement group.
    '''

    def __init__(self):
        self.queue = PlacementGroupQueue.remote()

    def stage_placement_group(self, 
        bundles: List[Dict[str, float]],
        strategy: str = "PACK",
        name: str = "",
        lifetime=None):

        pending_placment_group = PendingPlacementGroup(bundles, strategy, name, lifetime)
        return ray.get(self.queue.add.remote(pending_placment_group))

    def get(self, pg_id):

        min_id = ray.get(self.queue.min_rank_id.remote())
        while pg_id != min_id:
            min_id = ray.get(self.queue.min_rank_id.remote())
            time.sleep(1)
        
        pendingPlacementGroup = ray.get(self.queue.get.remote(pg_id))

        pg = pendingPlacementGroup.start()
        ray.get(pg.ready())

        return pg

    def remove(self, pg, pg_id):
        ray_util.remove_placement_group(pg)
        self.queue.remove.remote(pg_id)
        pass
