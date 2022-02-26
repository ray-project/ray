from typing import Dict, Union, List, Optional

import ray.util as ray_util
import ray
import time
import random

import sqlite3
import os
import pathlib
import pickle

'''
PG_TABLE (which will just be a subset of columns in jobs table):
    PG_ID INT (could use job id instead with slight modification)
    PENDING_PG [PICKLE_TYPE]
    PG_RANK INT [ADD BTREE]
'''

class PendingPlacementGroup:

    '''
    Stores a pending placement group that can be turned into actual placement group with the
    start function
    '''

    def __init__(self, 
        bundles: List[Dict[str, float]],
        strategy: str = "PACK",
        name: str = "",
        lifetime=None):
        self.id = random.getrandbits(32)
        self.bundles = bundles
        self.strategy = strategy
        self.name = name
        self.lifetime = lifetime
    
    def start(self):
        '''
        If someone desires to manually schedule their pg, they can just run start by themselves instead of using the wait function
        '''
        pg = ray_util.placement_group(bundles=self.bundles, strategy=self.strategy, name = self.name, lifetime = self.lifetime)
        return pg

class PlacementGroupScheduler:
    def __init__(self):
        _DB_PATH = os.path.expanduser('~/.sky/jobs.db')
        os.makedirs(pathlib.Path(_DB_PATH).parents[0], exist_ok=True)

        self._CONN = sqlite3.connect(_DB_PATH)
        self._CURSOR = self._CONN.cursor()

        # CREATE TABLE
        try:
            self._CURSOR.execute('SELECT * FROM pg_table LIMIT 0')
        except sqlite3.OperationalError:
            # Tables do not exist, create them.
            self._CURSOR.execute(""" CREATE TABLE pg_table( \
                pg_id INTEGER,
                pending_placement_group TEXT,
                pg_rank INTEGER
            )""")

    def resource_requirement(self, bundles: List[Dict[str, float]],
        strategy: str = "PACK",
        name: str = "",
        lifetime=None):

        pending_pg = PendingPlacementGroup(bundles, strategy, name, lifetime)
        return pending_pg
    
    def wait(self, pending_pg, timeout = 1):
        # NOTE: Currently this scheduler runs FIFO order based on when wait is called on the pg instead of
        # when the pg is created
        rank = int(time.time() * 10000)
        self._add_placement_group(pending_pg, rank)

        while self._query_min_rank_ID() != pending_pg.id:
            time.sleep(timeout)
        
        pg = pending_pg.start()
        ray.get(pg.ready())
        self._remove_placement_group(pending_pg)
        return pg

    def remove(self, pending_pg, pg):
        ray_util.remove_placement_group(pg)

    def _add_placement_group(self, pg, rank):
        # SQL query to add placement group to queue
        self._CURSOR.execute('INSERT INTO pg_table VALUES (?,?,?)', (pg.id, pickle.dumps(pg),rank))
        self._CONN.commit()

    def _query_min_rank_ID(self, ):
        # SQL query for placement group with min pg_rank
        query = self._CURSOR.execute("SELECT pg_id FROM pg_table WHERE pg_rank = (SELECT min(pg_rank) FROM pg_table)")
        for row in query: return row[0]

    def clear_queue(self, ):
        # SQL query to clear all pending placement_groups
        query = self._CURSOR.execute("DELETE FROM pg_table WHERE true")
        self._CONN.commit()

    def _remove_placement_group(self, pg):
        # SQL query to remove placement_group when completed 
        # NOTE: Alternative could be to set the rank very high i.e if we want to maintain the job in the table
        query = self._CURSOR.execute(f"DELETE FROM pg_table WHERE pg_id = {pg.id}")
        self._CONN.commit()


'''
Edge cases:
    1.  if a placement group is scheduled (added to the database) but the program is exited before ray.get is called
        on pg.ready(), since there is no premption, the rest of the placement groups scheduled later will stall
        -> Solution: 
    2. 
'''