
class ActorCheckpoint(object):

    def __init__(self, checkpoint_index, checkpoint_data, actor_frontier):
        self.index = checkpoint_index
        self.data = checkpoint_data
        self.frontier = actor_frontier
