import logging
from typing import List, Optional

import ray
import ray.util.collective as col
from ray.experimental.channel import ChannelContext
from ray.util.collective.types import Backend

logger = logging.getLogger(__name__)


class _GlooGroup:
    def __init__(
        self,
        rank: Optional[int],
        group_name: str,
        actor_handles: List["ray.actor.ActorHandle"],
    ):
        """
        Initialize a Gloo communicator that can be used to communicate p2p with
        other actors.

        Args:
            rank: The rank of this actor. If None, then the caller is not a
                participant of the Gloo group.
            group_name: The name of the Gloo group.
            actor_handles: All actor handles in the Gloo group.
        """
        self._backend = Backend.GLOO
        self._rank = rank
        self._group_name = group_name
        self._actor_handles = actor_handles
        # The number of participating actors/devices.
        self._world_size = len(actor_handles)
        self._closed = False
        if self._rank is not None:
            col.init_collective_group(
                self._world_size,
                self._rank,
                self._backend,
                self._group_name,
            )

    def get_rank(self, actor: "ray.actor.ActorHandle") -> int:
        """
        Return the given actor's rank in the Gloo communicator.

        Args:
            actor: The actor handle to look up.
        """
        actor_ids = [a._ray_actor_id for a in self._actor_handles]
        try:
            rank = actor_ids.index(actor._ray_actor_id)
        except ValueError as exc:
            raise ValueError("Actor is not in the Gloo group.") from exc
        return rank

    def send(self, buf, peer_rank: int):
        """
        Send data to a peer.

        Args:
            buf: The buffer to send.
            peer_rank: The rank of the actor to send to.
        """
        col.send(buf, peer_rank, self._group_name)

    def recv(self, buf, peer_rank: int):
        """
        Receive data from a peer.

        Args:
            buf: The buffer to receive into.
            peer_rank: The rank of the actor to receive from.
        """
        col.recv(buf, peer_rank, self._group_name)
        return buf

    def destroy(self):
        """
        Destroy the Gloo group.
        """
        # TODO (kevin85421): can't teardown successfully
        if self._closed:
            return

        self._closed = True

        col.destroy_collective_group(self._group_name)


def _do_init_gloo_group(
    self,
    rank: int,
    group_name: str,
    actor_handles: List["ray.actor.ActorHandle"],
) -> _GlooGroup:
    ctx = ChannelContext.get_current()
    ctx.gloo_groups[group_name] = _GlooGroup(rank, group_name, actor_handles)


def _init_gloo_group(group_name: str, actor_handles: List["ray.actor.ActorHandle"]):
    init_tasks = [
        actor.__ray_call__.remote(
            _do_init_gloo_group,
            rank,
            group_name,
            actor_handles,
        )
        for rank, actor in enumerate(actor_handles)
    ]
    ray.get(init_tasks, timeout=60)
    ctx = ChannelContext.get_current()
    group = _GlooGroup(None, group_name, actor_handles)
    ctx.gloo_groups[group_name] = group
