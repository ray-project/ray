"""Cross-node validation of the prompt-token ZMQ channel.

The CPU tests drive TokenSender/TokenReceiver inside one process, so they cannot
catch anything address-related. Here each LLMServer replica gets its own node, so
the LLMRouter has to reach a *remote* replica's PULL socket: a loopback-only
bind, a wrong advertised address, or a port the cluster blocks fails here rather
than in production.

Send a batch of requests and assert their prompt tokens were pushed to replicas
on two distinct nodes. Requests go through a node that hosts a replica, so the
ingress had a local replica available and still had to reach the remote one.

The assertion is on delivery -- that the push succeeded -- not on the engine
subsequently skipping tokenization. TokenSender sets ZMQ_IMMEDIATE, so a push
only succeeds once the peer connection is established; an unreachable replica
returns False and the router omits the token key.
"""

import os
import sys

import pytest
import requests

import ray
from ray import serve
from ray._common.test_utils import async_wait_for_condition
from ray.serve.config import RequestRouterConfig
from ray.serve.llm import LLMConfig, ModelLoadingConfig, build_openai_app
from ray.serve.llm.request_router import KVAwareRouter
from ray.util.state import list_actors

from utils import patch_ingress

MODEL_ID = "qwen3-0.6b"
MODEL_SOURCE = "Qwen/Qwen3-0.6B"
APP_NAME = "token_channel_cross_node_test"
NUM_REPLICAS = 2
NUM_REQUESTS = 10
MAX_TOKENS = 8
REQUIRED_ENV = {
    "RAY_SERVE_ENABLE_HA_PROXY": "1",
    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
    "RAY_SERVE_DEFAULT_HTTP_HOST": "0.0.0.0",
    # Defaults to 0, which makes HAProxy hand the router an empty body. The
    # router then derives no routing key, skips tokenization, and never pushes
    # tokens -- the channel silently no-ops.
    "RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY": "1",
    # More than one ingress per node, so each node's HAProxy round-robins over
    # several routers and the channel is exercised from every one of them.
    "RAY_SERVE_INGRESS_ROUTER_REPLICAS_PER_NODE": "2",
}
# Deliberately prefix-disjoint: see test_prompt_tokens_reach_a_remote_replica.
_UNIQUE_PROMPTS = [
    "Volcanoes near Reykjavik erupt because",
    "The lattice constant of silicon governs",
    "Baroque counterpoint differs from romantic harmony in that",
    "Migrating terns navigate by",
]


def _replica_node_ips():
    """Node IPs hosting an LLMServer replica."""
    ip_by_node = {n["NodeID"]: n["NodeManagerAddress"] for n in ray.nodes()}
    return {
        ip_by_node[actor.node_id]
        for actor in list_actors(filters=[("state", "=", "ALIVE")])
        if "LLMServer" in (actor.class_name or "") and actor.node_id in ip_by_node
    }


def _post_completion(host, prompt):
    response = requests.post(
        f"http://{host}:8000/v1/completions",
        json={
            "model": MODEL_ID,
            "prompt": prompt,
            "max_tokens": MAX_TOKENS,
            "temperature": 0.0,
        },
        timeout=120,
    )
    assert response.status_code == 200, response.text
    return response.json()


def _endpoint_host(endpoint):
    """``tcp://10.0.1.2:7557`` -> ``10.0.1.2``"""
    return endpoint.rsplit(":", 1)[0].rsplit("/", 1)[-1]


def _target_nodes(pushes):
    """Nodes whose replicas received a prompt-token push."""
    return {_endpoint_host(push["endpoint"]) for _, push in pushes}


class TestTokenChannelCrossNode:
    @pytest.fixture(scope="class")
    def deployed_app(self):
        """One direct-streaming KV-aware LLMServer replica per worker node."""
        # These are read as module-level constants in whichever process imports
        # them, so they must be set cluster-wide (the release entry does it via
        # the byod runtime_env). Unset in the driver, build_openai_app silently
        # produces the non-direct-streaming OpenAiIngress shape and there is no
        # LLMRouter to push tokens at all; unset in the proxy, HAProxy never runs
        # and /internal/route is never called. Fail loudly instead.
        missing = [k for k in REQUIRED_ENV if os.environ.get(k) != REQUIRED_ENV[k]]
        assert not missing, (
            f"{missing} must be set cluster-wide for the token channel to engage; "
            "without them this test cannot exercise the transport"
        )
        if not ray.is_initialized():
            # runtime_env covers actors the driver spawns (controller, proxies);
            # the driver's own env is what the assert above guards.
            ray.init(address="auto", runtime_env={"env_vars": REQUIRED_ENV})
        serve.shutdown()

        llm_config = LLMConfig(
            model_loading_config=ModelLoadingConfig(
                model_id=MODEL_ID,
                model_source=MODEL_SOURCE,
            ),
            deployment_config=dict(
                autoscaling_config=dict(
                    min_replicas=NUM_REPLICAS, max_replicas=NUM_REPLICAS
                ),
                # The real KVAwareRouter: it is what makes the ingress tokenize
                # before routing and hand the token endpoint to the push, so a
                # substituted router would not exercise the channel at all.
                request_router_config=RequestRouterConfig(
                    request_router_class=KVAwareRouter
                ),
            ),
            engine_kwargs=dict(
                max_model_len=2048,
                enforce_eager=True,
                gpu_memory_utilization=0.4,
            ),
            # One GPU per replica against one GPU per worker node (llm_2x_1xl4.yaml)
            # is what puts the replicas on separate nodes. placement_group_bundles
            # rules out max_replicas_per_node, the direct way to say this.
            placement_group_config={"bundles": [{"GPU": 1}]},
            experimental_configs={"KV_EVENTS_PORT_BASE": 23557},
            runtime_env=dict(
                env_vars={
                    "RAY_SERVE_ENABLE_DIRECT_INGRESS": "1",
                    "RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING": "1",
                },
            ),
            log_engine_metrics=False,
        )
        with patch_ingress():
            app = build_openai_app({"llm_configs": [llm_config]})
            serve.run(app, name=APP_NAME)
        yield
        serve.shutdown()

    def test_replicas_span_distinct_nodes(self, deployed_app):
        """Guard the premise: colocated replicas would make the next test vacuous."""
        nodes = {
            actor.node_id
            for actor in list_actors(filters=[("state", "=", "ALIVE")])
            if "LLMServer" in (actor.class_name or "")
        }
        assert len(nodes) >= NUM_REPLICAS, (
            f"expected {NUM_REPLICAS} LLMServer replicas on distinct nodes, got "
            f"{len(nodes)}; the cluster must have one GPU per worker node"
        )

    def _send_batch(self, host, count):
        # Prompts share no prefix, so KV overlap is zero for all of them and
        # selection falls back to load, spreading across both replicas. A
        # repeated prompt would pin every request to one replica and the push
        # might never leave its node.
        for i in range(count):
            _post_completion(host, f"{_UNIQUE_PROMPTS[i % len(_UNIQUE_PROMPTS)]} {i}")

    async def _pushes(self, router):
        reports = await router.broadcast("get_token_push_report").results_async()
        return reports, [(r["node_ip"], p) for r in reports for p in r["pushes"]]

    async def test_prompt_tokens_reach_a_remote_replica(self, deployed_app):
        router = serve.get_deployment_handle("LLMRouter", app_name=APP_NAME)

        # Drive requests at a node that *hosts* a replica. HAProxy routes via its
        # node-local ingress, so that ingress has a local replica available and
        # still has to reach the remote one when selection picks it. Going through
        # the head instead would make every push cross-node trivially, since the
        # head has no GPU and so never hosts an LLMServer.
        replica_ips = _replica_node_ips()
        assert len(replica_ips) >= 2, f"expected replicas on 2 nodes, got {replica_ips}"
        host = sorted(replica_ips)[0]

        # TokenSender sets ZMQ_IMMEDIATE, so the first push over each
        # (ingress, replica) pipe races the TCP handshake and is expected to miss
        # -- that request just falls back to engine tokenization. With several
        # ingress replicas per node there are several pipes to establish, so retry
        # whole rounds until one lands with every push delivered and both nodes
        # covered. That round is the measurement, which leaves no warm/measure
        # boundary to get wrong.
        async def _round_covers_both_nodes():
            await router.broadcast("reset_token_pushes").results_async()
            self._send_batch(host, NUM_REQUESTS)
            reports, attempted = await self._pushes(router)
            assert attempted, (
                "the router never attempted a prompt-token push; ingress state "
                f"was {[{k: v for k, v in r.items() if k != 'pushes'} for r in reports]}"
            )
            if not all(p["delivered"] for _, p in attempted):
                return False
            return len(_target_nodes(attempted)) >= NUM_REPLICAS

        try:
            await async_wait_for_condition(
                _round_covers_both_nodes, timeout=300, retry_interval_ms=500
            )
        except RuntimeError:
            # The bare timeout says nothing; name what the last round managed,
            # which is the whole point of running this cross-node.
            _, attempted = await self._pushes(router)
            raise AssertionError(
                f"{NUM_REQUESTS} requests never produced delivered prompt-token "
                f"pushes to {NUM_REPLICAS} distinct nodes. Last round: "
                f"{sorted({(ip, p['endpoint'], p['delivered']) for ip, p in attempted})}"
            ) from None

        # The router pushes tokens while serving /internal/route, before the
        # completion returns, so the last round is fully recorded by now.
        reports, attempted = await self._pushes(router)
        assert attempted, f"no push recorded after warmup: {reports}"
        undelivered = [(ip, p) for ip, p in attempted if not p["delivered"]]
        assert (
            not undelivered
        ), f"prompt-token pushes failed on an established pipe: {undelivered}"
        # The point of the test: tokens for these requests were pushed to replicas
        # on more than one node. The ingress lives on one of them, so that
        # necessarily means at least one push crossed a node boundary.
        target_nodes = _target_nodes(attempted)
        assert len(target_nodes) >= NUM_REPLICAS, (
            f"prompt tokens only reached {sorted(target_nodes)}; expected pushes "
            f"to {NUM_REPLICAS} distinct nodes: {attempted}"
        )
        # Printed so a passing run shows which boundaries were actually crossed,
        # rather than only asserting that some were.
        print(
            f"\n{len(attempted)} prompt-token pushes from ingress on {host}, "
            f"delivered to {len(target_nodes)} nodes {sorted(target_nodes)}:"
        )
        for ingress_ip, push in attempted:
            hop = (
                "same-node"
                if _endpoint_host(push["endpoint"]) == ingress_ip
                else "CROSS-NODE"
            )
            print(f"  {ingress_ip} -> {push['endpoint']}  [{hop}]")


if __name__ == "__main__":
    sys.exit(pytest.main(["-vs", __file__]))
