import asyncio
import contextlib
from concurrent import futures
from tempfile import TemporaryDirectory

import click
import grpc

from ray.serve._private.benchmarks.streaming._grpc import (
    test_server_pb2,
    test_server_pb2_grpc,
)
from ray.serve._private.benchmarks.streaming._grpc.grpc_server import TestGRPCServer
from ray.serve._private.benchmarks.streaming.common import Caller, IOMode


class GrpcCaller(Caller):
    async def _consume_single_stream(self):
        async for r in self._h.ServerStreaming(test_server_pb2.Request()):
            self.sink(r)


async def create_test_server(socket_type, tempdir):
    server = grpc.aio.server(futures.ThreadPoolExecutor(max_workers=1))

    if socket_type == "uds":
        addr = f"unix://{tempdir}/server.sock"
        server_creds = grpc.local_server_credentials(grpc.LocalConnectionType.UDS)
        channel_creds = grpc.local_channel_credentials(grpc.LocalConnectionType.UDS)
    elif socket_type == "local_tcp":
        addr = "localhost:5432"
        server_creds = grpc.local_server_credentials(grpc.LocalConnectionType.LOCAL_TCP)
        channel_creds = grpc.local_channel_credentials(
            grpc.LocalConnectionType.LOCAL_TCP
        )
    else:
        raise NotImplementedError(f"Not supported socket type ({socket_type})")

    server.add_secure_port(addr, server_creds)

    await server.start()

    @contextlib.asynccontextmanager
    async def get_channel(interceptors=None):
        async with grpc.aio.secure_channel(
            addr, credentials=channel_creds, interceptors=interceptors or []
        ) as channel:
            yield channel

    return server, get_channel


async def run_grpc_benchmark(
    batch_size,
    io_mode,
    socket_type,
    num_replicas,
    num_trials,
    tokens_per_request,
    trial_runtime,
):
    with TemporaryDirectory() as tempdir:
        server, get_channel = await create_test_server(socket_type, tempdir)
        test_server_pb2_grpc.add_GRPCTestServerServicer_to_server(
            TestGRPCServer(tokens_per_request), server
        )

        async with get_channel() as c:
            stub = test_server_pb2_grpc.GRPCTestServerStub(c)

            c = GrpcCaller(
                stub,
                mode=IOMode(io_mode.upper()),
                tokens_per_request=tokens_per_request,
                batch_size=batch_size,
                num_trials=num_trials,
                trial_runtime=trial_runtime,
            )

            mean, stddev = await c.run_benchmark()

            print(
                "DeploymentHandle streaming throughput ({}) {}: {} +- {} tokens/s".format(
                    io_mode.upper(),
                    f"(num_replicas={num_replicas}, "
                    f"tokens_per_request={tokens_per_request}, "
                    f"batch_size={batch_size})",
                    mean,
                    stddev,
                )
            )

        await server.stop(None)


@click.command(help="Benchmark streaming deployment handle throughput.")
@click.option(
    "--tokens-per-request",
    type=int,
    default=1000,
    help="Number of tokens (per request) to stream from downstream deployment",
)
@click.option(
    "--batch-size",
    type=int,
    default=10,
    help="Number of requests to send to downstream deployment in each batch.",
)
@click.option(
    "--num-replicas",
    type=int,
    default=1,
    help="Number of replicas in the downstream deployment.",
)
@click.option(
    "--num-trials",
    type=int,
    default=5,
    help="Number of trials of the benchmark to run.",
)
@click.option(
    "--trial-runtime",
    type=int,
    default=5,
    help="Duration to run each trial of the benchmark for (seconds).",
)
@click.option(
    "--io-mode",
    type=str,
    default="async",
    help="Controls mode of the streaming generation (either 'sync' or 'async')",
)
@click.option(
    "--socket-type",
    type=str,
    default="local_tcp",
    help="Controls type of socket used as underlying transport (either 'uds' or 'local_tcp')",
)
def main(
    tokens_per_request: int,
    batch_size: int,
    num_replicas: int,
    num_trials: int,
    trial_runtime: float,
    io_mode: str,
    socket_type: grpc.LocalConnectionType,
):
    asyncio.run(
        run_grpc_benchmark(
            batch_size,
            io_mode,
            socket_type,
            num_replicas,
            num_trials,
            tokens_per_request,
            trial_runtime,
        )
    )


if __name__ == "__main__":
    main()
