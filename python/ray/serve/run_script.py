import argparse
import pathlib
import sys
import time
import traceback
import yaml

from ray import serve
from ray._private.utils import import_attr
from ray.autoscaler._private.cli_logger import cli_logger
from ray.serve._private import api as _private_api
from ray.serve._private.constants import (
    DEFAULT_HTTP_HOST,
    DEFAULT_HTTP_PORT,
)
from ray.serve.schema import ServeApplicationSchema


def main():
    """
    This is the Job that gets submitted to the Ray Cluster when `serve run` is executed.

    Loads the Serve app (either from a YAML config file or a direct import path), starts
    Serve and runs the app. By default, the code blocks until a SIGINT signal is
    received, at which point Serve is shutdown and the process exits.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-or-import-path")
    parser.add_argument("--app-dir")
    parser.add_argument("--host")
    parser.add_argument("--port", type=int)
    parser.add_argument("--blocking", action="store_true")
    parser.add_argument("--gradio", action="store_true")
    args = parser.parse_args()
    host, port = args.host, args.port

    sys.path.insert(0, args.app_dir)
    if pathlib.Path(args.config_or_import_path).is_file():
        config_path = args.config_or_import_path
        cli_logger.print(f"Deploying from config file: '{config_path}'.")

        with open(config_path, "r") as config_file:
            config_dict = yaml.safe_load(config_file)
            # If host or port is specified as a CLI argument, they should take priority
            # over config values.
            config_dict.setdefault("host", DEFAULT_HTTP_HOST)
            if host is not None:
                config_dict["host"] = host

            config_dict.setdefault("port", DEFAULT_HTTP_PORT)
            if port is not None:
                config_dict["port"] = port

            config = ServeApplicationSchema.parse_obj(config_dict)
        is_config = True
    else:
        if host is None:
            host = DEFAULT_HTTP_HOST
        if port is None:
            port = DEFAULT_HTTP_PORT
        import_path = args.config_or_import_path
        cli_logger.print(f"Deploying from import path: '{import_path}'.")
        node = import_attr(import_path)
        is_config = False

    if is_config:
        client = _private_api.serve_start(
            detached=True,
            http_options={
                "host": config.host,
                "port": config.port,
                "location": "EveryNode",
            },
        )
    else:
        client = _private_api.serve_start(
            detached=True,
            http_options={
                "host": host,
                "port": port,
                "location": "EveryNode",
            },
        )

    try:
        if is_config:
            client.deploy_app(config, _blocking=args.gradio)
            cli_logger.success("Submitted deploy config successfully.")
            if args.gradio:
                handle = serve.get_deployment("DAGDriver").get_handle()
        else:
            handle = serve.run(node, host=host, port=port)
            cli_logger.success("Deployed Serve app successfully.")

        if args.gradio:
            from ray.serve.experimental.gradio_visualize_graph import (
                GraphVisualizer,
            )

            visualizer = GraphVisualizer()
            visualizer.visualize_with_gradio(handle)
        else:
            if args.blocking:
                while True:
                    # Block, letting Ray print logs to the terminal.
                    time.sleep(10)

    except KeyboardInterrupt:
        cli_logger.info("Got KeyboardInterrupt, shutting down...")
        serve.shutdown()
        sys.exit()

    except Exception:
        traceback.print_exc()
        cli_logger.error(
            "Received unexpected error, see console logs for more details. Shutting "
            "down..."
        )
        serve.shutdown()
        sys.exit()


if __name__ == "__main__":
    main()
