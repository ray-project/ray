"""DEPRECATED: Please use rllib.env.PolicyServerInput instead."""

import pickle
import traceback

from http.server import SimpleHTTPRequestHandler, HTTPServer
from socketserver import ThreadingMixIn

from ray.rllib.utils.annotations import PublicAPI
from ray.rllib.utils.policy_client import PolicyClient
from ray.rllib.utils.deprecation import deprecation_warning


@PublicAPI
class PolicyServer(ThreadingMixIn, HTTPServer):
    """DEPRECATED: Please use rllib.env.PolicyServerInput instead."""

    @PublicAPI
    def __init__(self, external_env, address, port):
        deprecation_warning(
            "rllib.utils.PolicyClient", new="rllib.env.PolicyClient")
        handler = _make_handler(external_env)
        HTTPServer.__init__(self, (address, port), handler)


def _make_handler(external_env):
    class Handler(SimpleHTTPRequestHandler):
        def do_POST(self):
            content_len = int(self.headers.get("Content-Length"), 0)
            raw_body = self.rfile.read(content_len)
            parsed_input = pickle.loads(raw_body)
            try:
                response = self.execute_command(parsed_input)
                self.send_response(200)
                self.end_headers()
                self.wfile.write(pickle.dumps(response))
            except Exception:
                self.send_error(500, traceback.format_exc())

        def execute_command(self, args):
            command = args["command"]
            response = {}
            if command == PolicyClient.START_EPISODE:
                response["episode_id"] = external_env.start_episode(
                    args["episode_id"], args["training_enabled"])
            elif command == PolicyClient.GET_ACTION:
                response["action"] = external_env.get_action(
                    args["episode_id"], args["observation"])
            elif command == PolicyClient.LOG_ACTION:
                external_env.log_action(args["episode_id"],
                                        args["observation"], args["action"])
            elif command == PolicyClient.LOG_RETURNS:
                external_env.log_returns(args["episode_id"], args["reward"],
                                         args["info"])
            elif command == PolicyClient.END_EPISODE:
                external_env.end_episode(args["episode_id"],
                                         args["observation"])
            else:
                raise Exception("Unknown command: {}".format(command))
            return response

    return Handler
