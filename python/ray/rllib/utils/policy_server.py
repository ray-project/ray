from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pickle
import sys
import traceback

from ray.rllib.utils.policy_client import PolicyClient

if sys.version_info[0] == 2:
    from SimpleHTTPServer import SimpleHTTPRequestHandler
    from SocketServer import TCPServer as HTTPServer
    from SocketServer import ThreadingMixIn
elif sys.version_info[0] == 3:
    from http.server import SimpleHTTPRequestHandler, HTTPServer
    from socketserver import ThreadingMixIn


class PolicyServer(ThreadingMixIn, HTTPServer):
    def __init__(self, serving_env, address, port):
        handler = _make_handler(serving_env)
        HTTPServer.__init__(self, (address, port), handler)


def _make_handler(serving_env):
    class Handler(SimpleHTTPRequestHandler):
        def do_POST(self):
            content_len = int(self.headers.get('Content-Length'), 0)
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
                serving_env.start_episode(
                    args["episode_id"], args["training_enabled"])
            elif command == PolicyClient.GET_ACTION:
                response["action"] = serving_env.get_action(
                    args["observation"], args["episode_id"])
            elif command == PolicyClient.LOG_ACTION:
                serving_env.log_action(
                    args["observation"], args["action"],
                    args["episode_id"])
            elif command == PolicyClient.LOG_RETURNS:
                serving_env.log_returns(
                    args["reward"], args["info"], args["episode_id"])
            elif command == PolicyClient.END_EPISODE:
                serving_env.end_episode(
                    args["observation"], args["episode_id"])
            else:
                raise Exception("Unknown command: {}".format(command))
            return response

    return Handler
