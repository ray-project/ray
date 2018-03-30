from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import argparse

from worker import Worker

parser = argparse.ArgumentParser()
parser.add_argument("raylet_socket_name")
parser.add_argument("object_store_socket_name")

if __name__ == '__main__':
    args = parser.parse_args()

    worker = Worker(args.raylet_socket_name, args.object_store_socket_name,
                    is_worker=True)
    worker.main_loop()
