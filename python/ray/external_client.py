import os
from subprocess import Popen, PIPE

# TODO (dsuo): eventually remove this dependency
import requests
import ray
import numpy as np


class ExternalClient(object):
    """A class used to proxy communication between a Ray external client
    and a remote Ray head node.
    """

    def __init__(self,
                 gateway_address,
                 gateway_socat_port=5001,
                 gateway_data_port=5002):
        """Initialize a new external client.

        Args:
            gateway_address (str): The IP address of the head node / gateway.
            gateway_socat_port (int): The gateway's port.
            data_port (int): The gateway's port for transferring data.

        Returns:
            A new ExternalClient object
        """
        self.gateway_address = gateway_address
        self.gateway_socat_port = gateway_socat_port
        self.gateway_data_port = gateway_data_port
        self.client_name = "external_client" + \
                str(np.random.randint(0, 99999999)).zfill(8)
        self.client_socket_name = "/tmp/ray_" + self.client_name
        self.url = "http://{}:{}".format(
            self.gateway_address,
            self.gateway_data_port)

        # TODO (dsuo): should move to connect()
        # TODO (dsuo): remove verbose mode
        command = [
            "socat", "-v", "UNIX-LISTEN:" + self.client_socket_name +
            ",reuseaddr,fork", "TCP:" + self.gateway_address + ":" +
            str(self.gateway_socat_port)
        ]

        # TODO (dsuo): handle cleanup, logging, etc
        raylogs = "/tmp/raylogs"
        if not os.path.exists(raylogs):
            os.makedirs(raylogs)

        outfile = os.path.join(raylogs, self.client_name + ".out")
        errfile = os.path.join(raylogs, self.client_name + ".err")
        with open(outfile, "w") as out, open(errfile, "w") as err:
            Popen(command, stdout=out, stderr=err)

    def put(self, value, object_id):
        """TODO (dsuo): Add comments

        Raises:
            Exception: An exception is raised if the serialization context
                was not properly initialized by the worker.py.
        """
        data = ray.pyarrow.serialize(value).to_buffer().to_pybytes()
        requests.post(url=self.url,
                      files={
                          "value": data,
                          "object_id": object_id
                      })

        return object_id

    def get(self, object_ids):
        """TODO (dsuo): Add comments

        Raises:
            Exception: An exception is raised if the serialization context
                was not properly initialized by the worker.py.
        """

        # TODO (dsuo): ignore batching
        # TODO (dsuo): would be nice to not encode / decode ObjectIDs
        param = ",".join([object_id.id().hex() for
                          object_id in object_ids])

        res = requests.get(url=self.url,
                           params={
                               "object_ids": param
                           },
                           stream=True)

        return ray.pyarrow.deserialize(res.raw.data)

    def submit(self, *args, **kwargs):
        for arg in kwargs:
            print(arg)
