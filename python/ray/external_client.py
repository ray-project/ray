import subprocess

# TODO (dsuo): eventually remove this dependency
import requests
import pyarrow
# import pickle
import numpy as np

class ExternalClient(object):
    """A class used to proxy communication between a Ray external client
    and a remote Ray head node.
    """

    def __init__(self,
                 gateway_address,
                 gateway_port=5432,
                 gateway_data_port=5000):
        """Initialize a new external client.

        Args:
            gateway_address (str): The IP address of the head node / gateway.
            gateway_port (int): The gateway's port.
            data_port (int): The gateway's port for transferring data.

        Returns:
            A new ExternalClient object
        """
        self.gateway_address = gateway_address
        self.gateway_port = gateway_port
        self.gateway_data_port = gateway_data_port
        self.client_socket_name = "/tmp/ray_external_client" + \
            str(np.random.randint(0, 99999999)).zfill(8)
        self.serialization_context = None
        self.url = "http://{}:{}".format(
            self.gateway_address,
            self.gateway_data_port)

        print(self.url)

        # TODO (dsuo): should move to connect()
        command = [
            "socat", "UNIX-LISTEN:" + self.client_socket_name + \
            ",reuseaddr,fork", "TCP:" + self.gateway_address + ":" + \
            str(self.gateway_port)
        ]

        # TODO (dsuo): handle cleanup, logging, etc
        p = subprocess.Popen(command, stdout=None, stderr=None)

    def put(self, value, object_id):
        """TODO (dsuo): Add comments

        Raises:
            Exception: An exception is raised if the serialization context
                was not properly initialized by the worker.py.
        """
        if self.serialization_context is None:
            raise Exception("Serialization context in ExternalClient not "
                            "initialized.")

        data = self.serialization_context.serialize(value) \
                                         .to_buffer().to_pybytes()
        res = requests.post(url=self.url,
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
        param = ",".join([object_id.id().hex() for \
                         object_id in object_ids])

        res = requests.get(url=self.url,
                           params={
                               "object_ids": param
                           },
                           stream=True)

        objects = self.serialization_context.deserialize(res.raw.read())

        return objects

    def submit(self, *args, **kwargs):
        for arg in kwargs:
            print(arg)
