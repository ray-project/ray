
# TODO implement thin interface for clients to external KV-stores, used for checkpointing

class KVStoreAddressUtil(object):

    @staticmethod
    def parse(address):
        address = address.split(":")
        # TODO switch case on address to return the correct type of client


class KVStoreClientInterface(object):

    def connect(self, address):
        raise NotImplementedError

    def disconnect(self):
        raise NotImplementedError

    def get(self, key):
        raise NotImplementedError

    def put(self, key, value):
        raise NotImplementedError


class RedisClient(KVStoreClientInterface):

    def __init__(self, redis_client):
        # TODO special case, just use the existing gcs client
        self.redis_client = redis_client

    def connect(self, address):
        pass

    def disconnect(self):
        pass

    def get(self, key):
        self.redis_client.get(key)

    def put(self, key, value):
        self.redis_client.set(key, value)


class S3Client(KVStoreClientInterface):

    # TODO attempt client library import, implement

    def connect(self, address):
        pass

    def disconnect(self):
        pass

    def get(self, key):
        pass

    def put(self, key, value):
        pass
