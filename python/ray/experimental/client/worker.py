
import cloudpickle
import grpc
import ray.core.generated.ray_client_pb2 as ray_client_pb2
import ray.core.generated.ray_client_pb2_grpc as ray_client_pb2_grpc


class ObjectID:
    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return "ObjectID(%s)" % self.id.hex()


class Worker:
    def __init__(self, conn_str="", stub=None):
        if stub is None:
            self.channel = grpc.insecure_channel(conn_str)
            self.server = ray_client_pb2_grpc.RayletDriverStub(self.channel)
        else:
            self.server = stub

    def get(self, ids):
        to_get = []
        single = False
        if isinstance(ids, list):
            to_get = [x.id for x in ids]
        elif isinstance(ids, ObjectID):
            to_get = [ids.id]
            single = True
        else:
            raise Exception(
                "Can't get something that's not a list of IDs or just an ID")

        out = [self._get(x) for x in to_get]
        if single:
            out = out[0]
        return out

    def _get(self, id: bytes):
        req = ray_client_pb2.GetRequest(id=id)
        data = self.server.GetObject(req)
        return cloudpickle.loads(data.data)

    def put(self, vals):
        to_put = []
        single = False
        if isinstance(vals, list):
            to_put = vals
        else:
            single = True
            to_put.append(vals)

        out = [self._put(x) for x in to_put]
        if single:
            out = out[0]
        return out

    def _put(self, val):
        data = cloudpickle.dumps(val)
        req = ray_client_pb2.PutRequest(data=data)
        resp = self.server.PutObject(req)
        return ObjectID(resp.id)

    def remote(self, func):
        return RemoteFunc(self, func)

    def schedule(self, task):
        return self.server.Schedule(task)

    def close(self):
        self.channel.close()


class RemoteFunc:
    def __init__(self, worker, f):
        self._func = f
        self._name = f.__name__
        self.id = None

    def __call__(self, *args, **kwargs):
        raise Exception("Matching the old API")

    def remote(self, *args):
        pass

    def __repr__(self):
        return "RemoteFunc(%s, %s)" % (self._name, self.id)
