
import cloudpickle
import grpc
import proto.task_pb2
import proto.task_pb2_grpc


class ObjectID:
    def __init__(self, id):
        self.id = id

    def __repr__(self):
        return "ObjectID(%s)" % self.id.decode()


class Worker:
    def __init__(self, conn_str="", stub=None):
        if stub is None:
            self.channel = grpc.insecure_channel(conn_str)
            self.server = proto.task_pb2_grpc.TaskServerStub(self.channel)
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
        req = proto.task_pb2.GetRequest(id=id)
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
        #print("val: %s\ndata: %s"%(val, data))
        req = proto.task_pb2.PutRequest(data=data)
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
        if self.id is None:
            self._push_func()
        t = proto.task_pb2.Task()
        t.name = self._name
        t.payload_id = self.id.id
        for a in args:
            arg = proto.task_pb2.Arg()
            if isinstance(a, ObjectID):
                arg.local = proto.task_pb2.Arg.Locality.REFERENCE
                arg.reference_id = a.id
            else:
                arg.local = proto.task_pb2.Arg.Locality.INTERNED
                arg.data = cloudpickle.dumps(a)
            t.args.append(arg)
        worker = get_worker_registry(self._worker_id)
        ticket = worker.schedule(t)
        return ObjectID(ticket.return_id)

    def _push_func(self):
        worker = get_worker_registry(self._worker_id)
        self.id = worker.put(self._func)

    def __repr__(self):
        return "RemoteFunc(%s, %s)" % (self._name, self.id)
