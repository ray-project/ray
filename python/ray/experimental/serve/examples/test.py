import ray
import socket
ray.init()
import pickle
import docker

# TODO(Rehan) Container Argument (already partially implemented, test)
# TODO(Rehan) Randomly generate port, check if unbound.
# TODO(Rehan) Move to Ray Fork.
@ray.remote
class Predictor(object):
    def __init__(self, container):
        self.port = 8887
        self.sock = None
        self.cl = docker.from_env()
        self.cl.containers.run(container, detach=True, environment={"port": self.port})
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        host = ''
        print(self.port)
        self.sock.bind((host, self.port))
        self.sock.listen()
        self.c, self.a = self.sock.accept()

    def predict(self, xs):
        a = pickle.dumps(xs)
        self.c.send(str(len(a)).encode())
        self.c.send(a)
        return pickle.loads(self.c.recv(int(self.c.recv(2))))

a = Predictor.remote()
c1 = a.predict.remote([1, 2, 3])
c2 = a.predict.remote([2, 3, 4])
print(c1)
print(c2)
print(ray.get(c1))
print(ray.get(c2))