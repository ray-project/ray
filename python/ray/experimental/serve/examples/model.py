import sys
import socket
import pickle

host = sys.argv[1]
port = int(sys.argv[2])
print(port)
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
connected = False
from time import sleep
sleep(0.05)
while not connected:
    try:
        s.connect((host,port))
        connected = True
    except Exception as e:
        pass
while True:
    size = int(s.recv(2))
    print("SIze:", size)
    xs = pickle.loads(s.recv(size))
    string = "Recieved: " + str(xs)
    print(string)
    sstring = pickle.dumps(string)
    print(str(len(sstring)).encode())
    s.send(str(len(sstring)).encode())
    s.send(sstring)
