import collections
import socket
import ctypes
import plasma

DEFAULT_PORT = 16121

Connection = collections.namedtuple("Connection", ["address", "port"])

# list of IP addresses and ports of managers
object_managers = []

def send_addresses(conn, object_managers):
  manager = plasma.PlasmaManager(conn.address, conn.port)
  for (manager_id, object_manager) in enumerate(object_managers):
    manager.register(manager_id, object_manager.address, object_manager.port)

if __name__ == '__main__':
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  sock.bind(('', DEFAULT_PORT))
  sock.listen(5)
  
  while True:
    (client, address) = sock.accept()
    request = plasma.PlasmaRequest()
    client.recv_into(request)
    address = ".".join(map(str, request.addr[:]))
    conn = Connection(address=address, port=request.port)
    print "object manager " + str(conn) + " connected"
    object_managers.append(conn)
    for c in object_managers:
      send_addresses(c, object_managers)
    
