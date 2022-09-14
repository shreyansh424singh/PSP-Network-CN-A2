import socket
import string 
import threading
import hashlib
import random
import time
from collections import OrderedDict

n = 5
c = 0
HEADER = 1024
localIP     = "127.0.0.1"
localPort   = 20001
bufferSize  = 1024
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
data = []
server_ports = []
client_ports = []

socket_list_tcp = []
socket_list_tcp_2 = []

busy = False

class LRUCache:

 # initialising capacity
 def __init__(self, capacity: int):
  self.cache = OrderedDict()
  self.capacity = capacity

 def get(self, key: int) -> string:
  if key not in self.cache:
   return "-1"
  else:
   self.cache.move_to_end(key)
   return self.cache[key]

 def put(self, key: int, value: string) -> None:
  self.cache[key] = value
  self.cache.move_to_end(key)
  if len(self.cache) > self.capacity:
   self.cache.popitem(last = False)

def read_file(f):
    while True:
        data = f.read(1024)
        if not data:
            break
        yield data

#initial connection to transfer ports
def initial_send():
    TCPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket.bind((localIP, localPort))
    TCPServerSocket.listen(1)
    print("TCP server up and listening")

    connectionSocket, addr = TCPServerSocket.accept()

    t = str(n) + " " + str(len(data))
    for i in range(n):
        temp1 = str(random.randint(1024, 49000))
        temp2 = str(random.randint(1024, 49000))
        server_ports.append((int(temp1), int(temp2)))
        t += " " + temp1 + " " + temp2
    for i in range(n):
        temp1 = str(random.randint(1024, 49000))
        temp2 = str(random.randint(1024, 49000))
        client_ports.append((int(temp1), int(temp2)))
        t += " " + temp1 + " " + temp2
    # print(t)
    
    while True:
        connectionSocket.send(t.encode())

        message = connectionSocket.recv(bufferSize).decode()
        message = int(message)
        
        if message == -1:
            print("connection closed with ",addr)
            connectionSocket.close()
            break


# Brodcast request to all clients and return data
def handle_request(client_id: int, packet_id: int, TCPServerSocket_2: socket.socket, UDPServerSocket_2: socket.socket) -> string:
    global n, busy
    busy = True

    for i in range(n):
        if(i == client_id): continue
        message = "Chunk_Request_S  " + str(packet_id) + " "
        UDPServerSocket_2.sendto(message.encode(), (localIP, client_ports[i][1]))
        try:
            data_recieved = socket_list_tcp_2[i].recv(1024).decode()
        except:
            print(f"Chunk_Request_S send to {i} but nothing recieved. Retrying....")
            i-=1
            continue
        socket_list_tcp_2[i].send("OK".encode())
        if(data_recieved == "Not_Present"):
            continue
        else:
            return data_recieved

    busy = False
    print("ERROR no client has data")
    return ""


# handles queries from client, brodcast to all clients then send back to client
def handle_client(index: int, TCPServerSocket_1: socket.socket, TCPServerSocket_2: socket.socket, UDPServerSocket_1: socket.socket, UDPServerSocket_2: socket.socket):
    global data

    # listen query from clients
    client_req, _ = UDPServerSocket_1.recvfrom(1024)
    request = client_req.decode().split()
    
    print(f"Server recieved request from Client {request}")

    if(request[0] != "Chunk_Request"): print(f"Some error in chunk reuest {request}")

    temp = "Chunk_Request_Ack"
    UDPServerSocket_1.sendto(temp.encode(), (localIP, client_ports[index][0]))

    print(f"Chunk_Request_Ack send for {request[1]}")

    # check ack from client using tcp and timeout , recurse
    TCPServerSocket_1.settimeout(1)
    try:
        temp = TCPServerSocket_1.recv(1024).decode()
    except:
        time.sleep(1)
        print("Chunk_Request_Ack but not recieved Ack via TCP")
        handle_client(index, TCPServerSocket_1, TCPServerSocket_2, UDPServerSocket_1, UDPServerSocket_2)
        
    print(f"{temp} recieved via tcp for {request[1]}")


    data_to_send = data[int(request[1])]
    TCPServerSocket_1.send(data_to_send.encode())
    print(f"{request[1]} Data send: {data_to_send}")




    # # check cache
    # data_to_send = cache.get(int(request[1]))
    # # if not in cache brodcast request to all clients
    # # and update cache
    # if(data_to_send == "-1"):
    #     while busy == True:
    #         time.sleep(1)
    #     data_to_send = handle_request(index, int(request[1]), TCPServerSocket_2, UDPServerSocket_2)
    #     cache.put(int(request[1]), data_to_send)

    # # send data back to requesting client
    # TCPServerSocket_1.send(data_to_send.encode())
    # print(f"{request[1]} Data send: {data_to_send}")





    # recurse
    handle_client(index, TCPServerSocket_1, TCPServerSocket_2, UDPServerSocket_1, UDPServerSocket_2)


def send_chunks(port1, port2):
    global c, data, socket_list_tcp, socket_list_tcp_2, socket_list_udp, socket_list_udp_2

    client_id = c

    c+=1
    num_packets = 0
    if(c > len(data)%n): num_packets = int(len(data)/n)
    else: num_packets = int(len(data)/n) + 1

    c1 = int(len(data)/n) * (c-1) + min(c-1, len(data)%n)

    print(str(c) + " jvb " + str(num_packets) + " " + str(len(data)) + '\n')

    TCPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket.bind((localIP, port1))
    TCPServerSocket.listen(n)

    print(f"TCP server up with port no {port1} and {localIP}")

    connectionSocket, _ = TCPServerSocket.accept()
    socket_list_tcp.append(connectionSocket)


    TCPServerSocket_2 = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket_2.bind((localIP, port2))
    TCPServerSocket_2.listen(n)
    connectionSocket_2, _ = TCPServerSocket_2.accept()
    socket_list_tcp_2.append(connectionSocket_2)
    connectionSocket_2.settimeout(2)

    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket.bind((localIP, port1))

    UDPServerSocket_2 = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket_2.bind((localIP, port2))

    temp = ""
    for i in range(num_packets):
        temp += str(c1) + " " + data[c1] + " "
        c1+=1
    # print(f"data send {temp}")

    # send initial data to clients
    connectionSocket.send(temp.encode())

    # handles queries from client, brodcast to all clients then send back to client
    handle_client(client_id, connectionSocket, connectionSocket_2, UDPServerSocket, UDPServerSocket_2)

    # print("connection closed with ",addr)
    # connectionSocket.close()

# connect to n clients using threads
def start():
    for i in range(n):
        thread = threading.Thread(target=send_chunks, args=(server_ports[i]))
        thread.start()

cache = LRUCache(0)

def main():
    global data, cache
    cache = LRUCache(n)

    for piece in read_file(open("./A2_small_file.txt", 'r')):
        data.append(hashlib.md5(piece.encode()).hexdigest())

    initial_send()
    start()

main()