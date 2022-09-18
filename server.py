import socket
import string 
import threading
import hashlib
import random
import time
import concurrent.futures
from collections import OrderedDict

n = 5
total_recieved = 0
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
lock = threading.Lock()

class LRUCache:

 # initialising capacity
 def __init__(self, capacity: int):
  self.cache = OrderedDict()
  self.capacity = capacity

 def get(self, key: int) -> string:
  lock.acquire()
  if key not in self.cache:
   lock.release()
   return "-1"
  else:
   self.cache.move_to_end(key)
   lock.release()
   return self.cache[key]

 def put(self, key: int, value: string) -> None:
  lock.acquire()
  self.cache[key] = value
  self.cache.move_to_end(key)
  if len(self.cache) > self.capacity:
   self.cache.popitem(last = False)
  lock.release()  

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

    # randomly select ports for server and clients
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
    
    # send initial data to client
    # initial data includes number of clients and number of chunks in file and all ports to be used
    while True:
        connectionSocket.send(t.encode())

        message = connectionSocket.recv(bufferSize).decode()
        message = int(message)
        
        if message == -1:
            print("connection closed with ",addr)
            connectionSocket.close()
            break


# Brodcast request to all clients and return data
def handle_request(client_id: int, packet_id: int, UDPServerSocket_2: socket.socket) -> string:
    global n, busy

    # busy = True

    print(f"handle request of client {client_id} for packet {packet_id} ")

    UDPServerSocket_2.settimeout(1)

    for i in range(n):

        print(f"{i} i and client id {client_id} ")
        if(i == client_id): continue
        message = "Chunk_Request_S  " + str(packet_id) + " "
        UDPServerSocket_2.sendto(message.encode(), (localIP, client_ports[i][1]))
        print(f"query: {message} send to client {i} port no {localIP} {client_ports[i][1]} ")
        
        # ack from client
        try:
            msgRecieved, _ = UDPServerSocket_2.recvfrom(1024)
            print(f"Good {msgRecieved} query: {message} send to client {i}")
        except:
            continue

        try:
            data_recieved = socket_list_tcp_2[i].recv(1024).decode().split('#')
            print(f"Data recieved from client {i} for packet {packet_id} is {data_recieved} ")
        except:
            print(f"Chunk_Request_S for {packet_id} send to {i} but nothing recieved. Retrying....")
            # i-=1
            continue
        socket_list_tcp_2[i].send("OK".encode())

        if(data_recieved[0] == "Not_Present"):
            continue
        else:
            busy = False
            return data_recieved[0]

    busy = False
    return "Retry"


# handles queries from client, brodcast to all clients then send back to client
def handle_client(index: int, TCPServerSocket_1: socket.socket, TCPServerSocket_2: socket.socket, UDPServerSocket_1: socket.socket, UDPServerSocket_2: socket.socket):
    global data, total_recieved

    # listen query from clients
    client_req, _ = UDPServerSocket_1.recvfrom(1024)
    request = client_req.decode().split()
    
    print(f"Server recieved request from Client {request}")

    if(request[0] == "Done"):
        total_recieved += 1
        return

    if(request[0] != "Chunk_Request"): print(f"Some error in chunk reuest {request}")

    temp = "Chunk_Request_Ack"
    UDPServerSocket_1.sendto(temp.encode(), (localIP, client_ports[index][0]))

    print(f"Chunk_Request_Ack send for {request[1]}")

    # check ack from client using tcp and timeout , recurse
    TCPServerSocket_1.settimeout(1)
    try:
        temp = TCPServerSocket_1.recv(1024).decode().split()
    except:
        print("Chunk_Request_Ack but not recieved Ack via TCP")
        handle_client(index, TCPServerSocket_1, TCPServerSocket_2, UDPServerSocket_1, UDPServerSocket_2)
        
    print(f"{temp} recieved via tcp for {request[1]}")

    # check cache
    data_to_send = cache.get(int(request[1]))
    # if not in cache brodcast request to all clients
    # and update cache



    with concurrent.futures.ThreadPoolExecutor() as executor:
        future = executor.submit(handle_request, int(request[2]), int(temp[1]), UDPServerSocket_2)
        data_to_send = future.result()
        print(data_to_send)



    # if(data_to_send == "-1"):
        # data_to_send = handle_request(int(request[2]), int(temp[1]), UDPServerSocket_2)






    print(f" {temp} {request[1]} ")

    data_to_send = data[int(temp[1])]
    cache.put(int(temp[1]), data_to_send)
    data_to_send = "#" + data_to_send + "#"
    data_to_send = request[1] + data_to_send
    try:
        TCPServerSocket_1.send(data_to_send.encode())
    except:
        TCPServerSocket_1.close()
        return
    print(f"chunk :{request[1]} send to client {request[2]} {data_to_send}")

    try:
        # ack from client
        _ =  TCPServerSocket_1.recv(1024)
    except:
        TCPServerSocket_1.send(data_to_send.encode())
        print("ERROR no tcp ack recieved")

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
    connectionSocket_2.settimeout(1)

    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket.bind((localIP, port1))

    UDPServerSocket_2 = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket_2.bind((localIP, port2))

    temp = ""
    for i in range(num_packets):
        temp += str(c1) + " " + data[c1] + " "
        c1+=1

    # send initial data to clients
    connectionSocket.send(temp.encode())

    # handles queries from client, brodcast to all clients then send back to client
    handle_client(client_id, connectionSocket, connectionSocket_2, UDPServerSocket, UDPServerSocket_2)

    if(total_recieved == n):
        message = "Close "
        for i in range(n):
            UDPServerSocket_2.sendto(message.encode(), (localIP, client_ports[i][1]))
        TCPServerSocket.close()
        TCPServerSocket_2.close()
        connectionSocket.close
        connectionSocket_2.close
        UDPServerSocket.close()
        UDPServerSocket_2.close()


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
    # for piece in read_file(open("./test1.txt", 'r')):
        data.append(hashlib.md5(piece.encode()).hexdigest())

    initial_send()
    start()

main()