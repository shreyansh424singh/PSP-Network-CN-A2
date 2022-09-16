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

listen_sck_1 = []
listen_sck_2 = []

busy = False
lock = threading.Lock()

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

    print("client ports")
    print(client_ports)
    
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
    # lock.acquire()
    print(busy)
    while busy == True:
        print("wrong")
        time.sleep(1)
    busy = True

    print(f"handle request of client {client_id} for packet {packet_id} ")

    UDPServerSocket_2.settimeout(2)

    i = -1
    while i<n-1:
        i+=1
        print(i)
        if(i == client_id): continue
        message = "Chunk_Request_S  " + str(packet_id) + " "
        print(message)
        UDPServerSocket_2.sendto(message.encode(), (localIP, client_ports[i][1]))
        print(f"query: {message} send to client {i} port no {client_ports[i][1]} ")

        try:
            msgRecieved, _ = UDPServerSocket_2.recvfrom(1024)
            print(f"Good {msgRecieved} ")
        except:
            i-=1
            continue

        connectionSocket, addr = TCPServerSocket_2.accept()
        print(f"new TCP connection for ans query with {addr} ")

        try:
            data_recieved = connectionSocket.recv(1024).decode()
            print(f"Data recieved from client {i} for packet {packet_id} is {data_recieved} ")
        except:
            print(f"Chunk_Request_S for {packet_id} send to {i} but nothing recieved. Retrying....")
            i-=1
            continue
        connectionSocket.send("OK".encode())
        if(data_recieved == "Not_Present"):
            continue
        else:
            # lock.release()
            busy = False
            connectionSocket.close()
            return data_recieved
        
    connectionSocket.close()






        # try:
        #     data_recieved = socket_list_tcp_2[i].recv(1024).decode()
        #     print(f"Data recieved from client {i} for packet {packet_id} is {data_recieved} ")
        # except:
        #     print(f"Chunk_Request_S for {packet_id} send to {i} but nothing recieved. Retrying....")
        #     i-=1
        #     continue
        # socket_list_tcp_2[i].send("OK".encode())
        # if(data_recieved == "Not_Present"):
        #     continue
        # else:
        #     # lock.release()
        #     busy = False
        #     return data_recieved





    busy = False
    # lock.release()
    print("ERROR no client has data")
    return "ERROR no client has data"


# handles queries from client, brodcast to all clients then send back to client
def handle_client(index: int, TCPServerSocket: socket.socket, TCPServerSocket_2: socket.socket, UDPServerSocket_1: socket.socket, UDPServerSocket_2: socket.socket):
    global data

    # listen query from clients
    client_req, address = UDPServerSocket_1.recvfrom(1024)
    request = client_req.decode().split()
    
    print(f"Server recieved request from Client {request}")

    if(request[0] != "Chunk_Request"): print(f"Some error in chunk reuest {request}")

    temp = "Chunk_Request_Ack"
    UDPServerSocket_1.sendto(temp.encode(), address)

    print(f"Chunk_Request_Ack send for {request[1]}")




    # check ack from client using tcp and timeout , recurse
    # make new tcp connection
    connectionSocket, addr = TCPServerSocket.accept()
    print(f"new tcp connection established with {addr}")

    try:
        temp = connectionSocket.recv(1024).decode().split()
    except:
        time.sleep(1)
        print("Chunk_Request_Ack but not recieved Ack via TCP")
        handle_client(index, TCPServerSocket, TCPServerSocket_2, UDPServerSocket_1, UDPServerSocket_2)
    

# # sending data from server for testing purpose
    # data_to_send = data[int(request[1])]
    # connectionSocket.send(data_to_send.encode())
    # print(f"{request[1]} Data send: {data_to_send}")

    # data_to_send = "-1"
    # check cache
    data_to_send = cache.get(int(request[1]))


    if(data_to_send != "-1"):
        print(f"packetno: {request[1]} data: {data_to_send}")

    # if not in cache brodcast request to all clients
    # and update cache
    if(data_to_send == "-1"):
        while busy == True:
            print(busy)
            time.sleep(1)
        data_to_send = handle_request(index, int(request[1]), TCPServerSocket_2, UDPServerSocket_2)
        cache.put(int(request[1]), data_to_send)

    # send data back to requesting client
    connectionSocket.send(data_to_send.encode())
    print(f"chunk :{request[1]} send to client {request[2]} {data_to_send}")


    connectionSocket.close()



    # recurse
    handle_client(index, TCPServerSocket, TCPServerSocket_2, UDPServerSocket_1, UDPServerSocket_2)


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
    listen_sck_1.append(TCPServerSocket)

    print(f"TCP server up with port no {port1} and {localIP}")

    connectionSocket, _ = TCPServerSocket.accept()
    socket_list_tcp.append(connectionSocket)


    TCPServerSocket_2 = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket_2.bind((localIP, port2))
    TCPServerSocket_2.listen(n)
    listen_sck_2.append(TCPServerSocket_2)
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



    # print("connection closed with ",addr)
    connectionSocket.close()
    connectionSocket_2.close()



    # handles queries from client, brodcast to all clients then send back to client
    handle_client(client_id, TCPServerSocket, TCPServerSocket_2, UDPServerSocket, UDPServerSocket_2)


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