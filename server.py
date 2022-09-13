import socket 
import threading
import hashlib
import random
from xmlrpc.client import Server

n = 5
c = 0
HEADER = 1024
localIP     = "127.0.0.1"
localPort   = 20001
bufferSize  = 1024
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
data = []
ports = []
socket_list_tcp = []
socket_list_tcp_2 = []
socket_list_udp = []
socket_list_udp_2 = []
# address pair is also needed for udp

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

    t = str(n)
    for i in range(n):
        temp1 = str(random.randint(1024, 49000))
        temp2 = str(random.randint(1024, 49000))
        ports.append((int(temp1), int(temp2)))
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

def handle_request(client_id, packet_id):
    global n

    for i in range(n):
        if(i == client_id): continue
        message = "please send packet_id"
        # socket_list_udp_2[i]     

def handle_client(port1, port2):
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
    TCPServerSocket.listen(1)

    print(f"TCP server up with port no {port1} and {localIP}")

    connectionSocket, addr = TCPServerSocket.accept()
    socket_list_tcp.append(connectionSocket)


    TCPServerSocket_2 = socket.socket(family=socket.AF_INET, type=socket.SOCK_STREAM)
    TCPServerSocket_2.bind((localIP, port2))
    TCPServerSocket_2.listen(1)
    connectionSocket_2, addr = TCPServerSocket_2.accept()
    socket_list_tcp_2.append(connectionSocket_2)

    UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket.bind((localIP, port1))
    socket_list_udp.append(UDPServerSocket)

    UDPServerSocket_2 = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    UDPServerSocket_2.bind((localIP, port2))
    socket_list_udp_2.append(UDPServerSocket_2)


    for i in range(num_packets):
        temp = str(c1) + " " + data[c1]
        connectionSocket.send(temp.encode())
        c1+=1
        print(temp + " data send\n")
        # print("\n")
    
    connectionSocket.send(str(-1).encode())

    print("connection closed with ",addr)

    connectionSocket.close()

# connect to n clients using threads
def start():
    for i in range(n):
        thread = threading.Thread(target=handle_client, args=(ports[i]))
        thread.start()

def main():
    global data

    for piece in read_file(open("./A2_small_file.txt", 'r')):
        data.append(hashlib.md5(piece.encode()).hexdigest())

    initial_send()
    start()

main()

# #initial connection to transfer ports
# def initial_send():

#     server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#     server.bind((SERVER, 20002))
#     server.listen(1)

#     connectionSocket, addr = server.accept()

#     print("Connection Established")

#     t = str(n)
#     for i in range(2*n):
#         t += " " + str(random.randint(1024, 49000))
#     print(t)

#     connectionSocket.send(t.encode())
#     connectionSocket.close()
    
# def main():
#     while True:
#         initial_send()

# main()


# def handle_client(conn, addr):
#     print(f"[NEW CONNECTION] {addr} connected.")

#     connected = True
#     while connected:
#         msg_length = conn.recv(HEADER).decode(FORMAT)
#         if msg_length:
#             msg_length = int(msg_length)
#             msg = conn.recv(msg_length).decode(FORMAT)
#             if msg == DISCONNECT_MESSAGE:
#                 connected = False

#             print(f"[{addr}] {msg}")
#             conn.send("Msg received".encode(FORMAT))

#     conn.close()
        

# def start():
#     server.listen()
#     print(f"[LISTENING] Server is listening on {SERVER}")
#     while True:
#         conn, addr = server.accept()
#         thread = threading.Thread(target=handle_client, args=(conn, addr))
#         thread.start()
#         print(f"[ACTIVE CONNECTIONS] {threading.activeCount() - 1}")


# print("[STARTING] server is starting...")
# start()

