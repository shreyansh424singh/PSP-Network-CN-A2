from contextlib import nullcontext
import socket
import threading
import time

HEADER = 64
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = "127.0.0.1"

n = 0
ports = []

#initial connection ot recieve ports
def initial_rec():
    global n 

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((SERVER, 20001))

    ports_data = (client.recv(4096).decode(FORMAT)).split()
    n = int(ports_data[0])
    for i in range(n):
        ports.append((int(ports_data[2*i+1]), int(ports_data[2*i+2])))

    client.send(str(-1).encode())
    client.close()
    print(ports)

def handle(port1, port2):
    client_data = {}

    print(SERVER + " " + str(port1))

    client_tcp_1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_tcp_1.connect((SERVER, port1))

    time.sleep(5)

    client_tcp_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_tcp_2.connect((SERVER, port2))

    client_udp_1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_udp_1.connect((SERVER, port1))

    client_udp_2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_udp_2.connect((SERVER, port2))

    incoming_data = client_tcp_1.recv(2048).decode()

    print(incoming_data + " in data\n")

    while incoming_data != "-1" and incoming_data != nullcontext:
        temp = incoming_data.split()
        index = temp[0]
        val = temp[1]
        client_data[int(index)] = val
        incoming_data = client_tcp_1.recv(2048).decode()

    client_tcp_1.close()
    print(client_data)

# connect to n clients using threads
def start():
    for i in range(n):
        time.sleep(5)
        thread = threading.Thread(target=handle, args=(ports[i]))
        thread.start()

def main(): 
    initial_rec()
    print(n)
    start()

main()