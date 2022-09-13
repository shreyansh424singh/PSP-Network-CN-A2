from contextlib import nullcontext
import socket
import threading
import time

HEADER = 64
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = "127.0.0.1"

n = 0
data_size = 0
server_ports = []
client_ports = []

#initial connection ot recieve ports
def initial_rec():
    global n, data_size

    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect((SERVER, 20001))

    ports_data = (client.recv(4096).decode(FORMAT)).split()
    n = int(ports_data[0])
    data_size = int(ports_data[1])

    for i in range(2*n):
        if(i<n): server_ports.append((int(ports_data[2*i+2]), int(ports_data[2*i+3])))
        else:    client_ports.append((int(ports_data[2*i+2]), int(ports_data[2*i+3])))

    client.send(str(-1).encode())
    client.close()
    print(server_ports)

def handle(p1, p2):
    (port1, port2) = p1
    (port3, port4) = p2
    client_data = {}

    print(SERVER + " " + str(port1))

    client_tcp_1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def tcp_conn():
        try:
            client_tcp_1.connect((SERVER, port1))
            print(f"connected with port no: {port1}")
        except:
            time.sleep(1)
            tcp_conn()
    tcp_conn()

    client_udp_1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_udp_1.bind((SERVER,port3))

    client_udp_2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_udp_2.bind((SERVER,port4))

    # initial data for clients
    incoming_data = client_tcp_1.recv(2048*n).decode()

    temp = incoming_data.split()
    i = 0
    while i<len(temp):
        client_data[int(temp[i])] = temp[i+1]
        i+=2

    client_tcp_1.close()
    print(client_data)

# connect to n clients using threads
def start():
    for i in range(n):
        thread = threading.Thread(target=handle, args=(server_ports[i], client_ports[i]))
        thread.start()

def main(): 
    initial_rec()
    print(n)
    start()

main()