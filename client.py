import hashlib
import socket
import threading
import time
import random

HEADER = 64
FORMAT = 'utf-8'
DISCONNECT_MESSAGE = "!DISCONNECT"
SERVER = "127.0.0.1"

n = 0
data_size = 0
server_ports = []
client_ports = []
client_data = []

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
    #print("client ports")
    #print(client_ports)
    #print("server ports")
    #print(server_ports)

# ask missing chunks from server
def ask_query(udp_socket: socket.socket, tcp_socket: socket.socket, index: int):
    global client_data

    udp_socket.settimeout(1)
    x = 0
    y = 0

    while (len(client_data[index]) < data_size):
        x = random.randint(0, data_size-1)

        # if data is present with client
        if client_data[index].get(x) != None: continue  

        #print(f"client {index} has {len(client_data[index])} chunks ")

        # when data is not present, select a port at random to request
        temp = "Chunk_Request " + str(x) + " "+ str(index) + " "
        #print(f"{temp} ")

        msgFromServer = ()
        def try_reuest():
            nonlocal msgFromServer, y
            y = random.randint(0, n-1)
            udp_socket.sendto(temp.encode(), (SERVER, server_ports[index][0]))
            try:
                # ack from server
                msgFromServer = udp_socket.recvfrom(1024)
            except:
                try_reuest()
        try_reuest()
        while(msgFromServer[0].decode() != "Chunk_Request_Ack"):
            try_reuest()

        # send ack to server using tcp
        temp = "Chunk_Request_Ack_Ack " + str(x) + " "
        tcp_socket.send(temp.encode())

        tcp_socket.settimeout(2)
        try:
            temp =  tcp_socket.recv(2048).decode().split('#')

            #print(f"!@!@!@!@!@!@!@!@!@!@!@!@!@!@! {temp}")

            if(temp[0] != "Retry" or temp[1] != "Retry"): 
                client_data[index][int(temp[0])] = temp[1] 
                # #print(f"Client {index} for query {temp[0]} {x}  recieved {temp[1]}")
                # if(x != int(temp[0])): print("##########################################")
        except:
            xs = 0
            #print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

        tcp_socket.send("OK".encode())
        #print("OK")

    # need to send ack to server that all chunks recieved

    #print(f"Client {index} has recieved all data")


    temp = "Done Client " + str(index)

    udp_socket.sendto(temp.encode(), (SERVER, server_ports[index][0]))    

    filename = "client" + str(index) + ".txt"
    complete_data = ""
    for i in range(data_size):
        complete_data += client_data[index][i]

    print(f"md5 hash of client {index} {hashlib.md5(complete_data.encode()).hexdigest()}")
    #print(client_data[index])

    f = open(filename, "w")
    f.write(complete_data)
    f.write(hashlib.md5(complete_data.encode()).hexdigest())
    f.close()

    tcp_socket.close()
    udp_socket.close()
    return


# answer queries from the server
def ans_query(udp_socket: socket.socket, tcp_socket: socket.socket, index: int):
    global client_data

    msgFromServer, addr = udp_socket.recvfrom(1024)
    # ack
    udp_socket.sendto("Request recieved".encode(), addr)

    temp = (msgFromServer.decode()).split()

    if(temp[0] == "Close"):
        print("hey")
        tcp_socket.close()
        udp_socket.close()
        return

    if(temp[0] != "Chunk_Request_S"): 
        ans_query(udp_socket, tcp_socket, index)

    data_send = ""
    if client_data[index].get(int(temp[1])) == None:    
        data_send = "Not_Present#"
    else:   
        data_send = client_data[index].get(int(temp[1]))

    try:
        tcp_socket.send(data_send.encode())
    except:
        return

    tcp_socket.settimeout(1)
    message = ""
    try:
        message = tcp_socket.recv(1024).decode()
    except:
        if(message != "OK"): 
            _ = 0

    # infinite loop
    # have an ack from server to close this
    ans_query(udp_socket, tcp_socket, index)



def handle(p1, p2, index):
    global client_data
    (port1, port2) = p1
    (port3, port4) = p2


    client_tcp_1 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def tcp_conn():
        try:
            client_tcp_1.connect((SERVER, port1))
        except:
            tcp_conn()
    tcp_conn()

    client_tcp_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def tcp_conn1():
        try:
            client_tcp_2.connect((SERVER, port2))
        except:
            tcp_conn1()
    tcp_conn1()

    client_udp_1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_udp_1.bind((SERVER,port3))

    client_udp_2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_udp_2.bind((SERVER,port4))

     # initial data for clients
    num_packets = client_tcp_1.recv(1024).decode()
    #print(num_packets)
    client_tcp_1.send("ok".encode())
    for i in range(int(num_packets)):
        incoming_data = client_tcp_1.recv(2048).decode()
        client_tcp_1.send("ok".encode())
        temp = incoming_data.split('#')
        #print(temp)
        client_data[index][int(temp[0])] = temp[1]

    client_tcp_1.send("done".encode())

    client_thread_1 = threading.Thread(target=ask_query, args=(client_udp_1, client_tcp_1, index))
    client_thread_2 = threading.Thread(target=ans_query, args=(client_udp_2, client_tcp_2, index))
    client_thread_1.start()
    client_thread_2.start()

    # client_tcp_1.close()
    # client_tcp_2.close()
    # client_udp_1.close()
    # client_udp_2.close()


# connect to n clients using threads
def start():
    for i in range(n):
        thread = threading.Thread(target=handle, args=(server_ports[i], client_ports[i], i))
        thread.start()

def main(): 
    global client_data
    initial_rec()
    client_data = [dict() for x in range(n)]
    #print(n)
    #print(data_size)
    start()

main()