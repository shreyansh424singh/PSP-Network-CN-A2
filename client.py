import socket
import threading
import time
import random
# from collections import OrderedDict

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
    print("client ports")
    print(client_ports)
    print("server ports")
    print(server_ports)

# ask missing chunks from server
def ask_query(udp_socket: socket.socket, tcp_socket: socket.socket, index: int):
    global client_data

    udp_socket.settimeout(1)
    x = -1
    y = 0

    print(client_data)


    while (len(client_data[index]) < data_size):
        # x = random.randint(0, data_size-1)
        x = (x+1) % data_size

        print(f"client {index} cache has {client_data[index].get(x)} for packet no {x} ")

        # if data is present with client
        if client_data[index].get(x) != None: continue  


        # when data is not present, select a port at random to request
        temp = "Chunk_Request " + str(x) + " " + str(index) + " "
        
        msgFromServer = ()
        def try_reuest():
            nonlocal msgFromServer, y
            print(temp)
            # select random server port
            y = random.randint(0, n-1)
            udp_socket.sendto(temp.encode(), (SERVER, server_ports[y][0]))
            try:
                # ack from server
                msgFromServer = udp_socket.recvfrom(1024)
                print(msgFromServer[0].decode())
            except:
                print(f"Requesting for ack client {index} packet {x} ")
                try_reuest()
        try_reuest()
        while(msgFromServer[0].decode() != "Chunk_Request_Ack"):
            print("UDP ask ACK ERROR")
            try_reuest()
        print(msgFromServer)



        # new TCP connection
        new_tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        new_tcp_sock.connect((SERVER, server_ports[y][0]))

        # send ack to server using tcp
        temp = "Chunk_Request_Ack_Ack " + str(index) + " "
        new_tcp_sock.send(temp.encode())

        new_tcp_sock.settimeout(10)
        try:
            temp =  new_tcp_sock.recv(1024).decode().split('#')
            client_data[index][x] = temp[0] 
            print(f"Chunk_Request {x} by {index} portno: {server_ports[y][0]} Send and recieved {temp}")
        except:
            print("Chunk_Request_Ack_Ack Send but Not recieved Data")

        new_tcp_sock.send("OK".encode())

        new_tcp_sock.close()

    # need to send ack to server that all chunks recieved

    print(f"Client {index} has recieved all data")

    print(client_data[index])

    # udp_socket.close()
    # tcp_socket.close()


# answer queries from the server
def ans_query(tcp_socket: socket.socket, index: int):
    global client_data

# make new udp socket
    udp_socket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    udp_socket.bind((SERVER,client_ports[index][1]))

    print(f"new udp socket made by client{index} on port {client_ports[index][1]} ")

    msgFromServer, addr = udp_socket.recvfrom(1024)

    udp_socket.sendto("Request recieved".encode(), addr)


    temp = (msgFromServer.decode()).split()

    print(f" query recieved by client: {index} port no: {client_ports[index][1]} is {temp} address {addr} ")

    if(temp[0] != "Chunk_Request_S"): 
        print(f"Error in recieving query {temp}")
        ans_query(tcp_socket, index)

    data_send = ""
    if client_data[index].get(int(temp[1])) == None:    
        data_send = "Not_Present"
    else:   
        data_send = client_data[index].get(int(temp[1]))

# close udp socket
    udp_socket.close()

    print(f"data: {data_send} packetno: {temp[1]} to addr: {addr} ")

    new_tcp_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    new_tcp_sock.connect(addr)

    new_tcp_sock.send(data_send.encode())

    message = new_tcp_sock.recv(1024).decode()
    if(message != "OK"): print(f"some error in answering query messaage : {message} ")

    new_tcp_sock.close()




    # tcp_socket.send(data_send.encode())

    # message = tcp_socket.recv(1024).decode()
    # if(message != "OK"): print("some error in answering query")


    # infinite loop
    # have an ack from server to close this
    ans_query(tcp_socket, index)



def handle(p1, p2, index):
    global client_data
    (port1, port2) = p1
    (port3, port4) = p2

    # client_data = {}

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

    client_tcp_2 = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    def tcp_conn1():
        try:
            client_tcp_2.connect((SERVER, port2))
            print(f"connected with port no: {port2}")
        except:
            time.sleep(1)
            tcp_conn1()
    tcp_conn1()

    client_udp_1 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    client_udp_1.bind((SERVER,port3))

    # client_udp_2 = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    # client_udp_2.bind((SERVER,port4))
    print(f"port3 {port3} port4 {port4} ")

    # initial data for clients
    incoming_data = client_tcp_1.recv(2048*n).decode()

    temp = incoming_data.split()
    i = 0
    while i<len(temp):
        client_data[index][int(temp[i])] = temp[i+1]
        i+=2

    # for i in range(len(client_data)):
    #     print(client_data[i])


    client_thread_1 = threading.Thread(target=ask_query, args=(client_udp_1, client_tcp_1, index))
    client_thread_2 = threading.Thread(target=ans_query, args=(client_tcp_2, index))
    client_thread_1.start()
    client_thread_2.start()






# connect to n clients using threads
def start():
    for i in range(n):
        thread = threading.Thread(target=handle, args=(server_ports[i], client_ports[i], i))
        thread.start()

def main(): 
    global client_data
    initial_rec()
    client_data = [dict() for x in range(n)]
    print(n)
    print(data_size)
    start()

main()