#[20000, 20000+n] - UDP port on server for receving messages
#[21000, 21000+n] - UDP port on client for receving messages
#[22000, 22000+n] - UDP port on server for sending messages
#[23000, 23000+n] - UDP port on client for sending messages
#[24000, 24000+n] - TCP port for broadcasts
#[25000, 25000+n] - TCP port for requests
# 26000 - TCP Connection to convey that the transfer is done
#[27000, 27000+n] - TCP port for important communication between server and client
#[28000, 28000+n] - TCP port for important communication between client and server

import chunk
from http import client, server
import client_functions2
import time
import threading
import sys
from socket import *

bufferSize          = 2048
localIP = "127.0.0.1"
timeout = 0.5
n = 5
offset = 5
ports = [19000+offset, 20000+offset, 21000+offset, 22000+offset, 23000+offset, 24000+offset, 25000+offset, 26000+offset, 27000+offset, 28000+offset]
queue = []
for i in range(n):
    queue.append([])
exit = []
for i in range(n):
    exit.append(False)
correct_hash = '9f9d1c257fe1733f6095a8336372616e'
#correct_hash = '89e57cef9c27f8b45cbb37f958dea193'

control_sock = socket(family=AF_INET, type=SOCK_STREAM)
control_sock.connect((localIP, ports[7]))
control_sock.send("hi".encode())
control_sock.recv(bufferSize).decode()
timing = {}
timing2 = {}
done = True


def send_all_requests(n, sockets, dont_have, bufferSize):
    def send_requests(sock, missing, bufferSize):
        global timing
        connectionSocket, addr = sock.accept()
        connectionSocket.recv(bufferSize).decode()
        for j in range(len(missing)):
            connectionSocket.send(missing[j].encode())
            connectionSocket.recv(bufferSize).decode()
            #timing[missing[j]] = time.time()
        connectionSocket.send("done".encode())
        connectionSocket.recv(bufferSize).decode()    

    for i in range(n):
        thread = threading.Thread(target=send_requests, args=(sockets[i], dont_have[i], bufferSize,))
        thread.start()

request_sockets = client_functions2.get_tcp_sockets(ports[6], n, localIP)
client_send_sockets = client_functions2.get_udp_sockets(ports[4], n, localIP)
client_receive_sockets = client_functions2.get_udp_sockets(ports[2], n, localIP)
tcp_communication_sockets = client_functions2.get_tcp_sockets(ports[8], n, localIP)

chunks, dont_have = client_functions2.get_chunks_from_server(bufferSize, n, localIP, ports[0])
begin = time.time()
timing = send_all_requests(n, request_sockets, dont_have, bufferSize)

def receive_broadcast(i, port, bufferSize):
    TCPClientSocket = socket(family=AF_INET, type=SOCK_STREAM)
    TCPClientSocket.connect((localIP, port+i))
    TCPClientSocket.send("hi".encode())

    global queue
    while True:
        id = TCPClientSocket.recv(bufferSize).decode()
        TCPClientSocket.send("ACK".encode())
        client_number = TCPClientSocket.recv(bufferSize).decode()
        TCPClientSocket.send("ACK".encode())
        if id=="done":
            break
        elif int(client_number)!=i:
            queue[i].append([id, client_number])
        #3 bit id,  1 bit client number

threads = []
for i in range(n):
    thread = threading.Thread(target=receive_broadcast, args=(i, ports[5], bufferSize,))
    thread.start()
    threads.append(thread)
#for i in range(n):
 #   threads[i].join()

def send_via_udp(udp_sock, server_add, chunk, connectionSocket, bufferSize):
    message = ''
    while message!="ACK":
        udp_sock.sendto(chunk.encode(), server_add)
        msg, addr = udp_sock.recvfrom(bufferSize)
        message = msg.decode() 
    connectionSocket.send("ACK".encode())
    connectionSocket.recv(bufferSize).decode()

def send_missing_chunks(tcp_sock, udp_sock, server_add, i, bufferSize, chunks_with_client):
    connectionSocket, addr = tcp_sock.accept()
    connectionSocket.recv(bufferSize).decode()
    global queue
    while len(queue[i])>0:
        elem = queue[i][0]
        id = elem[0]
        client_number = elem[1]            #the client who wants it
        l = list(chunks_with_client.keys())
        if id in l:
            chunk = chunks_with_client[id]
            send_via_udp(udp_sock, server_add, chunk, connectionSocket, bufferSize)
            connectionSocket.send(client_number.encode())
            connectionSocket.recv(bufferSize).decode()
            connectionSocket.send(id.encode())
            connectionSocket.recv(bufferSize).decode()
        queue[i].pop(0)
    send_via_udp(udp_sock, server_add, "done", connectionSocket, bufferSize)

threads = []
for i in range(n):
    thread = threading.Thread(target=send_missing_chunks, args=(tcp_communication_sockets[i], client_send_sockets[i], (localIP, ports[1]+i), i, bufferSize, chunks[i],))
    thread.start()
    threads.append(thread)
#for i in range(n):
 #   threads[i].join()

def wait_for_confirmation(sock, bufferSize, i):
    global exit
    msg = sock.recv(bufferSize).decode()
    sock.send("ACK".encode())
    exit[i] = True

def get_missing_chunks(port, i, udp_socket):
    TCPClientSocket = socket(family=AF_INET, type=SOCK_STREAM)
    TCPClientSocket.connect((localIP, port))
    global chunks
    TCPClientSocket.send("hi".encode())
    hash = client_functions2.hash(i, chunks[i])
    while hash!=correct_hash:
        thread = threading.Thread(target=wait_for_confirmation, args=(TCPClientSocket, bufferSize, i,))
        thread.start()
        message = ''
        while exit[i]==False:
            message, addr = udp_socket.recvfrom(bufferSize) 
            udp_socket.sendto("ACK".encode(), addr)
            time.sleep(0.002)
        
        exit[i] = False
        chunk = message.decode()
        global timing2
        chunk_id = TCPClientSocket.recv(bufferSize).decode()
        #timing2[chunk_id] = time.time()
        TCPClientSocket.send("ACK".encode())
        chunks[i][chunk_id] = chunk
        hash = client_functions2.hash(i, chunks[i])

threads = []
for i in range(n):
    thread = threading.Thread(target=get_missing_chunks, args=(ports[9]+i,i, client_receive_sockets[i]))
    thread.start()
    threads.append(thread)
for i in range(n):
    threads[i].join()

control_sock.send("done".encode())

digests = client_functions2.assemble(chunks, n)
for i in range(n):
    print(digests[i])