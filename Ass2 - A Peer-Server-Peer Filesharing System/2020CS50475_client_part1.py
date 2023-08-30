#[19999-n, 19999]  - TCP port for initial chunks transfer from server to client
#[20999-n, 20999] - UDP ports on server for receiving requests
#[21999-n, 21999] - UDP ports on client for receiving broadcasts
#[22999-n, 22999] - UDP ports on server for sending broadcasts
#[23999-n, 23999] - UDP ports on client for sending requests
#[24999-n, 24999] - TCP port for client to server
#[25999-n, 25999] - TCP port for server to client
#26999 - TCP Connection to convey that the transfer is done

from http import client
import client_functions
import time
import threading
import sys
from socket import *

bufferSize          = 2048
localIP = "127.0.0.1"
timeout = 0.5
n = 5
queue = []
for i in range(n):
    queue.append([])
correct_hash = '9f9d1c257fe1733f6095a8336372616e'
#correct_hash = '89e57cef9c27f8b45cbb37f958dea193'

client_receive_sockets = client_functions.get_udp_sockets(21999, n, localIP)
client_send_tcp_sockets = client_functions.get_tcp_sockets(24999, n, localIP)
chunks, dont_have = client_functions.get_chunks_from_server(bufferSize, n, localIP, 19999)
client_functions.send_all_requests(n, 23999, 20999, timeout, dont_have, bufferSize)
sock1 = socket(family=AF_INET, type=SOCK_STREAM)
sock1.bind((localIP, 26999))
sock1.listen(1)

def rec_send_udp(sock, bufferSize):
    message, address = sock.recvfrom(bufferSize)
    sock.sendto("ACK".encode(), address)
    return message

def send_rec_tcp(sock, bufferSize, msg, encoding):
    sock.send(msg.encode(encoding))
    sock.recv(bufferSize).decode(encoding)

def rec_send_tcp(sock, bufferSize, encoding):
    msg = sock.recv(bufferSize).decode(encoding)
    sock.send("ACK".encode(encoding))
    return msg

def receive_broadcast(i, sock, bufferSize):
    global queue
    while True:
        #message = rec_send_udp(sock, bufferSize)
        message, address = sock.recvfrom(bufferSize)
        sock.sendto("ACK".encode(), address)
        number, address = sock.recvfrom(bufferSize)
        sock.sendto("ACK".encode(), address)
        id = message.decode()
        client_number = number.decode()
        if id=="done":
            break
        elif int(client_number)!=i:
            queue[i].append([id, client_number])
        #3 bit id,  1 bit client number

for i in range(n):
    thread = threading.Thread(target=receive_broadcast, args=(i, client_receive_sockets[i], bufferSize,))
    thread.start()

def send_missing_chunks(sock, i, bufferSize, chunks_with_client):
    connectionSocket, addr = sock.accept()
    connectionSocket.recv(bufferSize).decode()
    global queue
    while len(queue[i])>0:
        elem = queue[i][0]
        id = elem[0]
        client_number = elem[1]            #the client who wants it
        l = list(chunks_with_client.keys())
        if id in l:
            chunk = chunks_with_client[id]
            #send_rec_tcp(connectionSocket, bufferSize, chunk, "utf-16")
            connectionSocket.send(chunk.encode("utf-16"))
            connectionSocket.recv(bufferSize).decode("utf-16")
            connectionSocket.send(client_number.encode("utf-16"))
            connectionSocket.recv(bufferSize).decode("utf-16")
            connectionSocket.send(id.encode("utf-16"))
            connectionSocket.recv(bufferSize).decode("utf-16")
        queue[i].pop(0)
    connectionSocket.send("done".encode("utf-16"))

threads = []
for i in range(n):
    thread = threading.Thread(target=send_missing_chunks, args=(client_send_tcp_sockets[i], i, bufferSize, chunks[i],))
    thread.start()
    threads.append(thread)
for i in range(n):
    threads[i].join()


def get_missing_chunks(port, i):
    TCPClientSocket = socket(family=AF_INET, type=SOCK_STREAM)
    TCPClientSocket.connect((localIP, port))
    global chunks
    TCPClientSocket.send("hi".encode())
    hash = client_functions.hash(i, chunks[i])
    while hash!=correct_hash:
        #chunk = rec_send_tcp(TCPClientSocket, bufferSize, "utf-16")
        chunk = TCPClientSocket.recv(bufferSize).decode("utf-16")
        TCPClientSocket.send("ACK".encode("utf-16"))
        chunk_id = TCPClientSocket.recv(bufferSize).decode("utf-16")
        TCPClientSocket.send("ACK".encode("utf-16"))
        chunks[i][chunk_id] = chunk
        hash = client_functions.hash(i, chunks[i])

for i in range(n):
    thread = threading.Thread(target=get_missing_chunks, args=(25999-i,i))
    thread.start()

decision = False
while decision == False:
    digests = client_functions.assemble(chunks, n)
    decision = True
    for hash in digests:
        decision = decision and (hash == correct_hash)


connectionSocket, addr = sock1.accept()

#rec_send_tcp(connectionSocket, bufferSize, "utf-8")
connectionSocket.recv(bufferSize).decode()
connectionSocket.send("done".encode())

digests = client_functions.assemble(chunks, n)
for i in range(n):
    print(digests[i])

