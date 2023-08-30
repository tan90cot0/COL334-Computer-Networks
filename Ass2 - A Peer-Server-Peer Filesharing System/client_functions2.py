from socket import *
import threading
import hashlib
import time

def get_udp_sockets(port, n, localIP):
    sockets = []
    for i in range(n):
        sock = socket(family=AF_INET, type=SOCK_DGRAM)
        sock.bind((localIP,port+i))
        sockets.append(sock)
    return sockets

def get_chunks_from_server(bufferSize, n, localIP, start):
    get_from = []
    for i in range(n):
        get_from.append((localIP, start+i))

    dont_have = []
    chunks = []
    for i in range(n):
        chunks.append({})
        dont_have.append([])

    def temp(i, address):
        TCPClientSocket = socket(family=AF_INET, type=SOCK_STREAM)
        TCPClientSocket.connect(address)
        while True:
            TCPClientSocket.send("hi".encode())
            chunk = TCPClientSocket.recv(bufferSize).decode()
            if chunk =="done":
                break
            TCPClientSocket.send("hi".encode())
            id = TCPClientSocket.recv(bufferSize).decode()
            chunks[i][id] = chunk
            for k in range(n):
                if k!=i:
                    dont_have[k].append(id)
        TCPClientSocket.close()

    threads = []
    for i in range(n):
        thread = threading.Thread(target=temp, args=(i, get_from[i], ))
        thread.start()
        threads.append(thread)
    for i in range(n):
        threads[i].join()
    return chunks, dont_have

def get_tcp_sockets(port, n, localIP):
    sockets = []
    for i in range(n):
        sock = socket(family=AF_INET, type=SOCK_STREAM)
        sock.bind((localIP, port+i))
        sock.listen(1)
        sockets.append(sock)
    return sockets

def hash(i, chunks):
    client = ""
    for i in range(len(chunks)):
        id = str(i)
        if id in list(chunks.keys()):
            client = client + chunks[id]
    return hashlib.md5(client.encode()).hexdigest()

def assemble(chunks, n):
    digests = []
    for j in range(n):
        digests.append(hash(j, chunks[j]))
    return digests

