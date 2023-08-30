from socket import *
import time
import threading
import hashlib
import sys

def get_chunks_from_server(bufferSize, n, localIP, start):
    get_from = []
    for i in range(n):
        get_from.append((localIP, start-i))

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

def send_all_requests(n, send, receive, timeout, dont_have, bufferSize):
    client_send_sockets = []
    for i in range(n):
        UDPClientSocket = socket(family=AF_INET, type=SOCK_DGRAM)
        UDPClientSocket.bind(("127.0.0.1",send-i))
        UDPClientSocket.settimeout(timeout)
        client_send_sockets.append(UDPClientSocket)

    def send_with_ack(missing, sock, bufferSize, i):
        sock.sendto(missing[0].encode(), ("127.0.0.1",receive-i))
        sock.recvfrom(bufferSize)
        
    def send_requests(i, start, sock, missing, bufferSize):
        while len(missing)>0:
            try:
                send_with_ack(missing, sock, bufferSize, i)
                missing.pop(0)
            except:
                print("timeout")
        try:
            sock.sendto("done".encode(), ("127.0.0.1", start-i))
            sock.recvfrom(bufferSize)
        except:
            print("timeout")

    for i in range(n):
        thread = threading.Thread(target=send_requests, args=(i, receive, client_send_sockets[i], dont_have[i], bufferSize,))
        thread.start()

def assemble(chunks, n):
    digests = []
    for j in range(n):
        digests.append(hash(j, chunks[j]))
    return digests

def hash(i, chunks):
    client = ""
    for i in range(len(chunks)):
        id = str(i)
        if id in list(chunks.keys()):
            client = client + chunks[id]
    return hashlib.md5(client.encode()).hexdigest()

def get_udp_sockets(port, n, localIP):
    sockets = []
    for i in range(n):
        sock = socket(family=AF_INET, type=SOCK_DGRAM)
        sock.bind((localIP,port-i))
        sockets.append(sock)
    return sockets

def get_tcp_sockets(port, n, localIP):
    sockets = []
    for i in range(n):
        sock = socket(family=AF_INET, type=SOCK_STREAM)
        sock.bind((localIP, port-i))
        sock.listen(1)
        sockets.append(sock)
    return sockets

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