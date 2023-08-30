from socket import *
import threading

def get_data(filename):
    with open(filename, 'r') as file:
        return file.read()

def divide_into_chunks(data, n):
    chunks = []
    client_chunks = []
    
    st = data
    size = 1000
    chunks = [[st[i:i+size], str(i//size)] for i in range(0, len(st), size)]
    chunks_copy = chunks
    num_each = len(chunks)//n
    for i in range(n):
        client_chunks.append(chunks[:num_each])
        chunks = chunks[num_each:]
    for i in range(len(chunks)%n):
        client_chunks[i].append(chunks[0])
        chunks.pop(0)
    return chunks_copy, client_chunks

def get_cache(chunks, n):
    cache = {}
    for i in range(n):
        cache[chunks[-1-i][1]] = [chunks[-1-i][0], 0]
    return cache

def get_udp_sockets(port, n, localIP):
    sockets = []
    for i in range(n):
        sock = socket(family=AF_INET, type=SOCK_DGRAM)
        sock.bind((localIP,port+i))
        sockets.append(sock)
    return sockets

def send_udp(sock, msg, addr, bufferSize):
    try:
        sock.sendto(msg.encode(), addr)
        sock.recvfrom(bufferSize)
        print("received ack")
    except:
        print("sending again")
        send_udp(sock, msg, addr, bufferSize)

def send_chunks_to_client(client_chunks, bufferSize, n, sockets):
    def temp(sock, i):
        connectionSocket, addr = sock.accept()
        for j in range(len(client_chunks[i])):
            connectionSocket.recv(bufferSize)
            connectionSocket.send(client_chunks[i][j][0].encode())
            connectionSocket.recv(bufferSize)
            connectionSocket.send(client_chunks[i][j][1].encode())
        connectionSocket.recv(bufferSize)
        connectionSocket.send("done".encode())

    threads = []
    for i in range(n):
        thread = threading.Thread(target=temp, args=(sockets[i], i, ))
        thread.start()
        threads.append(thread)
    for i in range(n):
        threads[i].join()

def get_tcp_sockets(port, n, localIP):
    sockets = []
    for i in range(n):
        sock = socket(family=AF_INET, type=SOCK_STREAM)
        sock.bind((localIP, port+i))
        sock.listen(1)
        sockets.append(sock)
    return sockets