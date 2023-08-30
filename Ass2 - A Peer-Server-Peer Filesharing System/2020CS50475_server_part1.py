import sys
from socket import *
import time
import server_functions
import threading

begin = time.time()

bufferSize = 2048
n =5
localIP     = "127.0.0.1"
queue = []
timeout = 1
check = True
count = 0
done = False

chunk_send_sockets = server_functions.get_tcp_sockets(19999, n, localIP)
server_receive_sockets = server_functions.get_udp_sockets(20999, n, localIP)
server_send_tcp_sockets = server_functions.get_tcp_sockets(25999, n, localIP)

data = server_functions.get_data('A2_small_file.txt')
chunks, client_chunks = server_functions.divide_into_chunks(data, n)
cache = server_functions.get_cache(chunks, n)
server_functions.send_chunks_to_client(client_chunks, bufferSize, n, chunk_send_sockets)
send_back_to_client = []
for i in range(n):
    send_back_to_client.append([])

def get_from_all(i, sock, bufferSize, n):
    msg = ""
    while msg!="done":
        global queue
        global count
        global cache
        message, address = sock.recvfrom(bufferSize)
        sock.sendto("ACK".encode(), address)
        msg = message.decode()
        #check msg
        if msg in cache.keys():
            send_back_to_client[i].append([msg, cache[msg][0]])
        elif msg!="done" or count==n-1:
            queue.append([msg,i])
        else:
            count+=1

def send_broadcast(localIP, bufferSize, n, sender, receiver, timeout):
    global queue
    while len(queue)>0:
        server_send_sockets = []
        for i in range(n):
            sock = socket(family=AF_INET, type=SOCK_DGRAM)
            sock.bind((localIP,sender-i))
            sock.settimeout(timeout)
            server_send_sockets.append(sock)

        
        def send_requests(msg, sock, bufferSize, port):
            try:
                sock.sendto(msg[0].encode(), (localIP,port))
                sock.recvfrom(bufferSize)
                sock.sendto(msg[1].encode(), (localIP,port))
                sock.recvfrom(bufferSize)
            except:
                global check
                check = False

        threads = []
        msgToSend = [queue[0][0], str(queue[0][1])]
        for i in range(n):
            thread = threading.Thread(target=send_requests, args=(msgToSend, server_send_sockets[i], bufferSize, receiver-i,))
            thread.start()
            threads.append(thread)
        for i in range(len(threads)):
            threads[i].join()
        global check
        if check==True:
            queue.pop(0)

def get_missing_chunks(port):
    global cache
    TCPClientSocket = socket(family=AF_INET, type=SOCK_STREAM)
    TCPClientSocket.connect((localIP, port))
    global send_back_to_client
    TCPClientSocket.send("hi".encode())
    while True:
        msg = TCPClientSocket.recv(bufferSize).decode("utf-16")
        TCPClientSocket.send("ACK".encode("utf-16"))
        if msg=="done":
            break
        client_number = TCPClientSocket.recv(bufferSize).decode("utf-16")
        TCPClientSocket.send("ACK".encode("utf-16"))
        chunk_id = TCPClientSocket.recv(bufferSize).decode("utf-16")
        TCPClientSocket.send("ACK".encode("utf-16"))
        client_number = int(client_number)
        min = list(cache.keys())[0]
        for key in cache.keys():
            if cache[key][1]<cache[min][1]:
                min = key
        del cache[min]
        cache[chunk_id] = [msg, time.time()]

        send_back_to_client[client_number].append([chunk_id, msg])

def send_to_client(sock, bufferSize, i):
    global send_back_to_client
    connectionSocket, addr = sock.accept()
    connectionSocket.recv(bufferSize).decode()
    global done
    global begin
    
    while done==False:
        if len(send_back_to_client[i])>0:
            id = send_back_to_client[i][0][0]
            chunk = send_back_to_client[i][0][1]
            connectionSocket.send(chunk.encode("utf-16"))
            connectionSocket.recv(bufferSize).decode("utf-16")
            connectionSocket.send(id.encode("utf-16"))
            connectionSocket.recv(bufferSize).decode("utf-16")
            send_back_to_client[i].pop(0)
    connectionSocket.send("done".encode("utf-16"))

print("hi")
for i in range(n):
    thread = threading.Thread(target=get_from_all, args=(i, server_receive_sockets[i], bufferSize,n))
    thread.start()
send_broadcast(localIP, bufferSize, n, 22999, 21999, timeout)
threads = []
for i in range(n):
    thread = threading.Thread(target=get_missing_chunks, args=(24999-i,))
    thread.start()
    threads.append(thread)
for i in range(n):
    threads[i].join()

for i in range(n):
    thread = threading.Thread(target=send_to_client, args=(server_send_tcp_sockets[i], bufferSize,i,))
    thread.start()

TCPClientSocket = socket(family=AF_INET, type=SOCK_STREAM)
TCPClientSocket.connect((localIP, 26999))
TCPClientSocket.send("hi".encode())
TCPClientSocket.recv(bufferSize).decode()
done = True
print(time.time()-begin)