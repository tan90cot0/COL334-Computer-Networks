#[20000, 20000+n] - UDP port on server for receving messages
#[21000, 21000+n] - UDP port on client for receving messages
#[22000, 22000+n] - UDP port on server for sending messages
#[23000, 23000+n] - UDP port on client for sending messages
#[24000, 24000+n] - TCP port for broadcasts
#[25000, 25000+n] - TCP port for requests
# 26000 - TCP Connection to convey that the transfer is done
#[27000, 27000+n] - TCP port for important communication between server and client
#[28000, 28000+n] - TCP port for important communication between client and server

import sys
from socket import *
import time
import server_functions2
import threading


bufferSize = 2048
n =5
localIP     = "127.0.0.1"
queue = []
timeout = 1
check = True
count = 0
done = False
offset = 5
exit = []
for i in range(n):
    exit.append(False)
ports = [19000+offset, 20000+offset, 21000+offset, 22000+offset, 23000+offset, 24000+offset, 25000+offset, 26000+offset, 27000+offset, 28000+offset]


sock1 = socket(family=AF_INET, type=SOCK_STREAM)
sock1.bind((localIP, ports[7]))
sock1.listen(1)
control_sock, addr = sock1.accept()
control_sock.recv(bufferSize).decode()
control_sock.send("hi".encode())

chunk_send_sockets = server_functions2.get_tcp_sockets(ports[0], n, localIP)
broadcast_sockets = server_functions2.get_tcp_sockets(ports[5], n, localIP)
server_send_sockets = server_functions2.get_udp_sockets(ports[3], n, localIP)
server_receive_sockets = server_functions2.get_udp_sockets(ports[1], n, localIP)
server_send_tcp_sockets = server_functions2.get_tcp_sockets(ports[9], n, localIP)

data = server_functions2.get_data('A2_small_file.txt')
chunks, client_chunks = server_functions2.divide_into_chunks(data, n)
cache = server_functions2.get_cache(chunks, n)
server_functions2.send_chunks_to_client(client_chunks, bufferSize, n, chunk_send_sockets)
send_back_to_client = []
for i in range(n):
    send_back_to_client.append([])

def get_from_all(i, port, bufferSize, n):
    msg = ""
    sock = socket(family=AF_INET, type=SOCK_STREAM)
    sock.connect((localIP, port))
    sock.send("hi".encode())
    while msg!="done":
        global queue
        global count
        global cache
        msg = sock.recv(bufferSize).decode()
        sock.send("ACK".encode())
        #check msg
        if msg in cache.keys():
            send_back_to_client[i].append([msg, cache[msg][0]])
        elif msg!="done" or count==n-1:
            queue.append([msg,i])
        else:
            count+=1

begin = time.time()
for i in range(n):
    thread = threading.Thread(target=get_from_all, args=(i, ports[6]+i, bufferSize,n))
    thread.start()

def send_broadcasts(msg, sock, bufferSize):
    sock.send(msg[0].encode())
    sock.recv(bufferSize).decode()
    sock.send(msg[1].encode())
    sock.recv(bufferSize).decode()

def send_broadcast(bufferSize, n, sockets):
    global queue
    connectionSockets = []
    for i in range(n):
        connectionSocket, addr = sockets[i].accept()
        connectionSocket.recv(bufferSize).decode()
        connectionSockets.append(connectionSocket)
    while len(queue)>0:
        msgToSend = [queue[0][0], str(queue[0][1])]
        threads = []
        for i in range(n):
            thread = threading.Thread(target=send_broadcasts, args=(msgToSend, connectionSockets[i], bufferSize,))
            thread.start()
            threads.append(thread)
        for i in range(len(threads)):
            threads[i].join()
        queue.pop(0)

send_broadcast(bufferSize, n, broadcast_sockets)

def wait_for_confirmation(sock, bufferSize, i):
    global exit
    msg = sock.recv(bufferSize).decode()
    sock.send("ACK".encode())
    exit[i] = True

def get_missing_chunks(port, sock, bufferSize, localIP, i):
    global cache
    global send_back_to_client
    global exit
    TCPClientSocket = socket(family=AF_INET, type=SOCK_STREAM)
    TCPClientSocket.connect((localIP, port))
    TCPClientSocket.send("hi".encode())
    while True:
        thread = threading.Thread(target=wait_for_confirmation, args=(TCPClientSocket, bufferSize, i,))
        thread.start()
        message = ''
        while exit[i]==False:
            message, addr = sock.recvfrom(bufferSize) 
            sock.sendto("ACK".encode(), addr)
            time.sleep(0.002)
        
        exit[i] = False
        msg = message.decode()
        if msg=="done":
            break
        client_number = TCPClientSocket.recv(bufferSize).decode()
        TCPClientSocket.send("ACK".encode())
        chunk_id = TCPClientSocket.recv(bufferSize).decode()
        TCPClientSocket.send("ACK".encode())
        client_number = int(client_number)
        min = list(cache.keys())[0]
        for key in cache.keys():
            if cache[key][1]<cache[min][1]:
                min = key
        del cache[min]
        cache[chunk_id] = [msg, time.time()]
        send_back_to_client[client_number].append([chunk_id, msg])

#client sends file
#servers receives and sends ack
#when client receives ack it sends TCP done
#when server receives TCP done, it sends an ack and both finish
threads = []
for i in range(n):
    thread = threading.Thread(target=get_missing_chunks, args=(ports[8]+i, server_receive_sockets[i], bufferSize, localIP, i,))
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

def send_to_client(sock, bufferSize, i, udp_socket, client_add):
    global send_back_to_client
    connectionSocket, addr = sock.accept()
    connectionSocket.recv(bufferSize).decode()
    global done
    global begin
    while done==False:
        #print("length = " + str(len(send_back_to_client[i])) + "i = " + str(i))
        time.sleep(0.01)
        if len(send_back_to_client[i])>0:
            id = send_back_to_client[i][0][0]
            chunk = send_back_to_client[i][0][1]

            send_via_udp(udp_socket, client_add, chunk, connectionSocket, bufferSize)

            connectionSocket.send(id.encode())
            connectionSocket.recv(bufferSize).decode()
            send_back_to_client[i].pop(0)
    #connectionSocket.send("done".encode())

for i in range(n):
    thread = threading.Thread(target=send_to_client, args=(server_send_tcp_sockets[i], bufferSize,i, server_send_sockets[i], (localIP, ports[2]+i), ))
    thread.start()

control_sock.recv(bufferSize).decode()
done = True


#copy saara same logic, just udp transfers ke baad, make a tcp connection that sends done. keep sending packets again until done arrives.