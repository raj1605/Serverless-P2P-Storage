import socket, time

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 8080  # The port used by the server

key = "monish"
data = "a"*1024

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

s.connect((HOST, PORT))
iterations = 1000

t0 = time.time()
for i in range(iterations):
    
    c = str(i)
    data_fin = key + c + " " + key + c
    data_fin = data_fin.encode('UTF-8')
    s.sendall(len(data_fin).to_bytes(2, byteorder='big'))
    s.sendall(data_fin)

    length_of_message = int.from_bytes(s.recv(2), byteorder='big')
    ans = s.recv(length_of_message)


    
t1 = time.time()

#time.sleep(5)
print(str((t1-t0)/iterations*1e6))
print("Above in micro")

time.sleep(3)

t2 = time.time()
for i in range(iterations):
    data_get = ""
    if(i < iterations//2):
        data_get = "monish" + str(i)
    else:
        data_get = "monish1"
    data_get = data_get.encode('UTF-8')
    s.sendall(len(data_get).to_bytes(2, byteorder='big'))
    s.sendall(data_get)

    length_of_message = int.from_bytes(s.recv(2), byteorder='big')
    ans = s.recv(length_of_message).decode()
    
    #if(ans != str(i) + data):
        #print(False)
    #print(ans.decode('UTF-8'))
    
    #data = s.recv(1056)

t3 = time.time()

print(str((t3-t2)/iterations*1e6))
print("Above in micro")
data = "over"
data = data.encode('UTF-8')
s.sendall(len(data).to_bytes(2, byteorder='big'))
s.sendall(data)




