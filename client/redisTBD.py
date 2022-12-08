import socket, time
import redis

HOST = "127.0.0.1"  # The server's hostname or IP address
PORT = 6379  # The port used by the server

key = "monish"
data = "a"*1024


r = redis.Redis(host = HOST, port = PORT)
r.set('f','b')
run = True
iterations = 1000

t0 = time.time()
for i in range(iterations):
    r.set("monish"+str(i), data)
t1 = time.time()




#time.sleep(5)
print((t1-t0)/iterations*1e6)
print("Above in micro")


t0 = time.time()
for i in range(iterations):
    r.get("monish"+str(i))
t1 = time.time()




#time.sleep(5)
print((t1-t0)/iterations*1e6)
print("Above in micro")



