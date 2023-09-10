# echo-server.py

import socket
import time

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
server.connect(("10.10.30.41", 4321))
time.sleep(0.1)


server.sendall(b"AX;")
# time.sleep(0.1)
server.sendall(b"VL1000;")
# time.sleep(0.1)
server.sendall(b"AC1000;")
# time.sleep(0.1)
server.sendall(b"DC1000;")
# time.sleep(0.1)
server.sendall(b"GP4000;")
# time.sleep(1.1)
server.sendall(b"GP0;")
# time.sleep(1.1)

server.sendall(b"RE")   # report encoder position
time.sleep(0.1)
data = server.recv(1024)

print(f"Received {data!r}")
   

# server.sendall(b"RI")   # report axes status
# time.sleep(0.1)
# data = server.recv(1024)
# time.sleep(0.1)
# print(f"Received {data!r}")
# time.sleep(0.1)

# server.sendall(b"WY")   # who are you
# time.sleep(0.1)
# data = server.recv(1024)
# time.sleep(0.1)
# print(f"Received {data!r}")
# time.sleep(0.1)

# server.sendall(b"AY")   # select axis Y
# time.sleep(0.1)
# server.sendall(b"QA")   # query axis status
# time.sleep(0.1)
# data = server.recv(1024)
# time.sleep(0.1)
# print(f"Received {data!r}")
