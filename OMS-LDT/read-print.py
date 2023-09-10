# echo-server.py

import socket
import time

server = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
server.connect(("10.10.30.41", 4321))
time.sleep(0.1)

server.sendall(b"AX")   # select axis X
time.sleep(0.1)
server.sendall(b"RE")   # report encoder position
time.sleep(0.1)
server.settimeout(0.1)
while(True):
    try:
        data = server.recv(1024)
    except:
        print("an exception occured")
    # time.sleep(0.1)
    print(f"Received {data!r}")
    # time.sleep(0.1)

# server.sendall(b"AX")   # select axis X
# time.sleep(0.1)
# server.sendall(b"RE")   # report encoder position
# time.sleep(0.1)
# data = server.recv(1024)
# time.sleep(0.1)
# print(f"Received {data!r}")
# time.sleep(0.1)

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
