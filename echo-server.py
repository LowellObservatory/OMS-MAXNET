# echo-server.py

import socket
import time
from signal import signal, SIGPIPE, SIG_DFL  
signal(SIGPIPE,SIG_DFL)

f = open('./OMS-LDT/java-programs/MasterInstrumentConfiguration.xml')
contents = f.read()
f.close()

contents = bytes(contents, 'utf-8')

sttime = round(time.time()*1000)
prevtime = sttime
socket.setdefaulttimeout(10.0)
server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server.connect(("10.10.30.41", 4321))
for i in range(6):
    
    # server.sendall(contents)
    print()
    print(i)
    print()

    try:
        server.sendall(contents)
        # server.sendall(b"#BQ")
        time.sleep(0.1)
    except socket.error as e:
        print("error during send contents")
        print(e)

    try:
        data = server.recv(1024)
        time.sleep(0.1)
        print(f" Received {data!r}")
    except socket.error as e:
        print("error during recv")
        print(e)

    try:
        server.sendall(b"#BQ")
        time.sleep(0.1)
    except socket.error as e:
        print("error during send #BQ")
        print(e)

    try:
        data = server.recv(1024)
        time.sleep(0.1)
        print(f" Received {data!r}")
    except socket.error as e:
        print("error during second recv")
        print(e)
    
    except socket.error as e:
        print(e)
# We should now be in "broken pipe" mode
# server.close()
# server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# server.connect(("10.10.30.41", 4321))

time.sleep(10.0)

print()
print("doing 100 recvs")
print()
for i in range(100):
    try:
        data = server.recv(1024)
        time.sleep(0.1)
        print(f" Received {data!r}")
    except socket.error as e:
        print("error during recv")
        print(e)
print()
print("done with 100 recvs")
print()

try:
    print("doing send #BQ")
    server.sendall(b"#BQ")
    time.sleep(0.1)
except socket.error as e:
    print("error during send #BQ")
    print(e)

try:
    print("doing receive after #BQ")
    data = server.recv(1024)
    time.sleep(0.1)
    print(f" Received {data!r}")
except socket.error as e:
    print("error during #BQ recv")
    print(e)

server.close()

# data = server.recv(1024)
# print(f"Received {data!r}")

# server.sendall(b"AY")   # select axis Y
# time.sleep(0.1)
# server.sendall(b"RE")   # report encoder position
# time.sleep(0.1)
# data = server.recv(1024)
# time.sleep(0.1)
# print(f"Received {data!r}")
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



# server.sendall(b"AY")   # select axis Y
# time.sleep(0.1)
# server.sendall(b"QA")   # query axis status
# time.sleep(0.1)
# data = server.recv(1024)
# time.sleep(0.1)
# print(f"Received {data!r}")
