import socket
import select
import time
import multiprocessing as mp


def routecard(omscard):
    data = None

    while True:
        r, w, e = select.select([omscard], [], [], 20.0)  # Make sure we block on read.
        if not (r or w or e):
            print("timed out, do some other work here")
        if r:
            try:
                data = omscard.recv(2048).decode()
            except socket.error as e:
                print(e)
                data = None
            if data:
                print(f"Received {data!r}")


def testcard(omscard):
    while True:
        omscard.sendall(b"WY")  # who are you
        time.sleep(1.0)


if __name__ == "__main__":
    # Connect to the omscard.
    socket.setdefaulttimeout(10.0)
    omscard = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    time.sleep(1.0)

    omscard.connect(("10.10.30.41", 4321))
    time.sleep(1.0)

    # creating multiple processes
    ctx = mp.get_context("spawn")
    proc1 = ctx.Process(target=testcard, args=(omscard,))
    # proc1.start()
    proc2 = ctx.Process(target=routecard, args=(omscard,))
    proc2.start()

    for i in range(100):
        time.sleep(10.0)

        # Let's move the stepper motor back and forth.
        omscard.sendall(b"AX;")
        omscard.sendall(b"VL1000;")
        omscard.sendall(b"AC1000;")
        omscard.sendall(b"DC1000;")
        omscard.sendall(b"GP200;")
        time.sleep(10.0)

        omscard.sendall(b"AX")  # select axis Y
        omscard.sendall(b"QA")  # query axis status
        omscard.sendall(b"AY")  # select axis Y
        omscard.sendall(b"QA")  # query axis status

        omscard.sendall(b"AX;")
        omscard.sendall(b"VL1000;")
        omscard.sendall(b"AC1000;")
        omscard.sendall(b"DC1000;")
        omscard.sendall(b"GP0;")

        time.sleep(10.0)

        omscard.sendall(b"AX")  # select axis Y
        omscard.sendall(b"QA")  # query axis status
        omscard.sendall(b"AY")  # select axis Y
        omscard.sendall(b"QA")  # query axis status

        time.sleep(10.0)

    # proc1.terminate()
    proc2.terminate()

    # Processes finished
    print("Both Processes Terminated!")
