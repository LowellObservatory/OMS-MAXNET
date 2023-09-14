import socket
import time
import string


cards = {'RC1': ("10.11.131.91", 4321)}
cards['RC2'] = ("10.11.131.92", 4321)
cards['OMS3'] = ("10.11.131.93", 4321)
cards['OMS4'] = ("10.11.131.94", 4321)
cards['DEVENY'] = ("10.11.131.36", 4321)

def check_card(name, address):
    commands = [("Who are you", b"WY"),
                ("All Axes mode", b"AA"),
                ("Report axes status", b"RI"),
                ("Encoder status", b"EA"),
                ("Query all limit sensors", b"QL"),
                ("Report position", b"RP"),
                ("Report free chars in com buf", b"#BQ")
                ]
    
    omscard = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    omscard.connect(address)
    time.sleep(0.1)

    print('')
    print ("info for omscard " + name)
    for command in commands:
        omscard.sendall(command[1])
        time.sleep(0.1)
        data = omscard.recv(1024).decode()
        filtered_data = ''.join(filter(lambda x: x in string.printable, data))
        time.sleep(0.1)
        print('    ', command[0], filtered_data.rstrip())
        time.sleep(0.1)
        
    omscard.close()

for name, address in cards.items():
    check_card(name, address)