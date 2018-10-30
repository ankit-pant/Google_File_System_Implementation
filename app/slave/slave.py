from threading import Thread
import socket
import sys

class ListenClientMaster:
    def __init__(self,ip,port,sock):
        Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.sock = sock
        print (" New thread started for "+ip+":"+str(port))
    
    def run(self):
        data = self.sock.recv(1024)
        data = data.decode()
        print(data)


tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tcpsock.bind((sys.argv[1], sys.argv[2]))

while True:
    tcpsock.listen(1000)
    print ("Waiting for incoming connections...")
    (conn, (ip,port)) = tcpsock.accept()
    print ('Got connection from ', (ip,port))
    listenthread = ListenClientMaster(ip,port,conn)
    listenthread.daemon = True
    listenthread.start()