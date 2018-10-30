import socket
from threading import Thread
import sys

class ListenMasterChunkServer(Thread):
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

class TakeUserInput(object):
    def __init__(self,master_ip_port,self_ip_port):
        thread = Thread(target=self.run, args=())
        thread.daemon = True
        thread.start()
        self.Master_Ip=master_ip_port[0]
        self.Master_Port=int(master_ip_port[1])
        self.self_Ip=self_ip_port[0]
        self.self_Port=int(self_ip_port[1])
        
    def run(self):
        while True:
            command = input("Input the command: ")
            request_data = {}
            request_data["agent"] = "client"
            request_data["ip"] = self.self_Ip
            request_data["port"] = self.self_Port
            request_data["data"] = {}
            if command == "read":
                fileName = input("Enter the filename: ")
                byteRange = input("Enter the byte range which you want to read: (starting_kilobyte-ending_kilobyte) Eg. 1024-6352 ")
                byte_read = byteRange.split('-')
                start_idx = int(byte_read[0])/66560
                end_idx = int(byte_read[1])/66560
                request_data["action"] = "read"
                request_data["data"]["file_name"] = fileName
                request_data["data"]["idx"] = []
                if start_idx == end_idx:
                    request_data["data"]["idx"].append(start_idx)
                else:
                    while start_idx<=end_idx:
                        request_data["data"]["idx"].append(int(start_idx))
                        start_idx+=1
            print("Outside while, data is: "+str(request_data))
            print("Outside while, master is: "+self.Master_Ip+":"+str(self.Master_Port))
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.Master_Ip, self.Master_Port))
            s.sendall(str(request_data).encode())
            s.close()

self_ip_port = str(sys.argv[1]).split(':')
master_ip_port = str(sys.argv[2]).split(':')
inputThread = TakeUserInput(master_ip_port, self_ip_port)

tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tcpsock.bind((self_ip_port[0], int(self_ip_port[1])))

while True:
    tcpsock.listen(1000)
    print ("Waiting for incoming connections...")
    (conn, (ip,port)) = tcpsock.accept()
    print ('Got connection from ', (ip,port))
    listenthread = ListenMasterChunkServer(ip,port,conn)
    listenthread.daemon = True
    listenthread.start()
