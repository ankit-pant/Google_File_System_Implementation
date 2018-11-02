import socket
from threading import Thread
import sys
import configparser
import json

byte_read = []
config = configparser.RawConfigParser()
config.read('client.properties')
REC_LIMIT = int(config.get('Client_Data','CHUNK_RECSIZE'))

class ListenMasterChunkServer(Thread):
    def __init__(self,sock, ip, port):
        Thread.__init__(self)
        self.sock = sock
        self.ip = ip
        self.port = port
        print (" New thread started for "+ip+":"+str(port))
    
    def find_nearest(self, ips):
        ip_arr = self.ip.split('.')
        distance_ip = []
        for ip in ips:
            counter=0
            for x in range(4):
                if ip_arr[x] == ip[x]:
                    counter+=1
                else:
                    break
            distance_ip.append(counter)
        max_dis = 0
        i=0
        for dis in range(len(distance_ip)):
           if distance_ip[dis]>max_dis:
               max_dis = distance_ip[dis]
               i=dis
        return '.'.join(ips[i])
    
           
    def run(self):
        data = self.sock.recv(REC_LIMIT)
        try:
            str_data = data.decode().replace("\'", "\"")
            json_data = json.loads(str_data)
            print("data recieved: "+str(json_data))
            if json_data["agent"]=="master":
                if json_data["action"]=="response/read":
                    reachable_ip = []
                    for chunk in json_data["data"]:
                        for chunk_server in chunk["chunk_servers"]:
                            x=chunk_server["ip"].split(':')
                            reachable_ip.append(x)
                        
                        sending_ip = self.find_nearest(reachable_ip)
                        for target_server in chunk["chunk_servers"]:
                            if target_server["ip"] == sending_ip:
                                sending_port = target_server["port"]
                                break
                        print("p: "+str(sending_port))
                        print("i: "+sending_ip)
                        print(target_server)
                        request_data = {}
                        request_data["agent"] = "client"
                        request_data["action"] = "request/read"
                        request_data["ip"] = self.ip
                        request_data["port"] = self.port
                        request_data["data"] = []
                        my_data = {}
                        my_data["handle"] = chunk["chunk_handle"]
                        my_data["start_byte"] = byte_read[0]
                        my_data["end_byte"] = byte_read[1]
                        request_data["data"].append(my_data)
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect((sending_ip, sending_port))
                        print("sending data to: "+sending_ip+str(sending_port))
                        print(request_data)
                        print(":::::::::::::::::::::::::::::::::::::::::::::::::::::")
                        s.sendall(str(request_data).encode())
                        s.close()
                    
        except:
            print("Not a json data")
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
        global byte_read
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
    listenthread = ListenMasterChunkServer(conn, self_ip_port[0], int(self_ip_port[1]))
    listenthread.daemon = True
    listenthread.start()
