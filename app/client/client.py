import socket
from threading import Thread
import sys
import configparser
import json
import copy


indices_arr = []
config = configparser.RawConfigParser()
config.read('client.properties')
REC_LIMIT = int(config.get('Client_Data','CHUNK_RECSIZE'))
CHUNKSIZE = int(config.get('Client_Data','CHUNKSIZE'))
DELIMITER = config.get('Client_Data','DELIMITER')


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
        global indices_arr
        data = self.sock.recv(REC_LIMIT)
        try:
            str_data = data.decode().replace("\'", "\"")
            print(str_data)
            json_data = json.loads(str_data)
            if json_data["agent"]=="master":
                if json_data["action"]=="response/read":
                    reachable_ip = []
                    for chunk in json_data["data"]:
                        for chunk_server in chunk["chunk_servers"]:
                            x=chunk_server["ip"].split('.')
                            reachable_ip.append(x)
                        
                        sending_ip = self.find_nearest(reachable_ip)
                        for target_server in chunk["chunk_servers"]:
                            if target_server["ip"] == sending_ip:
                                sending_port = target_server["port"]
                                break
                        request_data = {}
                        request_data["agent"] = "client"
                        request_data["action"] = "request/read"
                        request_data["ip"] = self.ip
                        request_data["port"] = self.port
                        request_data["data"] = []
                        my_data = {}
                        my_data["handle"] = chunk["chunk_handle"]
                        my_idx = chunk["chunk_index"]
                        print("printing indices arr: ")
                        print(indices_arr)
                        for check in indices_arr:
                            if check["idx"] == my_idx:
                                my_data["start_byte"] = check["start_byte"]
                                my_data["end_byte"] = check["end_byte"]
                                break
                        request_data["data"].append(my_data)
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect((sending_ip, sending_port))
                        s.sendall(str(request_data).encode())
                        s.close()
                elif json_data["action"]=="delete/response":
                    if json_data["data"]["ok_status"]:
                        print("File Successfully Deleted")
                    else:
                        print("Unable to delete the file")
            elif json_data["agent"] == "slave":
                print("data from slave")
                    
        except ValueError:
            indices_arr[:]=[]
            print("Data recieved from the chunk server")
            #print(data)

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
        global indices_arr
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
                start_idx = int(int(byte_read[0])/CHUNKSIZE)
                end_idx = int(int(byte_read[1])/CHUNKSIZE)
                request_data["action"] = "read"
                request_data["data"]["file_name"] = fileName
                request_data["data"]["idx"] = []
                if start_idx == end_idx:
                    request_data["data"]["idx"].append(start_idx)
                    book_keeping_info = {}
                    book_keeping_info["start_byte"] = int(byte_read[0])
                    book_keeping_info["end_byte"] = int(byte_read[1])
                    book_keeping_info["idx"]= start_idx
                    indices_arr.append(book_keeping_info)
                else:
                    f_i = start_idx
                    while start_idx<=end_idx:
                        book_keeping_info = {}
                        if start_idx == f_i:
                            book_keeping_info["start_byte"] = int(byte_read[0]) - start_idx * CHUNKSIZE
                            book_keeping_info["end_byte"] = CHUNKSIZE
                        elif start_idx == end_idx:
                            book_keeping_info["start_byte"] = 0
                            book_keeping_info["end_byte"] = int(byte_read[1]) - start_idx * CHUNKSIZE
                        else:
                            book_keeping_info["start_byte"] = 0
                            book_keeping_info["end_byte"] = CHUNKSIZE
                        book_keeping_info["idx"] = start_idx
                        indices_arr.append(copy.deepcopy(book_keeping_info))
                        request_data["data"]["idx"].append(int(start_idx))
                        start_idx+=1
                    print("On taking input frm user: ")
                    print(indices_arr)
            elif command == "snapshot":
                request_data["action"] = "snapshot"
                request_data["data"] = {}
            elif command == "delete":
                fileName = input("Enter the filepath to be deleted: ")
                request_data["action"] = "delete_file"
                request_data["data"] = {}
                request_data["data"]["file_path"] = fileName
            elif command == "create":
                fileName = input("Enter the filename: ")
                request_data["action"] = "create_file"
                x = input("For reading data from an existing file print 1 else for feeding data print 2:")
                if x==str("1"):
                    fileName_arr = fileName.split('/')
                    null_idx = []
                    i=0
                    for dir in fileName_arr:
                        if dir=='':
                            null_idx.append(i)
                        i+=1
                    k=len(null_idx)-1
                    while k>=0:
                        del fileName_arr[null_idx[k]]
                        k-=1
                    fname = fileName_arr[len(fileName_arr)-1]
                    print("Reading data from file: ",fname)
                    file = open(fname, "rb")
                    bytes_read = file.read()
                    headers = DELIMITER+"distribute"+DELIMITER+self.self_Ip+DELIMITER+str(self.self_Port)+DELIMITER+fileName+DELIMITER
                    if len(headers)<118:
                        headers = headers.ljust(118)
                    request_data = headers.encode()+bytes_read
                elif x==str("2"):
                    data = input("Enter the data for the file")
                    data=data.encode()
                    headers = DELIMITER+"distribute"+DELIMITER+self.self_Ip+DELIMITER+str(self.self_Port)+DELIMITER+fileName+DELIMITER
                    if len(headers)<118:
                        headers = headers.ljust(118)
                    request_data = headers.encode()+data
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((self.Master_Ip, self.Master_Port))
            if type(request_data) is bytes:
                s.sendall(request_data)
            else:                
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
