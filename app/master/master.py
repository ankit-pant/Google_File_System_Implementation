# -*- coding: utf-8 -*-
#==============================================================================
# import sys
# sys.path.append('..')
# 
# from client.client import another_test
#==============================================================================
from dir_struct import Tree
from dir_struct import DumpObj
from dir_struct import ChunkLoc
import json
from threading import Thread
import pickle
import time
import socket
import sys

class BgPoolChunkServer(object):
    def __init__(self, chunk_servers, self_ip_port, interval=5):
        self.interval = interval
        self.chunk_servers = chunk_servers
        self.ip = self_ip_port[0]
        self.port = int(self_ip_port[1])
        thread = Thread(target=self.run, args=())
        thread.daemon = True
        thread.start()

    def run(self):
        while True:
            send_data = {}
            send_data["agent"]="master"
            send_data["ip"]=self.ip
            send_data["port"]=self.port
            send_data["action"]="periodic_report"
            for c_server in self.chunk_servers:
                TCP_IP=c_server["ip"]
                TCP_PORT=c_server["port"]
                print(TCP_IP+"  "+str(TCP_PORT))
                try:    
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((TCP_IP, TCP_PORT))
                    s.sendall(str(send_data).encode())
                    s.close()
                except:
                    #print("Connection refused by the slaveServer: "+TCP_IP+":"+str(TCP_PORT))
                    continue
            time.sleep(self.interval)


class ListenClientChunkServer(Thread):
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
    

try:
    with open('chunk_servers.json') as f:
        chunk_servers = json.load(f)
except IOError:
    print("Unable to locate chunkservers in the database!")

servers_ip_port = []
for server in chunk_servers:
    j={}
    j["ip"]=server["ip"]
    j["port"]=server["port"]
    servers_ip_port.append(j)

self_ip_port = str(sys.argv[1]).split(":")
print(self_ip_port)
bgthread = BgPoolChunkServer(servers_ip_port, self_ip_port)

chunkMapping = ChunkLoc()
try:
    pklFile = open('masterState','rb')
    metaData = pickle.load(pklFile)
except IOError:
    new_tree = Tree()
    metaData = DumpObj()
    
    dir_arr = ["home", "home/a", "home/b", "home/a/d", "home/c", "home/a/e", 
               "home/b/g", "home/b/h", "home/b/i", "home/c/j", "home/c/k", "home/c/l", "home/a/d/m", "home/a/d/n", "home/a/d/n/EOT.mkv"]
    
    type_dir = [0,0,0,0,0,0,0,0,0,0,0,0,0,0,1]
    
    for i in range(len(dir_arr)):
        incoming = new_tree.insert(dir_arr[i], type_dir[i], new_tree, metaData, chunkMapping)
        metaData = incoming[1]
        chunkMapping = incoming[2]
    
    metaData.fileNamespace = new_tree
    new_tree.showDirectoryStructure(metaData.fileNamespace)
    master_state = open('masterState','ab')
    pickle.dump(metaData, master_state)

tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tcpsock.bind((self_ip_port[0], int(self_ip_port[1])))

while True:
    tcpsock.listen(1000)
    print ("Waiting for incoming connections...")
    (conn, (ip,port)) = tcpsock.accept()
    print ('Got connection from ', (ip,port))
    listenthread = ListenClientChunkServer(ip,port,conn)
    listenthread.daemon = True
    listenthread.start()
