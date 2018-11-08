# -*- coding: utf-8 -*-
#==============================================================================
# import sys
# sys.path.append('..')
# 
# from client.client import another_test
#==============================================================================
import dir_struct
from dir_struct import Tree
from dir_struct import DumpObj
from dir_struct import ChunkLoc
import json
from threading import Thread
import pickle
import time
import socket
import sys
import reReplicateChunk
import configparser
from ListenClientSlave import ListenClientChunkServer

config = configparser.RawConfigParser()
config.read('master.properties')
Json_rcv_limit = int(config.get('Master_Data','JSON_RCV_LIMIT'))

class BgPoolChunkServer(object):
    def __init__(self, metaData, chunk_servers, self_ip_port, interval=5):
        self.interval = interval
        self.chunk_servers = chunk_servers
        self.ip = self_ip_port[0]
        self.port = int(self_ip_port[1])
        self.metadata = metaData
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
                try:    
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((TCP_IP, TCP_PORT))
                    s.sendall(str(send_data).encode())
                    s.close()
                except:
                    reReplicateChunk.distribute_load(self.ip, self.port, TCP_IP, TCP_PORT, self.metadata, task="old_removed")
                    continue
            time.sleep(self.interval)

                    
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


dir_struct.globalChunkMapping = ChunkLoc()
dir_struct.globalChunkMapping.slaves_state = chunk_servers
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
        incoming = new_tree.insert(dir_arr[i], type_dir[i], new_tree, metaData)
        metaData = incoming[1]
    
    metaData.fileNamespace = new_tree
    new_tree.showDirectoryStructure(metaData.fileNamespace)
    master_state = open('masterState','ab')
    pickle.dump(metaData, master_state)
    master_state.close()

bgthread = BgPoolChunkServer(metaData, servers_ip_port, self_ip_port)

tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tcpsock.bind((self_ip_port[0], int(self_ip_port[1])))


while True:
    tcpsock.listen(1000)
    print ("Waiting for incoming connections...")
    (conn, (ip,port)) = tcpsock.accept()
    print ('Got connection from ', (ip,port))
    listenthread = ListenClientChunkServer(metaData, conn, self_ip_port[0], int(self_ip_port[1]))
    listenthread.daemon = True
    listenthread.start()
