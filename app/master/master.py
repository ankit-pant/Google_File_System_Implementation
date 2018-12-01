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
from threading import Thread, BoundedSemaphore
import pickle
import time
import socket
import sys
import reReplicateChunk
import configparser
from ListenClientSlave import ListenClientChunkServer
import copy

container = BoundedSemaphore()
config = configparser.RawConfigParser()
config.read('master.properties')
Json_rcv_limit = int(config.get('Master_Data','JSON_RCV_LIMIT'))

class BgSaveOperationLog(object):
    def __init__(self, metaData, interval=10):
        self.metaData = metaData
        self.interval = interval
        thread = Thread(target=self.run, args=())
        thread.daemon=True
        thread.start()
    
    def run(self):
        while True:
            print("Dumping: ",self.metaData.slaves_list)
            print("Chunks DB len: ",len(self.metaData.chunksDB))
            print("number of files: ",len(self.metaData.metadata))
            master_state = open('masterState','wb')
            pickle.dump(self.metaData, master_state)
            master_state.close()
            time.sleep(self.interval)

class BgPoolChunkServer(object):
    def __init__(self, metaData, container, self_ip_port, interval=5):
        self.interval = interval
        self.ip = self_ip_port[0]
        self.port = int(self_ip_port[1])
        self.metadata = metaData
        self.container = container
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
            for c_server in dir_struct.globalChunkMapping.slaves_state:
                TCP_IP=c_server["ip"]
                TCP_PORT=c_server["port"]
                try:    
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((TCP_IP, TCP_PORT))
                    s.sendall(str(send_data).encode())
                    s.close()
                except:
                    reReplicateChunk.distribute_load(self.ip, self.port, TCP_IP, TCP_PORT, self.metadata, self.container, task="old_removed")
                    continue
            time.sleep(self.interval)

self_ip_port = str(sys.argv[1]).split(":")
print(self_ip_port)
servers_ip_port = []
    
try:
    pklFile = open('masterState','rb')
    metaData = pickle.load(pklFile)
    dir_struct.globalChunkMapping = ChunkLoc()
    dir_struct.globalChunkMapping.slaves_state = copy.deepcopy(metaData.slaves_list)
    for server in metaData.slaves_list:
        j={}
        j["ip"]=server["ip"]
        j["port"]=server["port"]
        servers_ip_port.append(j)
except IOError:
    try:
        with open('chunk_servers.json') as f:
            chunk_servers = json.load(f)
    except IOError:
        print("Unable to locate chunkservers in the database!")
    for server in chunk_servers:
        j={}
        j["ip"]=server["ip"]
        j["port"]=server["port"]
        servers_ip_port.append(j)
    dir_struct.globalChunkMapping = ChunkLoc()
    dir_struct.globalChunkMapping.slaves_state = chunk_servers
    new_tree = Tree()
    metaData = DumpObj()
    metaData.slaves_list = chunk_servers
    
    dir_arr = ["home", "home/a", "home/b", "home/a/d", "home/c", "home/a/e", 
               "home/b/g", "home/b/h", "home/b/i", "home/c/j", "home/c/k", "home/c/l", "home/a/d/m", "home/a/d/n"]
    
    type_dir = [0,0,0,0,0,0,0,0,0,0,0,0,0,0]
    for i in range(len(dir_arr)):
        incoming = new_tree.insert(dir_arr[i], type_dir[i], new_tree, metaData, container)
        metaData = incoming[1]
    
    metaData.fileNamespace = new_tree
    new_tree.showDirectoryStructure(metaData.fileNamespace)
    master_state = open('masterState','ab')
    pickle.dump(metaData, master_state)
    master_state.close()

bgthread = BgPoolChunkServer(metaData, container, self_ip_port)

bgthreadoplog = BgSaveOperationLog(metaData)

tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tcpsock.bind((self_ip_port[0], int(self_ip_port[1])))


while True:
    tcpsock.listen(1000)
    (conn, (ip,port)) = tcpsock.accept()
    listenthread = ListenClientChunkServer(metaData, conn, self_ip_port[0], int(self_ip_port[1]), container)
    listenthread.daemon = True
    listenthread.start()
