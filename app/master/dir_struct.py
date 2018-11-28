# -*- coding: utf-8 -*-
import hashlib
import json
import configparser
import socket
from socket import error as socket_error
from threading import BoundedSemaphore
import copy
globalChunkMapping = None

config = configparser.RawConfigParser()
config.read('master.properties')
CHUNKSIZE = int(config.get('Master_Data','CHUNKSIZE'))

# not persistent---> chunks to chunkserver mapping
#==============================================================================
#   CHUNKS MAPPING
#   [{'chunk_handle': 'b37a3a8a72177acefead77b46c611eb664739754',
#   'servers': [{'ip': '192.168.43.90',
#     'isValidReplica': 1,
#     'port': 12004,
#     'type': 'secondary'},
#    {'ip': '192.168.43.90',
#     'isValidReplica': 1,
#     'port': 12005,
#     'type': 'primary'},
#    {'ip': '192.168.43.90',
#     'isValidReplica': 1,
#     'port': 12006,
#     'type': 'secondary'}]}, {.....}]
#==============================================================================

#==============================================================================
# SLAVES STATE
# [{'chunks': ['b37a3a8a72177acefead77b46c611eb664739754',
#    '1b640eb1c5263587504d240d44b90dcbb39d81cf',
#    '264491f7cdf87aa79906935b09f0bceabdb21084',
#    'f10c900990d31ec2b28a8aa451c809e9a7876905',
#    '1a0bbc11c05684237b5792bef3f783fa4ddbe994',
#    'c92875fca0ed4e4b7640b54f7ef0ef446b5bdf2c',
#    'ec4e1127ab1cb8725f8636172816dcd79023eb22',
#    '445fe93f7fd422910e0464803d0d7563be6fa2c4',
#    '4eb0196ac43668b11280a5641426bb6e9722616c',
#    'cf0e53327567457e5dc5cd3ad7c0e76d83c9e13d',
#    'e8e9aaa71b5ee21e3185c1a96111ec784c2677ff',
#    '05f5cf65cc5143d1d80bb3a5d9dd03c633ebadca'],
#   'disk_free_space': 65470001152,
#   'ip': '192.168.43.90',
#   'port': 12004}, {.......}]
#==============================================================================

class ChunkLoc:
    def __init__(self):
        self.chunks_mapping = []
        self.slaves_state = []

# persistent---> namespace stores file structure while metadata stores file to chunks mapping
#==============================================================================
# CHUNKSDB
# ['7b6c57ead60ec8726bc859f02be5d11c2d644a8a',
#  '56b629cf3ae3bac4e80aa7ca897eb835ba989529',
#  'b37a3a8a72177acefead77b46c611eb664739754',
#  '1b640eb1c5263587504d240d44b90dcbb39d81cf',.........,]
#==============================================================================

#==============================================================================
# METADATA
# [{'chunkDetails': [{'chunk_handle': '7b6c57ead60ec8726bc859f02be5d11c2d644a8a',
#     'chunk_index': 0},{........}],
#     'fileHashName': 'fd95d257c98f2dba866571de1c43e8c93b058c6a'}]
#==============================================================================
class DumpObj:
    def __init__(self):
        self.fileNamespace=None
        self.metadata=[]
        self.chunksDB=[]
        self.slaves_list=[]

# used to create namespace
class Tree:
    def __init__(self, x=""):
        self.children_ptr = []
        self.children_name = []
        self.name = x
        self.isFile = False
        self.fileHash = ""
    
    def allocateServers(self, container, metaObj):
        container.acquire()
        try:
            with open('chunk_servers.json') as f:
                chunk_servers = json.load(f)
        except IOError:
            print("Unable to locate chunkservers in the database!")
        container.release()
        chunk_servers.sort(key=lambda x: x["disk_free_space"], reverse=True)
        server_list = []
        p_replica = {}
        p_replica["ip"] = chunk_servers[0]["ip"]
        p_replica["port"] = chunk_servers[0]["port"]
        p_replica["type"] = "pri"
        s_replica1 = {}
        s_replica1["ip"] = chunk_servers[1]["ip"]
        s_replica1["port"] = chunk_servers[1]["port"]
        s_replica1["type"] = "sec"
        s_replica2 = {}
        s_replica2["ip"] = chunk_servers[2]["ip"]
        s_replica2["port"] = chunk_servers[2]["port"]
        s_replica2["type"] = "sec"
        server_list.append(p_replica)
        server_list.append(s_replica1)
        server_list.append(s_replica2)
        chunk_servers[0]["disk_free_space"] -= CHUNKSIZE
        chunk_servers[1]["disk_free_space"] -= CHUNKSIZE
        chunk_servers[2]["disk_free_space"] -= CHUNKSIZE
        container.acquire()
        k=open('chunk_servers.json', 'w')
        chunk_servers_updated = json.dumps(chunk_servers)
        k.write(chunk_servers_updated)
        container.release()
        metaObj.slaves_list[:]=[]
        metaObj.slaves_list = copy.deepcopy(chunk_servers)
        return server_list, metaObj
    
    def fillMetaData(self, file_name, file_hash, metaObj, container):
        file_obj = {}
        file_obj["fileHashName"] = file_hash
        file_obj["chunkDetails"] = []
        file = open(file_name, "rb")
        try:
            bytes_read = file.read(CHUNKSIZE)
            c_num=0
            while bytes_read:
#==============================================================================
#                 fname=str(c_num)+".dat"
#                 f=open(fname, "wb")
#                 f.write(bytes_read)
#                 f.close()
#==============================================================================
                result = hashlib.sha1(bytes_read)
                chunk_hash = result.hexdigest()
                chunk = {}
                chunk["chunk_handle"] = chunk_hash
                chunk["chunk_index"] = c_num
                j = {}
                j["chunk_handle"]=chunk_hash
                metaObj.chunksDB.append(chunk_hash)
                j["servers"], metaObj=self.allocateServers(container,metaObj)
                globalChunkMapping.chunks_mapping.append(j)
                
                DELIMITER = config.get('Master_Data', 'DELIMITER')
                for chunk_server in j["servers"]:
                    try:
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        print("Socket created, connecting to"+chunk_server["ip"]+":"+str(chunk_server["port"]))
                        s.connect((chunk_server["ip"], chunk_server["port"]))
                        print("Connected, preparing data.")
                        data_len = str(len(bytes_read))
                        while len(data_len)<8:
                            data_len = "0"+data_len
                        x=CHUNKSIZE-len(bytes_read)
                        new_str = "0"*x
                        padded_bytes = bytes_read+new_str.encode()
                        print("Data length is: ", data_len)
                        headers = DELIMITER+"store"+DELIMITER+chunk_hash+DELIMITER+chunk_server["type"]+DELIMITER+data_len+DELIMITER
                        if len(headers)<200:
                            headers = headers.ljust(200)
                        chunk_data = headers.encode()+padded_bytes
                        print("sending data to: "+chunk_server["ip"]+":"+str(chunk_server["port"]))
                        print("Size of the sending data is: "+str(len(chunk_data)))
                        f = s.sendall(chunk_data)
                        s.close()
                    except socket_error as serr:
                        print("Unable to connect to the chunk server")
                        continue
                file_obj["chunkDetails"].append(chunk)
                c_num+=1
                bytes_read = file.read(CHUNKSIZE)
        finally:
            file.close()
        metaObj.metadata.append(file_obj)
        return metaObj
        
    
    def traverseInsert(self, dir_path, tree_root, isFile, metaObj, container):
        dir_found = False
        if dir_path[0] == tree_root.name and tree_root.isFile==False:
            del dir_path[0]
            for ptr_loc in range(len(tree_root.children_name)):
                if dir_path[0] == tree_root.children_name[ptr_loc]:
                    tree_root = tree_root.children_ptr[ptr_loc]
                    dir_found = True
                    break
            if dir_found:
                return self.traverseInsert(dir_path, tree_root, isFile, metaObj, container)
            elif dir_found==False and dir_path:
                new_obj = Tree(x=dir_path[0])
                new_obj.isFile = isFile
                if isFile:
                    with open(dir_path[0], 'rb') as f:
                        file_content = f.read()
                    result = hashlib.sha1(file_content)
                    file_hash = result.hexdigest()
                    new_obj.fileHash = file_hash
                    incoming = self.fillMetaData(dir_path[0], file_hash, metaObj, container)
                tree_root.children_name.append(dir_path[0])
                tree_root.children_ptr.append(new_obj)
                return True, metaObj
        else:
            return False, incoming
    
    def insert(self, name, isFile, tree_root, metaObj, container):
        parent_directories = name.split('/')
        null_idx = []
        i=0
        for dir in parent_directories:
            if dir=='':
                null_idx.append(i)
            i+=1
        k=len(null_idx)-1
        while k>=0:
            del parent_directories[null_idx[k]]
            k-=1
        if tree_root.name == "" and not tree_root.children_name:
            tree_root.name = name
            tree_root.isFile = isFile
            return True, metaObj
        else:
            insert_loc = self.traverseInsert(parent_directories, tree_root, isFile, metaObj, container)
            return insert_loc
        
    def showDirectoryStructure(self, tree_root):
        if tree_root==None:
            return False
        print(tree_root.name+"==>>"+str(tree_root.isFile))
        for i in tree_root.children_ptr:
            tree_root=i
            self.showDirectoryStructure(tree_root)
          
    def traverseTree(self, tree_root, fileName_arr):
        if tree_root.name == fileName_arr[0]:
            del fileName_arr[0]
            for i in range(len(tree_root.children_name)):
                if tree_root.children_name[i] == fileName_arr[0] and len(fileName_arr)==1 and tree_root.children_ptr[i].isFile:
                    return tree_root.children_ptr[i].fileHash
                elif tree_root.children_name[i] == fileName_arr[0] and len(fileName_arr)>1:
                    tree_root = tree_root.children_ptr[i]
                    return self.traverseTree(tree_root, fileName_arr)
        return None
        
        
    def retrieveHandle(self, file_name):
        fileName_arr = file_name.split('/')
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
        return self.traverseTree(self, fileName_arr)
    
    def traverseRemove(self, tree_root, fileName_arr):
        if tree_root.name == fileName_arr[0]:
            del fileName_arr[0]
            for i in range(len(tree_root.children_name)):
                if tree_root.children_name[i] == fileName_arr[0] and len(fileName_arr)==1 and tree_root.children_ptr[i].isFile:
                    removed_obj = tree_root.children_ptr[i]
                    del tree_root.children_ptr[i]
                    del tree_root.children_name[i]
                    del removed_obj
                    break
                elif tree_root.children_name[i] == fileName_arr[0] and len(fileName_arr)>1:
                    tree_root = tree_root.children_ptr[i]
                    self.traverseRemove(tree_root, fileName_arr)
                    break
            
    def removeEntry(self, file_name):
        fileName_arr = file_name.split('/')
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
        self.traverseRemove(self, fileName_arr)