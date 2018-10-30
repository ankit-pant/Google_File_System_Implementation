# -*- coding: utf-8 -*-
import hashlib
import json
CHUNKSIZE = 1024*1024*64

# not persistent---> chunks to chunkserver mapping
class ChunkLoc:
    def __init__(self):
        self.chunks_mapping = []
        
# persistent---> namespace stores file structure while metadata stores file to chunks mapping
class DumpObj:
    def __init__(self):
        self.fileNamespace=None
        self.metadata=[]

# used to create namespace
class Tree:
    def __init__(self, x=""):
        self.children_ptr = []
        self.children_name = []
        self.name = x
        self.isFile = False
        self.fileHash = ""
    
    def allocateServers(self):
        try:
            with open('chunk_servers.json') as f:
                chunk_servers = json.load(f)
        except IOError:
            print("Unable to locate chunkservers in the database!")
        chunk_servers.sort(key=lambda x: x["server_details"]["disk_free_space"], reverse=True)
        server_list = []
        p_replica = {}
        p_replica["ip"] = chunk_servers[0]["ip"]
        p_replica["port"] = chunk_servers[0]["port"]
        p_replica["type"] = "primary"
        s_replica1 = {}
        s_replica1["ip"] = chunk_servers[1]["ip"]
        s_replica1["port"] = chunk_servers[1]["port"]
        s_replica1["type"] = "secondary"
        s_replica2 = {}
        s_replica2["ip"] = chunk_servers[2]["ip"]
        s_replica2["port"] = chunk_servers[2]["port"]
        s_replica2["type"] = "secondary"
        server_list.append(p_replica)
        server_list.append(s_replica1)
        server_list.append(s_replica2)
        return server_list
    
    def fillMetaData(self, file_name, file_hash, metaObj, cmap):
        file_obj = {}
        file_obj["fileHashName"] = file_hash
        file_obj["chunkDetails"] = []
        
        file = open(file_name, "rb")
        try:
            bytes_read = file.read(CHUNKSIZE)
            c_num=0
            while bytes_read:
                fname=str(c_num)+".dat"
                f=open(fname, "wb")
                f.write(bytes_read)
                f.close()
                result = hashlib.sha1(bytes_read)
                chunk_hash = result.hexdigest()
                chunk = {}
                chunk["chunk_handle"] = chunk_hash
                chunk["chunk_index"] = c_num
                j = {}
                j["chunk_handle"]=chunk_hash
                j["servers"]=self.allocateServers()
                cmap.chunks_mapping.append(j)
                
                
                # ping chunkservers with the data
                file_obj["chunkDetails"].append(chunk)
                c_num+=1
                bytes_read = file.read(CHUNKSIZE)
        finally:
            file.close()
        metaObj.metadata.append(file_obj)
        return cmap, metaObj
        
    
    def traverseInsert(self, dir_path, tree_root, isFile, metaObj, cmap):
        dir_found = False
        if dir_path[0] == tree_root.name and tree_root.isFile==False:
            del dir_path[0]
            for ptr_loc in range(len(tree_root.children_name)):
                if dir_path[0] == tree_root.children_name[ptr_loc]:
                    tree_root = tree_root.children_ptr[ptr_loc]
                    dir_found = True
                    break
            if dir_found:
                return self.traverseInsert(dir_path, tree_root, isFile, metaObj, cmap)
            elif dir_found==False and dir_path:
                new_obj = Tree(dir_path[0])
                new_obj.isFile = isFile
                if isFile:
                    with open(dir_path[0], 'rb') as f:
                        file_content = f.read()
                    result = hashlib.sha1(file_content)
                    file_hash = result.hexdigest()
                    new_obj.fileHash = file_hash
                    incoming = self.fillMetaData(dir_path[0], file_hash, metaObj, cmap)
                tree_root.children_name.append(dir_path[0])
                tree_root.children_ptr.append(new_obj)
                return True, metaObj, cmap
        else:
            return False, incoming[0], incoming[1]
    
    def insert(self, name, isFile, tree_root, metaObj, cmap):
        parent_directories = name.split('/')
        null_idx = []
        i=0
        for dir in parent_directories:
            if dir=='':
                null_idx.append(i)
            i+=1
        for k in range(len(null_idx)):
            del parent_directories[k]
        if tree_root.name == "" and not tree_root.children_name:
            tree_root.name = name
            tree_root.isFile = isFile
            return True, metaObj, cmap
        else:
            insert_loc = self.traverseInsert(parent_directories, tree_root, isFile, metaObj, cmap)
            return insert_loc
        
    def showDirectoryStructure(self, tree_root):
        if tree_root==None:
            return False
        print(tree_root.name)
        for i in tree_root.children_ptr:
            tree_root=i
            self.showDirectoryStructure(tree_root)
    