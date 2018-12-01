from threading import Thread
import json
import socket
import configparser
import dir_struct
import delete_file
import copy
import reReplicateChunk
import pickle
import sys
import snapshot

config = configparser.RawConfigParser()
config.read('master.properties')
Json_rcv_limit = int(config.get('Master_Data','JSON_RCV_LIMIT'))
DELIMITER = config.get('Master_Data','DELIMITER')
RCVCHUNKSIZE = int(config.get('Master_Data','CHUNK_RECSIZE'))

class ListenClientChunkServer(Thread):
    def __init__(self, metaData, sock, self_ip, self_port,container):
        Thread.__init__(self)
        self.metaData = metaData
        self.sock = sock
        self.ip = self_ip
        self.port = self_port
        self.container = container
    
    def send_json_data(self, ip, port, data):
        try:    
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.connect((ip, port))
            s.sendall(str(data).encode())
            s.close()
        except:
            print("Connection refused by the master: "+ip+":"+str(port))
    
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
        data = []
        while True:
            data_rcv = self.sock.recv(RCVCHUNKSIZE)
            if not data_rcv:
                break
            data.append(data_rcv)
        data = b''.join(data)
        
        try:
            data = data.decode()
            data = data.replace("\'", "\"")
            j = json.loads(data)
            if j["agent"]=="client":
                client_ip=j["ip"]
                client_port=j["port"]
                if j["action"]=="read":
                    file_name = j["data"]["file_name"]
                    file_handle = self.metaData.fileNamespace.retrieveHandle(file_name)
                    response_data = {}
                    response_data["agent"] = "master"
                    response_data["ip"]=self.ip
                    response_data["port"]=self.port
                    response_data["action"] = "response/read"
                    response_data["data"] = []
                    if file_handle == None:
                        response_data["responseStatus"] = 404
                        print("File not found")
                    else:
                        print("File found")
                        response_data["responseStatus"] = 200
                        handle_data = self.metaData.metadata
                        for file in handle_data:
                            if file["fileHashName"] == file_handle:
                                for chunk in file["chunkDetails"]:
                                    if chunk["chunk_index"] in j["data"]["idx"]:
                                        handle_details = {}
                                        handle_details["chunk_handle"] = chunk["chunk_handle"]
                                        handle_details["chunk_index"] = chunk["chunk_index"]
                                        print("Printing.........")
                                        print(dir_struct.globalChunkMapping.chunks_mapping)
                                        #self.container.acquire()
                                        for cmap in dir_struct.globalChunkMapping.chunks_mapping:
                                            if cmap["chunk_handle"] == chunk["chunk_handle"]:
                                                handle_details["chunk_servers"] = copy.deepcopy(cmap["servers"])
                                                break
                                        #self.container.release()
                                        response_data["data"].append(handle_details)
                                break
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((client_ip, client_port))
                    s.sendall(str(response_data).encode())
                    s.close()
                elif j["action"]=="snapshot":
                    self.sock.close()
                    snapshot.takeSnap(self.metaData, j["data"]["dir_path"], self.ip, self.port)
                elif j["action"]=="restore_snapshot":
                    dirName = j["data"]["dir_path"]
                    for snaps in self.metaData.snapshotRecord:
                        if snaps["path"] == dirName:
                            folderName = snaps["time"]
                            host_ip = snaps["slave_details"]["ip"]
                            host_port = snaps["slave_details"]["port"]
                            break
                    retrieve_snap = {}
                    retrieve_snap["agent"] = "master"
                    retrieve_snap["action"] = "restore_snapshot"
                    retrieve_snap["ip"] = self.ip
                    retrieve_snap["port"] = self.port
                    retrieve_snap["data"] = {}
                    retrieve_snap["data"]["folder"] = folderName
                    retrieve_snap["data"]["directory"] = dirName
                    self.send_json_data(host_ip, host_port, retrieve_snap)
                elif j["action"] == "delete_file":
                    file_name = j["data"]["file_path"]
                    self.sock.close()
                    resp_data = {}
                    resp_data["agent"]="master"
                    resp_data["action"]="delete/response"
                    resp_data["ip"]=self.ip
                    resp_data["port"]=self.port
                    resp_data["data"]={}
                    if delete_file.DeleteFile(file_name, self.metaData, self.ip, self.port):
                        resp_data["data"]["ok_status"] = 1    
                    else:
                        resp_data["data"]["ok_status"] = 0
                    self.send_json_data(j["ip"], j["port"], resp_data)
                    print(self.metaData.chunksDB)
                    print(self.metaData.metadata)
                    print(dir_struct.globalChunkMapping.chunks_mapping)
                    print(dir_struct.globalChunkMapping.slaves_state)
            elif j["agent"]=="chunk_server":
                if j["action"]=="report_ack":
                    valid_slave = False
                    self.container.acquire()
                    k = open('chunk_servers.json','r')
                    x=k.read()
                    print("On reading the data is: ",x)
                    try:
                        with open('chunk_servers.json') as f:
                            chunk_servers = json.load(f)
                    except IOError:
                        print("Unable to locate chunkservers in the database!")
                    self.container.release()
                    for schunk in chunk_servers:
                        if schunk["ip"] == j["ip"] and schunk["port"] == j["port"]:
                            valid_slave = True
                            break
                    if valid_slave:
                        orphaned_chunks_list = []
                        for specific_chunk in j["data"]:
                            sc_handle = specific_chunk["chunk_handle"]
                            server_id = {}
                            server_id["ip"]=j["ip"]
                            server_id["port"]=j["port"]
                            server_id["type"]=specific_chunk["type"]
                            server_id["isValidReplica"] = 1
                            found = False
                            #self.container.acquire()
                            for i in range(len(dir_struct.globalChunkMapping.chunks_mapping)):
                                if dir_struct.globalChunkMapping.chunks_mapping[i]["chunk_handle"] == sc_handle:
                                    if server_id not in dir_struct.globalChunkMapping.chunks_mapping[i]["servers"]:
                                        dir_struct.globalChunkMapping.chunks_mapping[i]["servers"].append(server_id)
                                    found = True
                                    break
                            #self.container.release()
                            if found==False:
                                if sc_handle in self.metaData.chunksDB:
                                    new_entry = {}
                                    new_entry["chunk_handle"] = sc_handle
                                    new_entry["servers"] = []
                                    new_entry["servers"].append(server_id)
                                    #self.container.acquire()
                                    dir_struct.globalChunkMapping.chunks_mapping.append(new_entry)
                                    #self.container.release()
                                else:
                                    orphaned_chunks_list.append(sc_handle)
                        found = False
                        fresh_chunks = [item["chunk_handle"] for item in j["data"] if item["chunk_handle"] not in orphaned_chunks_list]
                        #self.container.acquire()
                        for i in range(len(dir_struct.globalChunkMapping.slaves_state)):
                            if dir_struct.globalChunkMapping.slaves_state[i]["ip"] == j["ip"] and dir_struct.globalChunkMapping.slaves_state[i]["port"] == j["port"]:
                                dir_struct.globalChunkMapping.slaves_state[i]["disk_free_space"] = j["extras"]
                                del dir_struct.globalChunkMapping.slaves_state[i]["chunks"][:]
                                dir_struct.globalChunkMapping.slaves_state[i]["chunks"] = fresh_chunks
                                found = True
                                break
                        
                        for i in range(len(chunk_servers)):
                            if chunk_servers[i]["ip"] == j["ip"] and chunk_servers[i]["port"] == j["port"]:
                                chunk_servers[i]["disk_free_space"] = j["extras"]
                                break
                        self.container.acquire()
                        f = open('chunk_servers.json','w')
                        jsonString = json.dumps(chunk_servers)
                        f.write(jsonString)
                        f.close()
                        self.container.release()
                        #self.container.release()
                        if found == False:
                            new_entry = {}
                            new_entry["ip"]=j["ip"]
                            new_entry["port"]=j["port"]
                            new_entry["disk_free_space"]=j["extras"]
                            new_entry["chunks"] = fresh_chunks
                            #container.acquire()
                            dir_struct.globalChunkMapping.slaves_state.append(new_entry)
                            #container.release()
                        
                        new_res = {}
                        new_res["ip"]=self.ip
                        new_res["port"]=self.port
                        new_res["agent"]="master"
                        new_res["action"]="report/response"
                        if len(orphaned_chunks_list)>0:
                            new_res["response_status"]="orphaned_chunks"
                            new_res["data"]=orphaned_chunks_list
                        else:
                            new_res["response_status"]="OK"
                            new_res["data"]=[]
                        try:
                            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            s.connect((j["ip"], j["port"]))
                            s.sendall(str(new_res).encode())
                            s.close()
                        except:
                            print("Slave is down")
                elif j["action"] == "manipulated_chunk_found":
                    for wrong_chunk in j["data"]:
                        right_ips = []
                        #container.acquire()
                        for l in range(len(dir_struct.globalChunkMapping.chunks_mapping)):
                            if dir_struct.globalChunkMapping.chunks_mapping[l]["chunk_handle"] == wrong_chunk:
                                for m in range(len(dir_struct.globalChunkMapping.chunks_mapping[l]["servers"])):
                                    servers = dir_struct.globalChunkMapping.chunks_mapping[l]["servers"][m]
                                    if (servers["ip"] != j["ip"] or servers["port"] != j["port"]) and servers["isValidReplica"]:
                                        right_ips.append(servers["ip"])
                                    elif servers["ip"] == j["ip"] or servers["port"] == j["port"]:
                                        dir_struct.globalChunkMapping.chunks_mapping[l]["servers"][m]["isValidReplica"]=0
                                r_ip = []
                                for x in right_ips:
                                    y=x.split('.')
                                    r_ip.append(y)
                                seeding_ip = self.find_nearest(r_ip)
                                for server in dir_struct.globalChunkMapping.chunks_mapping[l]["servers"]:
                                    if server["ip"] == seeding_ip and server["isValidReplica"]:
                                        seeding_port = server["port"]
                                        break
                                break
                        #container.release()
                        # ask seeding ip and port to seed data 
                        seeding_data = {}
                        seeding_data["agent"] = "master"
                        seeding_data["action"] = "seedChunkToSlave"
                        seeding_data["ip"] = self.ip
                        seeding_data["port"] = self.port
                        seeding_data["data"] = {}
                        seeding_data["data"]["infected_slave_ip"] = j["ip"]
                        seeding_data["data"]["infected_slave_port"] = j["port"]
                        seeding_data["data"]["infected_chunk_handle"] = wrong_chunk
                        self.sock.close()
                        self.send_json_data(seeding_ip, seeding_port, seeding_data)
                
                elif j["action"] == "resto":
                    print("Message Recieved by slave to set up the replica bit")
                    #container.acquire()
                    for l in range(len(dir_struct.globalChunkMapping.chunks_mapping)):
                            if dir_struct.globalChunkMapping.chunks_mapping[l]["chunk_handle"] == j["data"]["handle"]:
                                for m in range(len(dir_struct.globalChunkMapping.chunks_mapping[l]["servers"])):
                                    servers = dir_struct.globalChunkMapping.chunks_mapping[l]["servers"][m]
                                    if (servers["ip"] != j["ip"] or servers["port"] != j["port"]) and not servers["isValidReplica"]:
                                        dir_struct.globalChunkMapping.chunks_mapping[l]["servers"][m]["isValidReplica"] = 1
                                        print("Field of valid replica set to true")
                    #container.release()
                elif j["action"] == "new_chunk_server":
                    slave_found = False
                    #container.acquire()
                    for slaves in dir_struct.globalChunkMapping.slaves_state:
                        if slaves["ip"] == j["ip"] and slaves["port"] == j["port"]:
                            slave_found = True
                            break
                    #container.release()
                    if not slave_found:
                        allSlavesUpdated = False
                        while not allSlavesUpdated:
                            allSlavesUpdated = True
                            #container.acquire()
                            for slaves in dir_struct.globalChunkMapping.slaves_state:
                                if len(slaves["chunks"]) == 0:
                                    allSlavesUpdated = False
                                    print("slaves not updated")
                                    break
                            #container.release()
                        reReplicateChunk.distribute_load(self.ip, self.port, j["ip"], j["port"], self.metaData, self.container, j["data"]["disk_free_space"], "new_added")
        except UnicodeDecodeError:
            print("Unexpected error:", sys.exc_info()[0])
            print("size of the data is: "+str(len(data)))
            flags = data[0:250]
            data = data[250:]
            print("size of the data after stripping is: "+str(len(data)))
            print(flags)
            flags = flags.decode()
            flags=flags.strip()
            headers = flags.split(DELIMITER)
            null_idx = []
            i=0
            for flag in headers:
                if flag=='':
                    null_idx.append(i)
                i+=1
            k=len(null_idx)-1
            while k>=0:
                del headers[null_idx[k]]
                k-=1
            action = headers[0]
            if action == "distribute":
                client_ip = headers[1]
                client_port = headers[2]
                file_name = headers[3]
                
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
                
                fname = fileName_arr[len(fileName_arr)-1]
                k=open(fname,"wb")
                k.write(data)
                k.close()
                incoming = self.metaData.fileNamespace.insert(file_name, 1, self.metaData.fileNamespace, self.metaData, self.container)
                self.metaData = incoming[1]
                self.metaData.fileNamespace.showDirectoryStructure(self.metaData.fileNamespace)
            elif action == "meta_file":
                snapped_dir = headers[1]
                snapped = pickle.loads(data)
                fileName_arr = snapped_dir.split('/')
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
                copy_farr = copy.deepcopy(fileName_arr)
                
                if self.metaData.fileNamespace.snapRemove(self.metaData.fileNamespace, copy_farr):
                    print("Successfully removed ",snapped_dir," from the directory tree")
                else:
                    print("Error in removing ",snapped_dir," from directory tree")
                
                if self.metaData.fileNamespace.mergeTrees(fileName_arr, self.metaData.fileNamespace, snapped.nameSpace):
                    print("Successfully stored ",snapped_dir," in the directory tree")
                else:
                    print("Error in storing ",snapped_dir," in the directory tree")
                    
                