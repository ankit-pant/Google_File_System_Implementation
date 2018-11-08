from threading import Thread
import json
import socket
import configparser
import dir_struct
import copy
import reReplicateChunk
import pickle

config = configparser.RawConfigParser()
config.read('master.properties')
Json_rcv_limit = int(config.get('Master_Data','JSON_RCV_LIMIT'))

class ListenClientChunkServer(Thread):
    def __init__(self, metaData, sock, self_ip, self_port):
        Thread.__init__(self)
        self.metaData = metaData
        self.sock = sock
        self.ip = self_ip
        self.port = self_port
    
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
        data = self.sock.recv(Json_rcv_limit)
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
                                    for cmap in dir_struct.globalChunkMapping.chunks_mapping:
                                        if cmap["chunk_handle"] == chunk["chunk_handle"]:
                                            handle_details["chunk_servers"] = copy.deepcopy(cmap["servers"])
                                            break
                                    response_data["data"].append(handle_details)
                            break
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((client_ip, client_port))
                s.sendall(str(response_data).encode())
                s.close()
            elif j["action"]=="snapshot":
                print("Client trying to snapshot")
        elif j["agent"]=="chunk_server":
            if j["action"]=="report_ack":
                valid_slave = False
                try:
                    with open('chunk_servers.json') as f:
                        chunk_servers = json.load(f)
                except IOError:
                    print("Unable to locate chunkservers in the database!")
                
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
                        for i in range(len(dir_struct.globalChunkMapping.chunks_mapping)):
                            if dir_struct.globalChunkMapping.chunks_mapping[i]["chunk_handle"] == sc_handle:
                                if server_id not in dir_struct.globalChunkMapping.chunks_mapping[i]["servers"]:
                                    dir_struct.globalChunkMapping.chunks_mapping[i]["servers"].append(server_id)
                                found = True
                                break
                        if found==False:
                            if sc_handle in self.metaData.chunksDB:
                                new_entry = {}
                                new_entry["chunk_handle"] = sc_handle
                                new_entry["servers"] = []
                                new_entry["servers"].append(server_id)
                                dir_struct.globalChunkMapping.chunks_mapping.append(new_entry)
                            else:
                                orphaned_chunks_list.append(sc_handle)
                    
                    found = False
                    fresh_chunks = [item["chunk_handle"] for item in j["data"] if item["chunk_handle"] not in orphaned_chunks_list]
                    for i in range(len(dir_struct.globalChunkMapping.slaves_state)):
                        if dir_struct.globalChunkMapping.slaves_state[i]["ip"] == j["ip"] and dir_struct.globalChunkMapping.slaves_state[i]["port"] == j["port"]:
                            dir_struct.globalChunkMapping.slaves_state[i]["disk_free_space"] = j["extras"]
                            del dir_struct.globalChunkMapping.slaves_state[i]["chunks"][:]
                            dir_struct.globalChunkMapping.slaves_state[i]["chunks"] = fresh_chunks
                            found = True
                            break
                    if found == False:
                        new_entry = {}
                        new_entry["ip"]=j["ip"]
                        new_entry["port"]=j["port"]
                        new_entry["disk_free_space"]=j["extras"]
                        new_entry["chunks"] = fresh_chunks
                        dir_struct.globalChunkMapping.slaves_state.append(new_entry)
                    
                    
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
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.connect((j["ip"], j["port"]))
                    s.sendall(str(new_res).encode())
                    s.close()
            elif j["action"] == "manipulated_chunk_found":
                for wrong_chunk in j["data"]:
                    right_ips = []
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
                for l in range(len(dir_struct.globalChunkMapping.chunks_mapping)):
                        if dir_struct.globalChunkMapping.chunks_mapping[l]["chunk_handle"] == j["data"]["handle"]:
                            for m in range(len(dir_struct.globalChunkMapping.chunks_mapping[l]["servers"])):
                                servers = dir_struct.globalChunkMapping.chunks_mapping[l]["servers"][m]
                                if (servers["ip"] != j["ip"] or servers["port"] != j["port"]) and not servers["isValidReplica"]:
                                    dir_struct.globalChunkMapping.chunks_mapping[l]["servers"][m]["isValidReplica"] = 1
                                    print("Field of valid replica set to true")
            elif j["action"] == "new_chunk_server":
                print("1")
                slave_found = False
                for slaves in dir_struct.globalChunkMapping.slaves_state:
                    if slaves["ip"] == j["ip"] and slaves["port"] == j["port"]:
                        slave_found = True
                        break
                if not slave_found:
                    print("2")
                    allSlavesUpdated = False
                    while not allSlavesUpdated:
                        allSlavesUpdated = True
                        for slaves in dir_struct.globalChunkMapping.slaves_state:
                            if len(slaves["chunks"]) == 0:
                                allSlavesUpdated = False
                                print("slaves not updated")
                                break
                    reReplicateChunk.distribute_load(self.ip, self.port, j["ip"], j["port"], self.metaData, j["data"]["disk_free_space"], "new_added")