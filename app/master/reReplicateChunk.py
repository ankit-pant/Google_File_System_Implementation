import configparser
import dir_struct
import socket
import json
import copy

config = configparser.RawConfigParser()
config.read('master.properties')
CHUNKSIZE = int(config.get('Master_Data','CHUNKSIZE'))

def isValidChunk(chunk_handle, server_ip, server_port):
    for chunk in dir_struct.globalChunkMapping.chunks_mapping:    
        if chunk["chunk_handle"] == chunk_handle:
            for server in chunk["servers"]:
                if server["ip"] == server_ip and server["port"] == server_port:
                    if server["isValidReplica"]:
                        return True
                    else:
                        return False


def find_nearest(ips, self_ip):
    ip_arr = self_ip.split('.')
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


def distribute_load(self_ip, self_port, target_ip, target_port, disk_free_space=0, task="new_added"):
    if task == "new_added":
        print("Free disk space is: ",disk_free_space)
        max_slave_capacity = disk_free_space/CHUNKSIZE
        total_slaves = len(dir_struct.globalChunkMapping.slaves_state)
        total_chunks = 0
        for slaves in dir_struct.globalChunkMapping.slaves_state:
            total_chunks += len(slaves["chunks"])

        avg_chunks = total_chunks/(total_slaves+1)
        if max_slave_capacity < avg_chunks:
            allocate_chunks = int(max_slave_capacity)
        else:
            allocate_chunks = int(avg_chunks)
        print("Chunks to be allocated is: ", allocate_chunks)
        new_server_chunks = []
        
        mod_servers_list = []
        
        for i in range(allocate_chunks):
            dir_struct.globalChunkMapping.slaves_state.sort(key=lambda x: len(x["chunks"]), reverse=True)
            if len(dir_struct.globalChunkMapping.slaves_state[0]["chunks"]) > avg_chunks:
                for m in range(len(dir_struct.globalChunkMapping.slaves_state[0]["chunks"])):
                    slave_chunk_handle = dir_struct.globalChunkMapping.slaves_state[0]["chunks"][m]
                    if (slave_chunk_handle not in new_server_chunks) and isValidChunk(slave_chunk_handle, dir_struct.globalChunkMapping.slaves_state[0]["ip"], dir_struct.globalChunkMapping.slaves_state[0]["port"]):
                        new_server_chunks.append(slave_chunk_handle)
                        
                        #update chunksMapping of slave_chunk_handle at this point
                        already_exist = False
                        for n in range(len(mod_servers_list)):
                            if mod_servers_list[n]["ip"] == dir_struct.globalChunkMapping.slaves_state[0]["ip"] and mod_servers_list[n]["port"] == dir_struct.globalChunkMapping.slaves_state[0]["port"]:
                                already_exist = True
                                bal_chunks = {}
                                bal_chunks["type"] = ""
                                bal_chunks["handle"] = slave_chunk_handle
                                mod_servers_list[n]["chunks"].append(bal_chunks)
                                break
                        
                        if not already_exist:
                            mod_server = {}
                            mod_server["ip"] = dir_struct.globalChunkMapping.slaves_state[0]["ip"]
                            mod_server["port"] = dir_struct.globalChunkMapping.slaves_state[0]["port"]
                            mod_server["chunks"] = []
                            bal_chunks = {}
                            bal_chunks["handle"] = slave_chunk_handle
                            bal_chunks["type"] = ""
                            mod_server["chunks"].append(bal_chunks)
                            mod_servers_list.append(mod_server)
                            
                        for p in range(len(dir_struct.globalChunkMapping.chunks_mapping)):
                            if dir_struct.globalChunkMapping.chunks_mapping[p]["chunk_handle"] == slave_chunk_handle:
                                for q in range(len(dir_struct.globalChunkMapping.chunks_mapping[p]["servers"])):
                                    if dir_struct.globalChunkMapping.chunks_mapping[p]["servers"][q]["ip"] == dir_struct.globalChunkMapping.slaves_state[0]["ip"] and dir_struct.globalChunkMapping.chunks_mapping[p]["servers"][q]["port"] == dir_struct.globalChunkMapping.slaves_state[0]["port"]:
                                       dir_struct.globalChunkMapping.chunks_mapping[p]["servers"][q]["ip"] = target_ip
                                       dir_struct.globalChunkMapping.chunks_mapping[p]["servers"][q]["port"] = target_port
                                       break
                                break
                        dir_struct.globalChunkMapping.slaves_state[0]["disk_free_space"] += 64*1024*1024
                        dir_struct.globalChunkMapping.slaves_state[0]["chunks"].remove(slave_chunk_handle)
                        break
            else:
                break
        
        for seed_servers in mod_servers_list:
            seed_ip = seed_servers["ip"]
            seed_port = seed_servers["port"]
            sending_data = {}
            sending_data["agent"] = "master"
            sending_data["action"] = "balance_load"
            sending_data["ip"] = self_ip
            sending_data["port"] = self_port
            sending_data["data"] = {}
            sending_data["data"]["target_ip"] = target_ip
            sending_data["data"]["target_port"] = target_port
            sending_data["data"]["balancing_chunk_handles"] = seed_servers["chunks"]
            
            try:    
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((seed_ip, seed_port))
                s.sendall(str(sending_data).encode())
                s.close()
            except:
                print("Connection refused by the slave server: "+seed_ip+":"+str(seed_port))
                    
        new_slave_entry = {}
        new_slave_entry["ip"] = target_ip
        new_slave_entry["port"] = target_port
        new_slave_entry["disk_free_space"] = disk_free_space
        new_slave_entry["chunks"] = new_server_chunks
        dir_struct.globalChunkMapping.slaves_state.append(new_slave_entry)
        try:
            with open('chunk_servers.json') as f:
                old_chunk_servers = json.load(f)
        except IOError:
            print("Unable to locate chunkservers in the database!")
        new_slave = {}
        new_slave["ip"] = target_ip
        new_slave["port"] = target_port
        new_slave["chunks"] = []
        new_slave["disk_free_space"] = disk_free_space
        old_chunk_servers.append(new_slave)
        
        f = open('chunk_servers.json', 'w')
        jsonString = json.dumps(old_chunk_servers)
        f.write(jsonString)
        f.close()
        
    elif task == "old_removed":
        print("A slave with ip: ", target_ip, " and port: ",target_port, "is down")
        target_chunks = []
        for x in range(len(dir_struct.globalChunkMapping.slaves_state)):
            if dir_struct.globalChunkMapping.slaves_state[x]["ip"] == target_ip and dir_struct.globalChunkMapping.slaves_state[x]["port"] == target_port:
                target_chunks = copy.deepcopy(dir_struct.globalChunkMapping.slaves_state[x]["chunks"])
                del dir_struct.globalChunkMapping.slaves_state[x]
                break
        updated_obj = []   
        for t_chunk in target_chunks:
            dir_struct.globalChunkMapping.slaves_state.sort(key=lambda x: x["disk_free_space"], reverse=True)
            updated_entry = {}
            for i in range(len(dir_struct.globalChunkMapping.slaves_state)):
                if t_chunk not in dir_struct.globalChunkMapping.slaves_state[i]["chunks"]:
                    dir_struct.globalChunkMapping.slaves_state[i]["chunks"].append(t_chunk)
                    chunk_type = ""
                    for j in range(len(dir_struct.globalChunkMapping.chunks_mapping)):
                        if dir_struct.globalChunkMapping.chunks_mapping[j]["chunk_handle"] == t_chunk:
                            right_ips = []
                            new_server = {}
                            for k in range(len(dir_struct.globalChunkMapping.chunks_mapping[j]["servers"])):
                                if dir_struct.globalChunkMapping.chunks_mapping[j]["servers"][k]["ip"] == target_ip and dir_struct.globalChunkMapping.chunks_mapping[j]["servers"][k]["port"] == target_port:
                                    chunk_type = dir_struct.globalChunkMapping.chunks_mapping[j]["servers"][k]["type"]
                                    new_server["type"] = chunk_type
                                    del_index = k
                                else:
                                    right_ips.append(dir_struct.globalChunkMapping.chunks_mapping[j]["servers"][k]["ip"])        
                            del dir_struct.globalChunkMapping.chunks_mapping[j]["servers"][del_index]
                            r_ip = []
                            for x in right_ips:
                                y=x.split('.')
                                r_ip.append(y)
                            seeding_ip = find_nearest(r_ip, self_ip)
                            for server in dir_struct.globalChunkMapping.chunks_mapping[j]["servers"]:
                                if server["ip"] == seeding_ip and server["isValidReplica"]:
                                    seeding_port = server["port"]
                                    break        
                            updated_entry["seeding_ip"] = seeding_ip
                            updated_entry["seeding_port"] = seeding_port
                            new_server["ip"] = dir_struct.globalChunkMapping.slaves_state[i]["ip"]
                            new_server["port"] = dir_struct.globalChunkMapping.slaves_state[i]["port"]
                            new_server["isValidReplica"] = 1
                            dir_struct.globalChunkMapping.chunks_mapping[j]["servers"].append(new_server)
                            break
                    dir_struct.globalChunkMapping.slaves_state[i]["disk_free_space"] -= 64*1024*1024
                    obj_created = False
                    for l in range(len(updated_obj)):
                        c_server = updated_obj[l]
                        if c_server["recieving_ip"] == dir_struct.globalChunkMapping.slaves_state[i]["ip"] and c_server["recieving_port"] == dir_struct.globalChunkMapping.slaves_state[i]["port"] and c_server["seeding_ip"] == seeding_ip and c_server["seeding_port"] == seeding_port:
                            chunk_details = {}
                            chunk_details["type"] = new_server["type"]
                            chunk_details["handle"] = t_chunk
                            updated_obj[l]["data"].append(chunk_details)
                            obj_created = True
                            break
                    if not obj_created:
                        chunk_details = {}
                        chunk_details["type"] = new_server["type"]
                        chunk_details["handle"] = t_chunk
                        updated_entry["recieving_ip"] = dir_struct.globalChunkMapping.slaves_state[i]["ip"]
                        updated_entry["recieving_port"] = dir_struct.globalChunkMapping.slaves_state[i]["port"]
                        updated_entry["data"]=[]
                        updated_entry["data"].append(chunk_details)
                        updated_obj.append(updated_entry)
                    break      
            
        try:
            with open('chunk_servers.json') as f:
                old_chunk_servers = json.load(f)
        except IOError:
            print("Unable to locate chunkservers in the database!")
        
        for i in range(len(old_chunk_servers)):
            if old_chunk_servers[i]["ip"] == target_ip and old_chunk_servers[i]["port"] == target_port:
                del old_chunk_servers[i]
                break
        
        f = open('chunk_servers.json','w')
        jsonString = json.dumps(old_chunk_servers)
        f.write(jsonString)
        f.close()
        
        for u_server in updated_obj:
            balance_data = {}
            balance_data["ip"] = self_ip
            balance_data["port"] = self_port
            balance_data["agent"] = "master"
            balance_data["action"] = "balance_load"
            balance_data["data"] = {}
            balance_data["data"]["target_ip"] = u_server["recieving_ip"]
            balance_data["data"]["target_port"] = u_server["recieving_port"]
            balance_data["data"]["balancing_chunk_handles"] = u_server["data"]
            try:    
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((u_server["seeding_ip"], u_server["seeding_port"]))
                s.sendall(str(balance_data).encode())
                s.close()
            except:
                print("Connection refused by the slave server: "+u_server["seeding_ip"]+":"+str(u_server["seeding_port"]))
        
                