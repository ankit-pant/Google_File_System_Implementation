from dir_struct import SnapObj
import copy
import pickle
import configparser
import socket
import dir_struct
import time

config = configparser.RawConfigParser()
config.read('master.properties')
DELIMITER = config.get('Master_Data','DELIMITER')

def send_json_data(ip, port, data):
    try:    
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        s.sendall(str(data).encode())
        s.close()
    except:
        print("Connection refused by the master: "+ip+":"+str(port))

def find_nearest(self_ip, ips):
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

def takeSnap(metaData, dir_path, self_ip, self_port):
    dir_arr = dir_path.split('/')
    null_idx = []
    i=0
    for dir in dir_arr:
        if dir=='':
            null_idx.append(i)
        i+=1
    k=len(null_idx)-1
    while k>=0:
        del dir_arr[null_idx[k]]
        k-=1
    snapshot_obj = SnapObj()
    snapshot_obj.nameSpace = metaData.fileNamespace.retrieveNode(metaData.fileNamespace, dir_arr)
    file_handles = []
    metaData.fileNamespace.retrieveAllFileHandles(snapshot_obj.nameSpace, file_handles)
    
    for tfile in file_handles:
        for file in metaData.metadata:
            if file["fileHashName"] == tfile:
                snapshot_obj.metadata.append(copy.deepcopy(file))
                for chunks in file["chunkDetails"]:
                    snapshot_obj.chunksDB.append(chunks["chunk_handle"])
                break
    
    dump_snap_data = pickle.dumps(snapshot_obj)
    
    slave_ips = []
    for slaves in metaData.slaves_list:
        slave_ips.append(slaves["ip"])
    
    r_ip = []
    for x in slave_ips:
        y=x.split('.')
        r_ip.append(y)
    
    snap_ip = find_nearest(self_ip,r_ip)
    for slaves in metaData.slaves_list:
        if slaves["ip"] == snap_ip:
            snap_port = slaves["port"]
            break
        
    now = str(time.time())
    headers = DELIMITER+"snaps"+DELIMITER+"metaInfo"+DELIMITER+now+DELIMITER
    if len(headers)<200:
        headers = headers.ljust(200)
    snap_data = headers.encode() + dump_snap_data
    map_str = pickle.dumps(snapshot_obj.metadata)
    new_headers = DELIMITER+"snaps"+DELIMITER+"mapInfo"+DELIMITER+now+DELIMITER
    if len(new_headers)<200:
        new_headers = new_headers.ljust(200)
    map_data = new_headers.encode() + map_str
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((snap_ip, snap_port))
        s.sendall(snap_data)
        s.close()
    except:
        print("Unable to send snapshot data to the slave")
    
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((snap_ip, snap_port))
        s.sendall(map_data)
        s.close()
    except:
        print("Unable to send snapshot data to the slave")
    
    list_chunks = []
    for slave in dir_struct.globalChunkMapping.slaves_state:
        if slave["ip"] == snap_ip and slave["port"] == snap_port:
            list_chunks = copy.deepcopy(slave["chunks"])
            break
        
    del_chunks = []
    for i in range(len(list_chunks)):
        if list_chunks[i] not in snapshot_obj.chunksDB:
            del_chunks.append(i)
            
    for idx in reversed(del_chunks):
        del list_chunks[idx]
        
    send_data = {}
    send_data["agent"] = "master"
    send_data["ip"] = self_ip
    send_data["port"] = self_port
    send_data["action"] = "copy/snapshot"
    send_data["data"] = {}
    send_data["data"]["chunks_list"] = list_chunks
    send_data["data"]["timestamp"] = now
    send_json_data(snap_ip, snap_port, send_data)
    
    remaining_chunks = [item for item in snapshot_obj.chunksDB if item not in list_chunks]
    
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
    
    
    send_snap_data = {}
    send_snap_data["agent"] = "master"
    send_snap_data["action"] = "send_snap_chunks"
    send_snap_data["ip"] = self_ip
    send_snap_data["port"] = self_port
    send_snap_data["data"] = {}
    for chunks in remaining_chunks:
        for map_chunk in dir_struct.globalChunkMapping.chunks_mapping:
            if map_chunk["chunk_handle"]==chunks:
                s_ip = map_chunk["servers"][0]["ip"]
                s_port = map_chunk["servers"][0]["port"]
                break
        send_snap_data["data"]["handle"] = chunks
        send_snap_data["data"]["ip"] = snap_ip
        send_snap_data["data"]["port"] = snap_port
        send_snap_data["data"]["timestamp"] = now
        send_json_data(s_ip, s_port, send_snap_data)
    
    snap_data = {}
    snap_data["path"] = dir_path
    snap_data["time"] = now
    snap_data["slave_details"] = {}
    snap_data["slave_details"]["ip"] = snap_ip
    snap_data["slave_details"]["port"] = snap_port
    metaData.snapshotRecord.append(snap_data)
                
    