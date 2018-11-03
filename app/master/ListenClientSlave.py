from threading import Thread
import json
import socket
import configparser
import dir_struct
import copy

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
    
    def run(self):
        data = self.sock.recv(Json_rcv_limit)
        data = data.decode()
        data = data.replace("\'", "\"")
        j = json.loads(data)
        print("Data recieved::::::")
        if j["agent"]=="client":
            print("Data recieved by client")
            client_ip=j["ip"]
            client_port=j["port"]
            if j["action"]=="read":
                print("input cmnd is read")
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
                                    for cmap in dir_struct.globalChunkMapping.chunks_mapping:
                                        if cmap["chunk_handle"] == chunk["chunk_handle"]:
                                            handle_details["chunk_servers"] = copy.deepcopy(cmap["chunk_servers"])
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
                for specific_chunk in j["data"]:
                    sc_handle = specific_chunk["chunk_handle"]
                    server_id = {}
                    server_id["ip"]=j["ip"]
                    server_id["port"]=j["port"]
                    server_id["type"]=specific_chunk["type"]
                    found = False
                    for i in range(len(dir_struct.globalChunkMapping.chunks_mapping)):
                        if dir_struct.globalChunkMapping.chunks_mapping[i]["chunk_handle"] == sc_handle:
                            if server_id not in dir_struct.globalChunkMapping.chunks_mapping[i]["chunk_servers"]:
                                dir_struct.globalChunkMapping.chunks_mapping[i]["chunk_servers"].append(server_id)
                            found = True
                            break
                    if found==False:
                        new_entry = {}
                        new_entry["chunk_handle"] = sc_handle
                        new_entry["chunk_servers"] = []
                        new_entry["chunk_servers"].append(server_id)
                        dir_struct.globalChunkMapping.chunks_mapping.append(new_entry)
                
                found = False
                for i in range(len(dir_struct.globalChunkMapping.slaves_state)):
                    if dir_struct.globalChunkMapping.slaves_state[i]["ip"] == j["ip"] and dir_struct.globalChunkMapping.slaves_state[i]["port"] == j["port"]:
                        dir_struct.globalChunkMapping.slaves_state[i]["disk_free_space"] = j["extras"]
                        found = True
                        break
                if found == False:
                    new_entry = {}
                    new_entry["ip"]=j["ip"]
                    new_entry["port"]=j["port"]
                    new_entry["disk_free_space"]=j["extras"]
                    dir_struct.globalChunkMapping.slaves_state.append(new_entry)
