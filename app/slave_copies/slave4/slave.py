from threading import Thread, BoundedSemaphore
import socket
import sys
import os
from time import sleep
import json
import configparser
import hashlib
import shutil

OK_REPORT = False
CHECKSUM_OBJ = []
container = BoundedSemaphore()
chunks_state = []

config = configparser.RawConfigParser()
config.read('slave.properties')
CHUNKSIZE = int(config.get('Slave_Data','CHUNKSIZE'))
RCVCHUNKSIZE = int(config.get('Slave_Data','CHUNK_RECSIZE'))
DELIMITER = str(config.get('Slave_Data','DELIMITER'))
BLOCKSIZE = int(config.get('Slave_Data','BLOCK_SIZE'))

class ListenClientMaster(Thread):
    def __init__(self,sock, self_ip, self_port):
        Thread.__init__(self)
        self.sock = sock
        self.master_ip = str(config.get('Slave_Data','MASTER_IP'))
        self.master_port = int(config.get('Slave_Data','MASTER_PORT'))
        self.ip = self_ip
        self.port = self_port
    
    def send_json_data(self, ip, port, data):
        try:    
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            print("Connecting to: "+ip+str(port))
            s.connect((ip, port))
            s.sendall(str(data).encode())
            s.close()
        except:
            print("Connection refused by the master: "+ip+":"+str(port))
            
    def generate_checkSum(self, file_name):
        file = open(file_name, "rb")
        check_sum = []
        bytes_read = file.read(BLOCKSIZE)
        while bytes_read:
            result = hashlib.sha1(bytes_read)
            block_hash = result.hexdigest()
            check_sum.append(block_hash)
            bytes_read = file.read(BLOCKSIZE)
        return check_sum
    
    def check_integrity(self, start_byte, end_byte, chunk_handle):
        start_block = int(start_byte/BLOCKSIZE)
        end_block = int(end_byte/BLOCKSIZE)
        if start_byte % BLOCKSIZE == 0 and start_byte!=0:
            start_block-=1
        if end_byte % BLOCKSIZE ==0:
            end_block-=1
        file = open(chunk_handle+".dat","rb")
        for i in range(len(CHECKSUM_OBJ)):
            if CHECKSUM_OBJ[i]["chunk_handle"] == chunk_handle:
                handle_index = i
                break
        print("Start block is: "+str(start_block)+" end block is: "+str(end_block))
        file.seek(start_block * BLOCKSIZE)
        print("size of the checsum_obj is: ",len(CHECKSUM_OBJ[handle_index]["check_sums"]))
        print("hash is: ",chunk_handle)
        while start_block <= end_block:
            bytes_read = file.read(BLOCKSIZE)
            result = hashlib.sha1(bytes_read)
            block_hash = result.hexdigest()
            #print("Comparing for block: "+str(start_block))
            #print("comparing curr: "+block_hash+" and "+CHECKSUM_OBJ[handle_index]["check_sums"][start_block])
            c_hash = CHECKSUM_OBJ[handle_index]["check_sums"][start_block]
            #print("Found c hashed")
            if c_hash != block_hash:
                file.close()                
                return False
            else:
                start_block+=1
                continue
        file.close()
        return True
    
    def check_send_data(self, start_byte, end_byte, handle, clients_ip, clients_port):
        container.acquire()
        int_flag = self.check_integrity(start_byte, end_byte, handle)
        container.release()
        if int_flag:
            print("Integrity is maintained, about to send data")
            file_name = handle+".dat"
            fp = open(file_name, 'rb')
            if start_byte!=0:
                fp.seek(start_byte-1)
            else:
                fp.seek(start_byte)
            read_buffer = fp.read(end_byte-start_byte+1)
            fp.close()
            #send this read_buffer over the socket connection to client
            try:    
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                print("Connecting to: "+ip+":"+str(port))
                s.connect((clients_ip, clients_port))
                s.sendall(read_buffer)
                s.close()
            except:
                print("Connection refused by the client: "+clients_ip+":"+str(clients_port))
        else:
            print("Integrity not maintained, about to notify master")
            for c in range(len(chunks_state)):
                if chunks_state[c]["handle"] == handle:
                    chunks_state[c]["isValid"] = False
                    break
            notify_master = {}
            notify_master["agent"]="chunk_server"
            notify_master["ip"]=self.ip
            notify_master["port"]=self.port
            notify_master["data"]=[]
            notify_master["action"]="manipulated_chunk_found"
            notify_master["data"].append(handle)
            self.sock.close()
            print("sending manipulated chunk data to master: "+self.master_ip+str(self.master_port))
            self.send_json_data(self.master_ip, self.master_port, notify_master)
            self.sock.close()
            while not chunks_state[c]["isValid"]:
                print("Waiting for the chunk to be recieved from another slave server")
                sleep(1)
            self.check_send_data(start_byte, end_byte, handle, clients_ip, clients_port)
    
    
    def replicate_chunks(self, handle, chunk_type, ip, port):
        container.acquire()
        int_flag = self.check_integrity(0, os.path.getsize(handle+".dat")-1, handle)
        container.release()
        if int_flag:
            if chunk_type == "":
                for chunk in chunks_state:
                    if chunk["handle"] == handle:
                        print("Integrity is maintained, about to send data to the slave")
                        fp = open(handle+".dat", "rb")
                        read_buff = fp.read(CHUNKSIZE)
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        print("Connecting to: "+ip+str(port))
                        s.connect((ip, port))
                        if chunk["type"] == "primary":
                            c_type = "pri"
                        else:
                            c_type = "sec"
                        for new_chunk in chunks_state:
                            if new_chunk["handle"] == handle:
                                valid_data_len = new_chunk["valid_data_len"]
                                break
                        headers = DELIMITER+"store"+DELIMITER+handle+DELIMITER+c_type+DELIMITER+valid_data_len+DELIMITER
                        if len(headers)<200:
                            headers = headers.ljust(200)
                        chunk_data = headers.encode()+read_buff
                        #create data 
                        s.sendall(chunk_data)
                        s.close()
                        break
                os.remove(handle+".dat")
                container.acquire()
                try:
                    with open('chunkServerState.json') as f:
                        chunks_details = json.load(f)
                except IOError:
                    resp = "Unable to retrieve chunks details!"
                    print(resp)
                container.release()
                remove_entries = []
                for i in range(len(chunks_details)):
                    if chunks_details[i]["chunk_handle"] == handle:
                        remove_entries.append(i)
                        break
                todel_checksums = []
                # DELETING ENTRY FROM CHECKSUMS
                container.acquire()
                for k in range(len(CHECKSUM_OBJ)):
                    if CHECKSUM_OBJ[k]["chunk_handle"] == handle:
                        todel_checksums.append(i)
                        break
                container.release()
                
                for todel in reversed(remove_entries):
                    del chunks_details[todel]
                container.acquire()
                for tod in reversed(todel_checksums):
                    del CHECKSUM_OBJ[tod]
                container.release()
                container.acquire()
                k=open('chunkServerState.json', 'w')
                jsonString = json.dumps(chunks_details)
                k.write(jsonString)
                k.close()
                container.release()
            else:
                print("Integrity is maintained, about to send data to the slave")
                fp = open(handle+".dat", "rb")
                read_buff = fp.read(CHUNKSIZE)
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                print("Connecting to: "+ip+str(port))
                s.connect((ip, port))
                if chunk_type == "primary":
                    c_type = "pri"
                else:
                    c_type = "sec"
                for new_chunk in chunks_state:
                    if new_chunk["handle"] == handle:
                        valid_data_len = new_chunk["valid_data_len"]
                        break
                headers = DELIMITER+"store"+DELIMITER+handle+DELIMITER+c_type+DELIMITER+valid_data_len+DELIMITER
                if len(headers)<200:
                    headers = headers.ljust(200)
                chunk_data = headers.encode()+read_buff
                #create data 
                s.sendall(chunk_data)
                s.close()
        else:
            print("Integrity not maintained, about to notify master")
            for c in range(len(chunks_state)):
                if chunks_state[c]["handle"] == handle:
                    chunks_state[c]["isValid"] = False
                    break
            notify_master = {}
            notify_master["agent"]="chunk_server"
            notify_master["ip"]=self.ip
            notify_master["port"]=self.port
            notify_master["data"]=[]
            notify_master["action"]="manipulated_chunk_found"
            notify_master["data"].append(handle)
            self.sock.close()
            print("sending manipulated chunk data to master: "+self.master_ip+str(self.master_port))
            self.send_json_data(self.master_ip, self.master_port, notify_master)
            self.sock.close()
            while not chunks_state[c]["isValid"]:
                print("Waiting for the chunk to be recieved from another slave server")
                sleep(1)
            self.replicate_chunks(handle,chunk_type,ip,port)
        
                
    def run(self):
        global CHECKSUM_OBJ
        global OK_REPORT
        data = []
        total_len = RCVCHUNKSIZE
        while total_len:
            data_rcv = self.sock.recv(RCVCHUNKSIZE)
            if not data_rcv:
                break
            data.append(data_rcv)
            total_len = total_len-len(data_rcv)
        data = b''.join(data)
        try:
            str_data = data.decode().replace("\'", "\"")
            json_data = json.loads(str_data)
            if json_data["agent"]=="master":
                if json_data["ip"]==self.master_ip and json_data["port"]==self.master_port:
                    if json_data["action"]=="periodic_report":
                        create_response = {}
                        create_response["agent"] = "chunk_server"
                        create_response["ip"] = self.ip
                        create_response["port"] = self.port
                        create_response["action"]="report_ack"
                        create_response["data"] = []
                        create_response["extras"] = (os.statvfs('/').f_bsize) * (os.statvfs('/').f_bavail)
                        container.acquire()
                        try:
                            with open('chunkServerState.json') as f:
                                chunks_details = json.load(f)
                            create_response["data"] = chunks_details
                        except IOError:
                            resp = "Unable to retrieve chunks details!"
                            create_response["data"].append(resp)
                        container.release()
                        self.sock.close()
                        self.send_json_data(self.master_ip, self.master_port, create_response)
                    elif json_data["action"]=="report/response":
                        #   Garbage Collection (removing entry from chunkServerState.json and deleting file)
                        print(json_data["data"])
                        if json_data["response_status"]=="orphaned_chunks":
                            orp_chunks_list = json_data["data"]
                            remove_entries = []
                            todel_checksums = []
                            for orp_chunks in orp_chunks_list:
                                os.remove(orp_chunks+".dat")
                                container.acquire()
                                try:
                                    with open('chunkServerState.json') as f:
                                        chunks_details = json.load(f)
                                except IOError:
                                    resp = "Unable to retrieve chunks details!"
                                container.release()
                                for i in range(len(chunks_details)):
                                    if chunks_details[i]["chunk_handle"] == orp_chunks:
                                        remove_entries.append(i)
                                        break
                                # DELETING ENTRY FROM CHECKSUMS
                                container.acquire()
                                for k in range(len(CHECKSUM_OBJ)):
                                    if CHECKSUM_OBJ[k]["chunk_handle"] == orp_chunks:
                                        todel_checksums.append(i)
                                        break
                                container.release()
                                
                            for todel in reversed(remove_entries):
                                del chunks_details[todel]
                            container.acquire()
                            for tod in reversed(todel_checksums):
                                del CHECKSUM_OBJ[tod]
                            k=open('chunkServerState.json', 'w')
                            jsonString = json.dumps(chunks_details)
                            k.write(jsonString)
                            k.close()
                            container.release()
                            OK_REPORT=True
                            print("new json string is: ",jsonString)
                            print("Removed orphaned chunk")
                        elif json_data["response_status"] == "OK":
                            OK_REPORT=True
                            print("Report is OK")
                    elif json_data["action"] == "seedChunkToSlave":
                        # seed chunk to slave
                        print("Seeding chunk to the slave")
                        inf_ip = json_data["data"]["infected_slave_ip"]
                        inf_port = json_data["data"]["infected_slave_port"]
                        inf_chunk_handle = json_data["data"]["infected_chunk_handle"]
                        chunk_name = inf_chunk_handle+".dat"
                        container.acquire()
                        inte_flag = self.check_integrity(0, CHUNKSIZE-1, json_data["data"]["infected_chunk_handle"])
                        container.release()
                        if inte_flag:
                            print("Integrity is maintained, about to send data to the slave")
                            fp = open(chunk_name, "rb")
                            read_buff = fp.read(CHUNKSIZE)
                            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                            print("Connecting to: "+ip+str(port))
                            s.connect((inf_ip, inf_port))
                            headers = DELIMITER+"resto"+DELIMITER+inf_chunk_handle+DELIMITER+"NuN"+DELIMITER+"00000000"+DELIMITER
                            if len(headers)<200:
                                headers = headers.ljust(200)
                            chunk_data = headers.encode()+read_buff
                            #create data 
                            s.sendall(chunk_data)
                            s.close()
                        else:
                            print("Integrity not maintained, about to notify master")
                            notify_master = {}
                            notify_master["agent"]="chunk_server"
                            notify_master["ip"]=self.ip
                            notify_master["port"]=self.port
                            notify_master["data"]=[]
                            notify_master["action"]="manipulated_chunk_found"
                            notify_master["data"].append(json_data["data"]["infected_chunk_handle"])
                            self.sock.close()
                            print("sending manipulated chunk data to master: "+self.master_ip+str(self.master_port))
                            self.send_json_data(self.master_ip, self.master_port, notify_master)
                    
                    elif json_data["action"] == "balance_load":
                        print(json_data)
                        threads = []
                        target_ip = json_data["data"]["target_ip"]
                        target_port = json_data["data"]["target_port"]
                        for s_chunk in json_data["data"]["balancing_chunk_handles"]:
                            incoming_chunk_type = s_chunk["type"]
                            incoming_chunk_handle = s_chunk["handle"]
                            t = Thread(target=self.replicate_chunks, args=(incoming_chunk_handle, incoming_chunk_type, target_ip, target_port))
                            threads.append(t)
                            t.start()
                        for t in threads:
                            t.join()
                    elif json_data["action"] == "copy/snapshot":
                        src = os.getcwd()
                        subdir = "snapshot"
                        timestamp = json_data["data"]["timestamp"]
                        dest = os.path.join(src, subdir, timestamp)
                        print(chunks_state)
                        try:
                            os.mkdir(os.path.join(src, subdir))
                        except FileExistsError:
                            print("Snapshot folder already exist")
                        
                        try:
                            os.mkdir(os.path.join(src, subdir, timestamp))
                        except FileExistsError:
                            print(timestamp, "folder alredy exist")
                            
                        for handle in json_data["data"]["chunks_list"]:
                            for new_chunk in chunks_state:
                                if new_chunk["handle"] == handle:
                                    valid_data_len = new_chunk["valid_data_len"]
                                    break
                            full_file_name = os.path.join(src, handle+".dat")
                            if int(valid_data_len) == 64*1024*1024:
                                if (os.path.isfile(full_file_name)):
                                    print("Copying",full_file_name, dest)
                                    shutil.copy(full_file_name, dest)
                            else:
                                new_chunk_copy = os.path.join(dest, handle+".dat")
                                k = open(handle+".dat","rb")
                                chunk_data = k.read(int(valid_data_len))
                                k.close()
                                k = open(new_chunk_copy,"wb")
                                k.write(chunk_data)
                                k.close()
                    elif json_data["action"] == "send_snap_chunks":
                        chunk_name = json_data["data"]["handle"]
                        snap_ip = json_data["data"]["ip"]
                        snap_port = json_data["data"]["port"]
                        print("Sending chunk to slave with: ",snap_ip,snap_port)
                        for new_chunk in chunks_state:
                            if new_chunk["handle"] == json_data["data"]["handle"]:
                                valid_data_len = new_chunk["valid_data_len"]
                                break
                        k = open(chunk_name+".dat","rb")
                        read_buff = k.read(int(valid_data_len))
                        k.close()
                        headers = DELIMITER+"snapc"+DELIMITER+json_data["data"]["handle"]+DELIMITER+json_data["data"]["timestamp"]+DELIMITER
                        if len(headers)<200:
                            headers = headers.ljust(200)
                        chunk_data = headers.encode()+read_buff
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect((snap_ip, snap_port))
                        s.sendall(chunk_data)
                        s.close()
                    elif json_data["action"] == "restore_snapshot":
                        print("requesting to retrieve snapshot data")
                        folderName = json_data["data"]["folder"]
                        here = os.getcwd()
                        target = os.path.join(here, "snapshot", folderName)
                        meta_file = open(os.path.join(target, "metaInfo"), "rb")
                        meta_data = meta_file.read()
                        meta_file.close()
                        headers = DELIMITER+"meta_file"+DELIMITER+json_data["data"]["directory"]+DELIMITER
                        if len(headers)<250:
                            headers = headers.ljust(250)
                        meta_data = headers.encode()+meta_data
                        self.sock.close()
                        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                        s.connect((json_data["ip"], json_data["port"]))
                        s.sendall(meta_data)
                        s.close()
            elif json_data["agent"]=="client":
                if json_data["action"] == "request/read":
                    while not OK_REPORT:
                        pass
                    print("Incoming read req data is: ",json_data)
                    for raw_data in json_data["data"]:
                        start_byte = raw_data["start_byte"]
                        end_byte = raw_data["end_byte"]
                        self.check_send_data(start_byte, end_byte, raw_data["handle"], json_data["ip"], json_data["port"])
                            
                                
        except ValueError:
            #also have to recieve data from other slave servers
            print("size of the data is: "+str(len(data)))
            flags = data[0:200]
            flags=flags.strip()
            data = data[200:]
            print("size of the data after stripping is: "+str(len(data)))
            flags = flags.decode()
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
            
            if action == "store":
                chunk_type = headers[2]
                chunk_name = headers[1]+".dat"
                valid_data_len = headers[3]
                chunk_file = open(chunk_name, "wb")
                chunk_file.write(data)
                chunk_file.close()
                container.acquire()
                with open('chunkServerState.json') as f:
                    chunks_details = json.load(f)
                container.release()
                fresh_chunk = {}
                fresh_chunk["chunk_handle"] = headers[1]
                if chunk_type == "pri":
                    fresh_chunk["type"] = "primary"
                elif chunk_type == "sec":
                    fresh_chunk["type"] = "secondary"
                fresh_chunk["valid_data_len"] = valid_data_len
                chunks_details.append(fresh_chunk)
                container.acquire()
                k=open('chunkServerState.json', 'w')
                jsonString = json.dumps(chunks_details)
                k.write(jsonString)
                k.close()
                container.release()
                checks = self.generate_checkSum(chunk_name)
                c_obj = {}
                c_obj["chunk_handle"] = headers[1]
                c_obj["check_sums"] = checks
                container.acquire()
                CHECKSUM_OBJ.append(c_obj)
                container.release()
                c_state = {}
                c_state["handle"] = headers[1]
                c_state["isValid"] = True
                c_state["type"] = fresh_chunk["type"]
                c_state["valid_data_len"] = valid_data_len
                chunks_state.append(c_state)
            elif action=="resto":
                chunk_type = headers[2]
                chunk_name = headers[1]+".dat"
                valid_data_len = headers[3]
                chunk_file = open(chunk_name, "wb")
                chunk_file.write(data)
                chunk_file.close()
                print("Data retrieved by another slave")
                for c in range(len(chunks_state)):
                    if chunks_state[c]["handle"] == headers[1]:
                        chunks_state[c]["isValid"] = True
                        break
                self.sock.close()
                notify_master = {}
                notify_master["agent"]="chunk_server"
                notify_master["ip"]=self.ip
                notify_master["port"]=self.port
                notify_master["action"]="recieved_correct_chunk"
                notify_master["data"]={}
                notify_master["handle"] = headers[1]
                self.send_json_data(self.master_ip, self.master_port, notify_master)
            elif action=="snaps":
                here = os.getcwd()
                timestamp = headers[2]
                subdir = "snapshot"
                filename = headers[1]
                filepath = os.path.join(here, subdir, timestamp, filename)
                
                try:
                    os.mkdir(os.path.join(here, subdir))
                except FileExistsError:
                    print("Snapshot folder already exist")
                
                try:
                    os.mkdir(os.path.join(here, subdir, timestamp))
                except FileExistsError:
                    print(timestamp, "folder alredy exist")
                    
                try:
                    f = open(filepath, 'wb')
                    f.write(data)
                    f.close()
                except IOError:
                    print("Wrong path provided")
            elif action=="snapc":
                print("Chunk recieved for snapshot from another slave")
                here = os.getcwd()
                subdir = "snapshot"
                filename = headers[1]+".dat"
                timestamp = headers[2]
                filepath = os.path.join(here, subdir, timestamp, filename)
                
                try:
                    os.mkdir(os.path.join(here, subdir))
                except FileExistsError:
                    print("Snapshot folder already exist")
                
                try:
                    os.mkdir(os.path.join(here, subdir, timestamp))
                except FileExistsError:
                    print(timestamp,"folder already exist")
                
                try:
                    f = open(filepath, 'wb')
                    f.write(data)
                    f.close()
                except IOError:
                    print("Wrong path provided")
def generate_checkSum(file_name):
    file = open(file_name, "rb")
    check_sum = []
    bytes_read = file.read(BLOCKSIZE)
    while bytes_read:
        result = hashlib.sha1(bytes_read)
        block_hash = result.hexdigest()
        check_sum.append(block_hash)
        bytes_read = file.read(BLOCKSIZE)
    return check_sum


try:
    with open('chunkServerState.json') as f:
        chunks_details = json.load(f)
    for chunk in chunks_details:
        file_name = chunk["chunk_handle"]+".dat"
        c_obj = {}
        c_obj["chunk_handle"] = chunk["chunk_handle"]
        c_obj["check_sums"] = generate_checkSum(file_name)
        CHECKSUM_OBJ.append(c_obj)
        c_state = {}
        c_state["handle"] = chunk["chunk_handle"]
        c_state["type"] = chunk["type"]
        c_state["isValid"] = True
        c_state["valid_data_len"] = chunk["valid_data_len"]
        chunks_state.append(c_state)
except IOError:
    print("Unable to load chunks details")

self_IP_PORT = str(sys.argv[1]).split(':')
masterIp = str(config.get('Slave_Data','MASTER_IP'))
masterPort = int(config.get('Slave_Data','MASTER_PORT'))

try:    
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    bootdata = {}
    bootdata["agent"] = "chunk_server"
    bootdata["action"] = "new_chunk_server"
    bootdata["data"] = {}
    bootdata["ip"] = self_IP_PORT[0]
    bootdata["port"] = int(self_IP_PORT[1])
    bootdata["data"]["disk_free_space"] = (os.statvfs('/').f_bsize) * (os.statvfs('/').f_bavail)
    print("Connecting to: "+masterIp+str(masterPort))
    s.connect((masterIp, masterPort))
    s.sendall(str(bootdata).encode())
    s.close()
except:
    print("Connection refused by the master: "+masterIp+":"+str(masterPort))


tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
tcpsock.bind((self_IP_PORT[0], int(self_IP_PORT[1])))

while True:
    tcpsock.listen(1000)
    print ("Waiting for incoming connections...")
    (conn, (ip,port)) = tcpsock.accept()
    listenthread = ListenClientMaster(conn, self_IP_PORT[0], int(self_IP_PORT[1]))
    listenthread.daemon = True
    listenthread.start()

