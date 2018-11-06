from threading import Thread, BoundedSemaphore
import socket
import sys
import os
import json
import configparser
import hashlib

OK_REPORT = False
CHECKSUM_OBJ = []
container = BoundedSemaphore()

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
            
    def generate_chunkSum(self, file_name):
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
        if start_byte % BLOCKSIZE == 0:
            start_block-=1
        if end_byte % BLOCKSIZE ==0:
            end_block-=1
        file = open(chunk_handle+".dat","rb")
        for i in range(len(CHECKSUM_OBJ)):
            if CHECKSUM_OBJ[i]["chunk_handle"] == chunk_handle:
                handle_index = i
                break
        file.seek(start_block * BLOCKSIZE)
        while start_block <= end_block:
            bytes_read = file.read(BLOCKSIZE)
            result = hashlib.sha1(bytes_read)
            block_hash = result.hexdigest()
            if CHECKSUM_OBJ[handle_index]["check_sums"][start_block] != block_hash:
                file.close()
                return False
            else:
                continue
            start_block+=1
        file.close()
        return True
    
    def run(self):
        global CHECKSUM_OBJ
        global OK_REPORT
        data = self.sock.recv(RCVCHUNKSIZE)
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
                        if json_data["response_status"]=="orphaned_chunks":
                            orp_chunks_list = json_data["data"]
                            for orp_chunks in orp_chunks_list:
                                os.remove(orp_chunks+".dat")
                                container.acquire()
                                try:
                                    with open('chunkServerState.json') as f:
                                        chunks_details = json.load(f)
                                except IOError:
                                    resp = "Unable to retrieve chunks details!"
                                container.release()
                                remove_entries = []
                                for i in range(len(chunks_details)):
                                    if chunks_details[i]["chunk_handle"] == orp_chunks:
                                        remove_entries.append(i)
                                        break
                                todel_checksums = []
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
                            print("Removed orphaned chunk")
                        elif json_data["response_status"] == "OK":
                            OK_REPORT=True
                            print("Report is OK")
                    elif json_data["action"] == "seedChunkToSlave":
                        print("Seeding chunk to the slave")
                        # seed chunk to slave
                        
            elif json_data["agent"]=="client":
                if json_data["action"] == "request/read":
                    while not OK_REPORT:
                        pass
                    
                    for raw_data in json_data["data"]:
                        file_name = raw_data["handle"]+".dat"
                        start_byte = raw_data["start_byte"]
                        end_byte = raw_data["end_byte"]
                        if self.check_integrity(start_byte, end_byte, raw_data["handle"]):
                            print("Integrity is maintained, about to send data")
                            fp = open(file_name, 'rb')
                            fp.seek(start_byte-1)
                            read_buffer = fp.read(end_byte-start_byte+1)
                            fp.close()
                            #send this read_buffer over the socket connection to client
                        else:
                            print("Integrity not maintained, about to notify master")
                            notify_master = {}
                            notify_master["agent"]="chunk_server"
                            notify_master["ip"]=self.ip
                            notify_master["port"]=self.port
                            notify_master["data"]=[]
                            notify_master["action"]="manipulated_chunk_found"
                            notify_master["data"].append(raw_data["handle"])
                            self.sock.close()
                            print("sending manipulated chunk data to master: "+self.master_ip+str(self.master_port))
                            self.send_json_data(self.master_ip, self.master_port, notify_master)
        except ValueError:
            #also have to recieve data from other slave servers
            print("size of the data is: "+str(len(data)))
            flags = data[0:100]
            data = data[100:]
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
            chunk_type = headers[2]
            chunk_name = headers[1]+".dat"
            chunk_file = open(chunk_name, "wb")
            chunk_file.write(data)
            chunk_file.close()
            if action == "store":
                with open('chunkServerState.json') as f:
                    chunks_details = json.load(f)
                fresh_chunk = {}
                fresh_chunk["handle"] = headers[1]
                if chunk_type == "pri":
                    fresh_chunk["type"] = "primary"
                elif chunk_type == "sec":
                    fresh_chunk["type"] = "secondary"
                chunks_details.append(fresh_chunk)
                k=open('chunkServerState.json', 'w')
                jsonString = json.dumps(chunks_details)
                k.write(jsonString)
                k.close()
                checks = self.generate_checkSum(chunk_name)
                c_obj = {}
                c_obj["chunk_handle"] = headers[1]
                c_obj["check_sums"] = checks
                container.acquire()
                CHECKSUM_OBJ.append(c_obj)
                container.release()
            

def generate_chunkSum(file_name):
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
        c_obj["check_sums"] = generate_chunkSum(file_name)
        CHECKSUM_OBJ.append(c_obj)
except IOError:
    print("Unable to load chunks details")


tcpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
tcpsock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
self_IP_PORT = str(sys.argv[1]).split(':')
tcpsock.bind((self_IP_PORT[0], int(self_IP_PORT[1])))

while True:
    tcpsock.listen(1000)
    print ("Waiting for incoming connections...")
    (conn, (ip,port)) = tcpsock.accept()
    listenthread = ListenClientMaster(conn, self_IP_PORT[0], int(self_IP_PORT[1]))
    listenthread.daemon = True
    listenthread.start()

