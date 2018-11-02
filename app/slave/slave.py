from threading import Thread
import socket
import sys
import os
import json
import configparser

config = configparser.RawConfigParser()
config.read('slave.properties')
CHUNKSIZE = int(config.get('Slave_Data','CHUNKSIZE'))
RCVCHUNKSIZE = int(config.get('Slave_Data','CHUNK_RECSIZE'))
DELIMITER = str(config.get('Slave_Data','DELIMITER'))

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
            s.connect((ip, port))
            s.sendall(str(data).encode())
            s.close()
        except:
            print("Connection refused by the master: "+ip+":"+str(port))
            
    def run(self):
        data = self.sock.recv(RCVCHUNKSIZE)
        try:
            str_data = data.decode().replace("\'", "\"")
            json_data = json.loads(str_data)
            if json_data["agent"]=="master":
                if json_data["ip"]==self.master_ip and json_data["port"]==self.master_port:
                    create_response = {}
                    create_response["agent"] = "chunk_server"
                    create_response["ip"] = self.ip
                    create_response["port"] = self.port
                    if json_data["action"]=="periodic_report":
                        create_response["action"]="report_ack"
                        create_response["data"] = []
                        create_response["extras"] = (os.statvfs('/').f_bsize) * (os.statvfs('/').f_bavail)
                        try:
                            with open('chunkServerState.json') as f:
                                chunks_details = json.load(f)
                            create_response["data"] = chunks_details
                        except IOError:
                            resp = "Unable to retrieve chunks details!"
                            create_response["data"].append(resp)
                    self.sock.close()
                    self.send_json_data(self.master_ip, self.master_port, create_response)
            elif json_data["agent"]=="client":
                print(json_data)
        except:
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
            chunk_name = headers[1]+".dat"
            chunk_file = open(chunk_name, "wb")
            chunk_file.write(data)


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