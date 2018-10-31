from threading import Thread
import socket
import sys
import json
import configparser

config = configparser.RawConfigParser()
config.read('slave.properties')
CHUNKSIZE = int(config.get('Slave_Data','CHUNKSIZE'))
RCVCHUNKSIZE = int(config.get('Slave_Data','CHUNK_RECSIZE'))
DELIMITER = str(config.get('Slave_Data','DELIMITER'))

class ListenClientMaster(Thread):
    def __init__(self,ip,port,sock):
        Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.sock = sock
        print (" New thread started for "+ip+":"+str(port))
    
    def run(self):
        data = self.sock.recv(RCVCHUNKSIZE)
        try:
            str_data = data.decode().replace("\'", "\"")
            json_data = json.loads(str_data)
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
tcpsock.bind((sys.argv[1], int(sys.argv[2])))

while True:
    tcpsock.listen(1000)
    print ("Waiting for incoming connections...")
    (conn, (ip,port)) = tcpsock.accept()
    print ('Got connection from ', (ip,port))
    listenthread = ListenClientMaster(ip,port,conn)
    listenthread.daemon = True
    listenthread.start()