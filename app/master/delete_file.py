import dir_struct
import socket
import pickle

def send_json_data(ip, port, data):
    try:    
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((ip, port))
        s.sendall(str(data).encode())
        s.close()
    except:
        print("Connection refused by the master: "+ip+":"+str(port))

def DeleteFile(file_name, metaData, self_ip, self_port):
    handle = metaData.fileNamespace.retrieveHandle(file_name)
    if handle == None:
        print("File does not exist")
        return False
    else:
        todel_metadata = []
        todel_fileChunks = []
        for i in range(len(metaData.metadata)):
            if metaData.metadata[i]["fileHashName"] == handle:
                todel_metadata.append(i)
                for chunk in metaData.metadata[i]["chunkDetails"]:                
                    todel_fileChunks.append(chunk["chunk_handle"])
                break
        
        todel_chunksDB = []
        for j in range(len(metaData.chunksDB)):
            if metaData.chunksDB[j] in todel_fileChunks:
                todel_chunksDB.append(j)
        
        for meta_idx in reversed(todel_metadata):
            del metaData.metadata[meta_idx]
            
        for db_idx in reversed(todel_chunksDB):        
            del metaData.chunksDB[db_idx]
        
        todel_chunks_mapping = []
        for i in range(len(dir_struct.globalChunkMapping.chunks_mapping)):
            chunk_entry = dir_struct.globalChunkMapping.chunks_mapping[i]
            if chunk_entry["chunk_handle"] in todel_fileChunks:
                todel_chunks_mapping.append(i)
        
        for map_idx in reversed(todel_chunks_mapping):
            del dir_struct.globalChunkMapping.chunks_mapping[map_idx]
            
        for j in range(len(dir_struct.globalChunkMapping.slaves_state)):
            slave_state = dir_struct.globalChunkMapping.slaves_state[j]
            del_chunk = []
            todel_chunk_idx = []
            for k in range(len(slave_state["chunks"])):
                if slave_state["chunks"][k] in todel_fileChunks:
                    del_chunk.append(slave_state["chunks"][k])
                    todel_chunk_idx.append(k)
            for del_cidx in reversed(todel_chunk_idx):
                del dir_struct.globalChunkMapping.slaves_state[j]["chunks"][del_cidx]
                
        metaData.fileNamespace.removeEntry(file_name)
        master_state = open('masterState','ab')
        pickle.dump(metaData, master_state)
        master_state.close()
        return True