CHUNKSIZE=64*1024*1024

file = open("EOT.mkv", "rb")
try:
    bytes_read = file.read(CHUNKSIZE)
    c_num=0
    while bytes_read:
        file_name=str(c_num)+".dat"
        f=open(file_name, "wb")
        f.write(bytes_read)
        f.close()
        c_num+=1
        bytes_read = file.read(CHUNKSIZE)
finally:
    file.close()    

print("Chunks created successfully")
new_file = open("NEW_EOT.mkv","wb")
for i in range(16):
    file_name = str(i)+".dat"
    dat_data = open(file_name, "rb")
    print("reading "+file_name)
    dat_bytes = dat_data.read()
    new_file.write(dat_bytes)
    dat_data.close()
new_file.close()
