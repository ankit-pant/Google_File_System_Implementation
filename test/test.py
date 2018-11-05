f = open("test.txt","rb")
start_b = 3
end_b = 6
BLOCK_SIZE = 3
f.seek(start_b-1)
print(f.read(end_b-start_b+1))
