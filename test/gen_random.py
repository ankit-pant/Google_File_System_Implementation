import random

file = open("mytext.txt","ab")

for x in range(1000000):
    data = str(random.randint(1, 10000))+"\n"
    file.write(data.encode())
    
file.close()
