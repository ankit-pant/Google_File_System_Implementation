import socket

TCP_IP = '10.2.129.89'
TCP_PORT = 12363
BUFFER_SIZE = 1024000

s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.connect((TCP_IP, TCP_PORT))
string = "Hello"
s.sendall(string.encode())

print('Successfully get the file')
s.close()
print('connection closed')
