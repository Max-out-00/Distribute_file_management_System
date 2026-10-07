import socket

PORT_NUMBER = 9999

client = socket.socket();
client.connect(('localhost',PORT_NUMBER))
print(client.recv(PORT_NUMBER).decode())

client.close()
