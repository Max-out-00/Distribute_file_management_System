import socket
from shared import constants

class Handle:

    def get():



class Server:
    def __init__(self):
        self.server = socket.socket()
        self.server.bind(( constants.HOST, constants.METADATA_PORT))
        self.server.listen(5)
        print(f"Listening on port {port}")
        self.storage = {} 

    def start(self);
        while True:
            connection, client_address = self.server.accept()
            try:
                self.handle(connection)
            finally:
                connection.close()
        
    def handle(self , request):
        # Read from the connection until EOF
        request = connection.recv(1024).decode()  # Read request from the client

        # Write back the result of the hash operation
        response = self.process(request)
        connection.sendall(response.encode())  # Send response back to the client
        

