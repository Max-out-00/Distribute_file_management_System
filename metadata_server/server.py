import socket
from shared import constants

class Server:
    def __init__(self):
        self.server = socket.socket()
        self.server.bind(( constants.HOST, constants.METADATA_PORT))
        self.server.listen(5)
        print(f"Listening on port {port}")
        self.storage = {} 

    def start(self):
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
        

    def process(self, request):
        # Split the request into command, key, and value
        parts = request.split()
        command = parts[0].upper()

        # Handle GET and SET commands
        if command == 'GET':
            key = parts[1]
            return self.storage.get(key, 'Key not found')

        elif command == 'SET':
            key = parts[1]
            value = parts[2]
            self.storage[key] = value
            return 'OK'

        return 'Unknown command'

# Start the server on port 9999 (METADATA_PORT)
server = CloudHashServer(constants.METADATA_PORT)
server.start()
