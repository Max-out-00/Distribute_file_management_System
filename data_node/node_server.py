import socket
form shared import constants

class Client:

    @classmethod
    def get(cls, key):
        # Send a GET request to the server
        request = f"GET {key}"
        return cls.request(request)

    @classmethod
    def set(cls, key, value):
        # Send a SET request to the server
        request = f"SET {key} {value}"
        return cls.request(request)

    @classmethod
    def request(cls, request_string):
        # Create a new connection for each operation
        with socket.socket() as client_socket:
            client_socket.connect((cls.costants.HOST, cls.constants.METADATA_PORT))
            client_socket.sendall(request_string.encode())  # Send request

            # Close write operation after sending the request
            client_socket.shutdown(socket.SHUT_WR)

            # Read the response until EOF
            response = client_socket.recv(1024).decode()  # Read response from the server
            return response

# Test the client
print(Client.set('prez', 'obama'))
print(Client.get('prez'))
print(Client.get('vp'))




