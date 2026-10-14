
import socket

from shared import constants
from shared.message import send_msg, recv_msg

from metadata_server.queries import (
    register_file,
    get_file,
    update_file_status,
    register_chunk,
    get_chunks_for_file,
    register_node,
    update_node_heartbeat,
    get_live_nodes,
    register_chunk_location,
    get_nodes_for_chunk,
)

from metadata_server.placement import get_chunk_placement


class Server:
    def __init__(self):
        self.server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server.bind((constants.HOST, constants.METADATA_PORT))
        self.server.listen(5)

        print(
            f"Metadata server listening on "
            f"{constants.HOST}:{constants.METADATA_PORT}"
        )

    def start(self):
        try:
            while True:
                connection, client_address = self.server.accept()

                with connection:
                    print(f"Connection from {client_address}")

                    try:
                        request, payload = recv_msg(connection)

                        if payload:
                            response = {
                                "ok": False,
                                "error": "Metadata commands do not accept payloads.",
                            }
                        else:
                            response = self.process(request)

                    except Exception as error:
                        response = {
                            "ok": False,
                            "error": str(error),
                        }

                    try:
                        send_msg(connection, response)
                    except (ConnectionError, OSError):
                        print("Could not send response to client.")

        except KeyboardInterrupt:
            print("\nStopping metadata server.")

        finally:
            self.server.close()

    def process(self, request):
        if not isinstance(request, dict):
            return {"ok": False, "error": "Request must be a dictionary."}

        command = str(request.get("command", "")).upper()

        try:
            if command == "REGISTER_FILE":
                register_file(
                    request["file_id"],
                    request["file_name"],
                    request["size"],
                    request.get("status", "UPLOADING"),
                )
                return {"ok": True, "message": "File registered."}

            elif command == "GET_FILE":
                result = get_file(request["file_id"])
                return {"ok": True, "file": result}

            elif command == "UPDATE_FILE_STATUS":
                updated = update_file_status(
                    request["file_id"],
                    request["status"],
                )
                return {"ok": updated, "message": (
                    "File status updated." if updated else "File not found."
                )}

            elif command == "REGISTER_CHUNK":
                register_chunk(
                    request["chunk_id"],
                    request["file_id"],
                    request["chunk_index"],
                    request["size"],
                    request["checksum"],
                )
                return {"ok": True, "message": "Chunk registered."}

            elif command == "GET_CHUNKS":
                chunks = get_chunks_for_file(request["file_id"])
                return {"ok": True, "chunks": chunks}

            elif command == "REGISTER_NODE":
                register_node(
                    request["node_id"],
                    request["address"],
                    request["disk_used"],
                    request["disk_total"],
                )
                return {"ok": True, "message": "Node registered."}

            elif command == "HEARTBEAT":
                updated = update_node_heartbeat(
                    request["node_id"],
                    request["disk_used"],
                    request["disk_total"],
                )
                return {
                    "ok": updated,
                    "message": (
                        "Heartbeat recorded."
                        if updated
                        else "Node not found; register it first."
                    ),
                }

            elif command == "GET_LIVE_NODES":
                return {"ok": True, "nodes": get_live_nodes()}

            elif command == "GET_CHUNK_PLACEMENT":
                nodes = get_chunk_placement(request["chunk_index"])
                return {"ok": True, "nodes": nodes}

            elif command == "RECORD_CHUNK_LOCATION":
                register_chunk_location(
                    request["chunk_id"],
                    request["node_id"],
                    request.get("status", "AVAILABLE"),
                )
                return {"ok": True, "message": "Chunk location recorded."}

            elif command == "GET_CHUNK_LOCATIONS":
                nodes = get_nodes_for_chunk(request["chunk_id"])
                return {"ok": True, "nodes": nodes}

            else:
                return {
                    "ok": False,
                    "error": f"Unknown command: {command}",
                }

        except KeyError as error:
            return {
                "ok": False,
                "error": f"Missing required field: {error.args[0]}",
            }

        except Exception as error:
            return {"ok": False, "error": str(error)}


if __name__ == "__main__":
    Server().start()
