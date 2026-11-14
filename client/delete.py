from client.upload import call
from shared import constants


def delete_file(file_id):
    """Ask the metadata server to delete a file; returns its reply dict."""
    reply, _ = call(constants.METADATA_HOST, constants.METADATA_PORT,
                    {"type": "DELETE_FILE", "file_id": file_id}, timeout=30)
    if reply.get("status") != "ok":
        raise RuntimeError(reply.get("error", "delete failed"))
    return reply
