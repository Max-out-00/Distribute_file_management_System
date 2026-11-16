from shared import constants
from .upload import call


def delete_file(file_id):
    if not isinstance(file_id, str) or not file_id.strip():
        raise ValueError("file_id must be a non-empty string")
    host = getattr(constants, "METADATA_HOST", constants.HOST)
    response, _ = call(
        host,
        constants.METADATA_PORT,
        {"type": "DELETE_FILE", "file_id": file_id},
    )
    if not isinstance(response, dict):
        raise RuntimeError("metadata server returned an invalid delete response")
    if response.get("status") != "ok":
        raise RuntimeError(response.get("error", "metadata server could not delete file"))
    return response
