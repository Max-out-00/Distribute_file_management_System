import os, hashlib
from client.chunker import split_file, reassemble_file
from client.upload import call

HOST, PORT, CHUNK = "127.0.0.1", 6001, 4096
SRC, OUT = "sample.bin", "sample_out.bin"

with open(SRC, "wb") as f:
    f.write(os.urandom(10_000))                  # make a test file

# upload: split, send each chunk
for index, data, checksum in split_file(SRC, CHUNK):
    resp, _ = call(HOST, PORT, {"type": "PUT_CHUNK",
                                "chunk_id": f"demo_{index}",
                                "checksum": checksum}, data)
    print("put", index, resp["status"])

# download: fetch each chunk, verify, reassemble
got = []
for index in range(3):
    resp, data = call(HOST, PORT, {"type": "GET_CHUNK", "chunk_id": f"demo_{index}"})
    assert hashlib.sha256(data).hexdigest() == resp["checksum"]
    got.append((index, data))
reassemble_file(got, OUT)

same = hashlib.sha256(open(SRC, "rb").read()).digest() == hashlib.sha256(open(OUT, "rb").read()).digest()
print("identical:", same)