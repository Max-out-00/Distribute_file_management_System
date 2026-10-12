import hashlib

def split_file(path, chunk_size):
    """Yield (index, data, checksum) one chunk at a time."""
    with open(path, "rb") as f:
        index = 0
        while True:
            data = f.read(chunk_size)
            if not data:           # empty bytes = end of file
                break
            yield index, data, hashlib.sha256(data).hexdigest()
            index += 1

def reassemble_file(chunks, out_path):
    """chunks: iterable of (index, data). Writes them in index order."""
    with open(out_path, "wb") as out:
        for _, data in sorted(chunks, key=lambda c: c[0]):
            out.write(data)