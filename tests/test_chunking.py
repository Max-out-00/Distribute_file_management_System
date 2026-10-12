import os, hashlib
from client.chunker import split_file, reassemble_file

def sha_of(path):
    return hashlib.sha256(open(path, "rb").read()).hexdigest()

def test_split_and_reassemble(tmp_path):
    src = tmp_path / "in.bin"
    src.write_bytes(os.urandom(10_000))           # 10,000 bytes
    chunks = [(i, d) for i, d, _ in split_file(str(src), 4096)]
    assert len(chunks) == 3                       # 4096 + 4096 + 1808
    assert len(chunks[-1][1]) == 10_000 - 2 * 4096
    out = tmp_path / "out.bin"
    reassemble_file(chunks, str(out))
    assert sha_of(src) == sha_of(out)

def test_empty_file(tmp_path):
    src = tmp_path / "empty.bin"
    src.write_bytes(b"")
    assert list(split_file(str(src), 4096)) == []