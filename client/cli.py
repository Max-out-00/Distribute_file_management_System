import argparse
import sys
from client.upload import upload_file
from client.download import download_file, read_range
from client.pipeline import upload_file_parallel
from client.delete import delete_file
from client.upload import call
from shared import constants as C

def main():
    ap = argparse.ArgumentParser(prog="dfs", description="Distributed file system client")
    sub = ap.add_subparsers(dest="command", required=True)

    up = sub.add_parser("upload", help="upload a file")
    up.add_argument("path")
    up.add_argument("--parallel", type=int, default=1,
                    help="send chunks with N worker threads (bounded buffer)")

    down = sub.add_parser("download", help="download a file by id")
    down.add_argument("file_id")
    down.add_argument("output")

    rm = sub.add_parser("delete", help="delete a file by id")
    rm.add_argument("file_id")

    rd = sub.add_parser("read", help="read a byte range of a file (direct access)")
    rd.add_argument("file_id")
    rd.add_argument("offset", type=int)
    rd.add_argument("length", type=int)
    rd.add_argument("-o", "--output", help="write to this file instead of stdout")

    st = sub.add_parser("node-stats", help="show chunk count and cache statistics of a data node")
    st.add_argument("host")
    st.add_argument("port", type=int)

    sub.add_parser("nodes", help="show nodes and whether they are alive")

    args = ap.parse_args()

    if args.command == "upload":
        file_id = (upload_file_parallel(args.path, workers=args.parallel)
                   if args.parallel > 1 else upload_file(args.path))
        print("Uploaded. file_id:", file_id)      # the user needs this id to download later
    elif args.command == "download":
        download_file(args.file_id, args.output)
        print("Saved to", args.output)
    elif args.command == "delete":
        reply = delete_file(args.file_id)
        print("Deleted", reply["deleted_chunks"], "chunk(s);",
              reply["unreachable_replicas"], "replica(s) unreachable")
    elif args.command == "read":
        data = read_range(args.file_id, args.offset, args.length)
        if args.output:
            with open(args.output, "wb") as out:
                out.write(data)
            print("Wrote", len(data), "bytes to", args.output)
        else:
            sys.stdout.buffer.write(data)
    elif args.command == "node-stats":
        resp, _ = call(args.host, args.port, {"cmd": "STATS"})
        print("chunks:", resp["chunks"], " used_bytes:", resp["used_bytes"])
        print("cache:", resp["cache"])
    elif args.command == "nodes":
        resp, _ = call(C.METADATA_HOST, C.METADATA_PORT, {"type": "LIST_NODES"})
        for n in resp["nodes"]:
            print(n["node_id"], "alive" if n["alive"] else "DEAD")

if __name__ == "__main__":
    try:
        main()
    except (RuntimeError, ValueError, OSError) as error:   # friendly message, no traceback
        print("Error:", error, file=sys.stderr)
        sys.exit(1)