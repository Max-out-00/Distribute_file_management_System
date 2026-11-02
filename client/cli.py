import argparse
from client.upload import upload_file
from client.download import download_file
from client.upload import call
from shared import constants as C

def main():
    ap = argparse.ArgumentParser(prog="dfs", description="Distributed file system client")
    sub = ap.add_subparsers(dest="command", required=True)

    up = sub.add_parser("upload", help="upload a file")
    up.add_argument("path")

    down = sub.add_parser("download", help="download a file by id")
    down.add_argument("file_id")
    down.add_argument("output")

    sub.add_parser("nodes", help="show nodes and whether they are alive")

    args = ap.parse_args()

    if args.command == "upload":
        file_id = upload_file(args.path)
        print("Uploaded. file_id:", file_id)      # the user needs this id to download later
    elif args.command == "download":
        download_file(args.file_id, args.output)
        print("Saved to", args.output)
    elif args.command == "nodes":
        resp, _ = call(C.METADATA_HOST, C.METADATA_PORT, {"type": "LIST_NODES"})
        for n in resp["nodes"]:
            print(n["node_id"], "alive" if n["alive"] else "DEAD")

if __name__ == "__main__":
    main()