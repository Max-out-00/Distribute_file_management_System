"""Process manager for the whole cluster: one command instead of six terminals.

    python scripts/cluster.py start [--nodes 5] [--meta-port 9999] [--base-port 5001]
    python scripts/cluster.py status
    python scripts/cluster.py kill-node node3       # simulate a CRASH (SIGKILL: no goodbye)
    python scripts/cluster.py stop-node node3       # GRACEFUL stop (SIGTERM: node says LEAVE_NODE)
    python scripts/cluster.py restart-node node3    # start it again (it re-registers by heartbeat)
    python scripts/cluster.py stop

OS concepts used: process creation (subprocess.Popen = fork+exec / CreateProcess), detached
processes (own session), PID bookkeeping, signals (os.kill), liveness checks, log redirection
to files (stdout/stderr file descriptors). Everything lives under ./run (state, logs, storage).
"""
import argparse
import json
import os
import signal
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

from client.upload import call  # noqa: E402

RUN = os.path.join(ROOT, "run")
STATE = os.path.join(RUN, "cluster.json")


def load():
    if not os.path.exists(STATE):
        return None
    with open(STATE) as f:
        return json.load(f)


def save(state):
    os.makedirs(RUN, exist_ok=True)
    with open(STATE, "w") as f:
        json.dump(state, f, indent=2)


def alive(pid):
    """Is a process with this PID running? (os.kill(pid, 0) would KILL it on Windows.)"""
    if os.name == "nt":
        out = subprocess.run(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True)
        return str(pid) in out.stdout
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    try:                               # a zombie still answers signal 0; treat it as dead
        with open(f"/proc/{pid}/stat") as f:
            return f.read().rsplit(")", 1)[1].split()[0] != "Z"
    except OSError:
        return True


def spawn(args, log_name):
    os.makedirs(os.path.join(RUN, "logs"), exist_ok=True)
    log = open(os.path.join(RUN, "logs", log_name), "a")
    kwargs = {"cwd": ROOT, "stdout": log, "stderr": subprocess.STDOUT, "stdin": subprocess.DEVNULL}
    if os.name == "nt":
        kwargs["creationflags"] = 0x00000008 | 0x00000200      # DETACHED_PROCESS | NEW_PROCESS_GROUP
    else:
        kwargs["start_new_session"] = True                     # survive this launcher / terminal
    proc = subprocess.Popen([sys.executable, "-u", "-m", *args], **kwargs)
    log.close()
    return proc.pid


def node_args(node_id, port, meta_port):
    return ["data_node.init", "--node-id", node_id, "--bind-host", "127.0.0.1",
            "--advertise-host", "127.0.0.1", "--port", str(port),
            "--storage-dir", os.path.join(RUN, "storage", node_id),
            "--meta-host", "127.0.0.1", "--meta-port", str(meta_port)]


def registered(meta_port):
    try:
        resp, _ = call("127.0.0.1", meta_port, {"type": "LIST_NODES"}, timeout=2)
        return {n["node_id"]: n["alive"] for n in resp["nodes"]}
    except (OSError, ValueError, KeyError):
        return {}


def cmd_start(a):
    state = load()
    if state and any(alive(p["pid"]) for p in [state["meta"], *state["nodes"].values()]):
        sys.exit("A cluster is already running. Run 'stop' first.")
    meta = {"pid": spawn(["metadata_server.server", "--bind-host", "127.0.0.1",
                          "--port", str(a.meta_port), "--db", os.path.join(RUN, "metadata.db")],
                         "metadata.log"), "port": a.meta_port}
    nodes = {}
    for i in range(1, a.nodes + 1):
        node_id, port = f"node{i}", a.base_port + i - 1
        nodes[node_id] = {"pid": spawn(node_args(node_id, port, a.meta_port), f"{node_id}.log"),
                          "port": port}
    save({"meta": meta, "nodes": nodes})
    deadline = time.time() + 20
    while time.time() < deadline:
        known = registered(a.meta_port)
        if all(known.get(n) for n in nodes):
            break
        time.sleep(0.5)
    print(f"Started metadata server (pid {meta['pid']}, port {a.meta_port}) and {a.nodes} nodes.")
    print("Use the client in this shell with:")
    print(f"  bash/zsh:    export METADATA_PORT={a.meta_port}")
    print(f"  PowerShell:  $env:METADATA_PORT={a.meta_port}")
    cmd_status(a)


def cmd_status(a):
    state = load()
    if not state:
        sys.exit("No cluster state found. Run 'start' first.")
    known = registered(state["meta"]["port"])
    print(f"{'PROCESS':10} {'PID':>7} {'PORT':>6} {'OS PROCESS':11} METADATA VIEW")
    print(f"{'metadata':10} {state['meta']['pid']:>7} {state['meta']['port']:>6} "
          f"{'running' if alive(state['meta']['pid']) else 'DEAD':11} -")
    for node_id, info in state["nodes"].items():
        view = {True: "ALIVE", False: "DEAD"}.get(known.get(node_id), "unknown")
        print(f"{node_id:10} {info['pid']:>7} {info['port']:>6} "
              f"{'running' if alive(info['pid']) else 'stopped':11} {view}")


def terminate(pid, hard=False):
    """SIGTERM = polite request (graceful shutdown handler runs); SIGKILL = crash, cannot be caught.
    Windows has no SIGKILL: every os.kill there is TerminateProcess (a hard stop)."""
    if alive(pid):
        try:
            os.kill(pid, getattr(signal, "SIGKILL", signal.SIGTERM) if hard else signal.SIGTERM)
        except OSError:
            pass


def cmd_stop(a):
    state = load()
    if not state:
        sys.exit("No cluster state found.")
    for info in [*state["nodes"].values(), state["meta"]]:
        terminate(info["pid"])                       # graceful first
    deadline = time.time() + 8
    while time.time() < deadline and any(alive(i["pid"]) for i in [*state["nodes"].values(), state["meta"]]):
        time.sleep(0.2)
    for info in [*state["nodes"].values(), state["meta"]]:
        terminate(info["pid"], hard=True)            # anything still running is killed
    os.remove(STATE)
    print("Cluster stopped.")


def cmd_kill(a):
    state = load()
    info = state["nodes"].get(a.node_id) if state else None
    if not info:
        sys.exit(f"Unknown node {a.node_id}")
    terminate(info["pid"], hard=True)
    print(f"Crashed {a.node_id} (pid {info['pid']}). The metadata server only notices after the "
          "failure timeout (about 10 s).")


def cmd_stop_node(a):
    state = load()
    info = state["nodes"].get(a.node_id) if state else None
    if not info:
        sys.exit(f"Unknown node {a.node_id}")
    terminate(info["pid"])
    print(f"Asked {a.node_id} (pid {info['pid']}) to shut down gracefully; it tells the metadata "
          "server, so recovery starts within about 2 s.")


def cmd_restart(a):
    state = load()
    info = state["nodes"].get(a.node_id) if state else None
    if not info:
        sys.exit(f"Unknown node {a.node_id}")
    if alive(info["pid"]):
        sys.exit(f"{a.node_id} is already running")
    info["pid"] = spawn(node_args(a.node_id, info["port"], state["meta"]["port"]), f"{a.node_id}.log")
    save(state)
    print(f"Restarted {a.node_id} (pid {info['pid']}).")


def main():
    p = argparse.ArgumentParser(description="Cluster process manager")
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("start")
    s.add_argument("--nodes", type=int, default=5)
    s.add_argument("--meta-port", type=int, default=9999)
    s.add_argument("--base-port", type=int, default=5001)
    s.set_defaults(fn=cmd_start)
    sub.add_parser("status").set_defaults(fn=cmd_status)
    sub.add_parser("stop").set_defaults(fn=cmd_stop)
    k = sub.add_parser("kill-node"); k.add_argument("node_id"); k.set_defaults(fn=cmd_kill)
    g = sub.add_parser("stop-node"); g.add_argument("node_id"); g.set_defaults(fn=cmd_stop_node)
    r = sub.add_parser("restart-node"); r.add_argument("node_id"); r.set_defaults(fn=cmd_restart)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
