"""End-to-end smoke test / demo for the Raft cluster.

Starts N nodes on localhost as separate processes, then:
  1. waits for a leader to be elected,
  2. writes keys (one through a follower, which forwards to the leader),
  3. reads them back from every node,
  4. kills the leader and waits for a new one to be elected,
  5. checks the data survived and writes a new key,
  6. restarts the old leader and checks it catches up on the missed write.

Every node process is stopped on exit. Node output and the persisted
logs_node_<id>/ directories are kept in the printed run directory.

Usage (from the repository root, with the virtual environment's Python):
    python scripts/demo_cluster.py [--nodes 5] [--base-port 4040]
"""

import argparse
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import grpc  # noqa: E402
import raft_pb2  # noqa: E402
import raft_pb2_grpc  # noqa: E402

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")


class Cluster:
    def __init__(self, nodes, base_port, run_dir):
        self.addresses = [f"localhost:{base_port + i}" for i in range(nodes)]
        self.run_dir = run_dir
        self.procs = {}
        self.start_time = time.monotonic()

    def log(self, message):
        print(f"[{time.monotonic() - self.start_time:6.1f}s] {message}", flush=True)

    def start(self, node_id):
        out = open(self.run_dir / f"node_{node_id}.out", "a", encoding="utf-8")
        env = dict(os.environ, PYTHONUTF8="1")
        self.procs[node_id] = subprocess.Popen(
            [
                sys.executable, "-u", str(SRC / "node.py"),
                "--id", str(node_id),
                "--cluster", ",".join(self.addresses),
                "--data-dir", str(self.run_dir),
            ],
            stdout=out, stderr=subprocess.STDOUT, stdin=subprocess.DEVNULL, env=env,
        )
        self.log(f"started node {node_id} on {self.addresses[node_id]} (pid {self.procs[node_id].pid})")

    def kill(self, node_id):
        proc = self.procs.pop(node_id)
        proc.kill()
        proc.wait(timeout=10)
        self.log(f"killed node {node_id} (simulated crash)")

    def stop_all(self):
        for node_id in list(self.procs):
            proc = self.procs.pop(node_id)
            proc.kill()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                pass

    def ask(self, node_id, request, timeout=20):
        """Send a client request to one specific node; None if it is unreachable."""
        try:
            with grpc.insecure_channel(self.addresses[node_id]) as channel:
                stub = raft_pb2_grpc.raft_serviceStub(channel)
                return stub.serveClient(raft_pb2.ServeClientArgs(Request=request), timeout=timeout)
        except grpc.RpcError:
            return None

    def ask_with_retry(self, node_id, request, attempts=5):
        """Like ask(), but retry a few times as a real client would (e.g. during an election)."""
        for attempt in range(1, attempts + 1):
            response = self.ask(node_id, request)
            if response is not None and response.Success:
                return response
            reason = response.Data if response is not None else "node unreachable"
            self.log(f"  {request!r} attempt {attempt} failed: {reason}")
            time.sleep(1)
        return None

    def wait_for_leader(self, timeout, exclude=None):
        """Poll the live nodes until one answers a GET as the leader holding a lease."""
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for node_id in list(self.procs):
                if node_id == exclude:
                    continue
                response = self.ask(node_id, "GET __probe__", timeout=3)
                if response is not None and response.Success and response.LeaderID == str(node_id):
                    return node_id
            time.sleep(0.5)
        return None

    def log_file(self, node_id):
        path = self.run_dir / f"logs_node_{node_id}" / "logs.txt"
        return path.read_text(encoding="utf-8").splitlines() if path.exists() else []


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--nodes", type=int, default=5)
    parser.add_argument("--base-port", type=int, default=4040)
    parser.add_argument("--run-dir", help="where node output and data go (default: a new temp directory)")
    args = parser.parse_args()

    run_dir = Path(args.run_dir) if args.run_dir else Path(tempfile.mkdtemp(prefix="raft-demo-"))
    run_dir.mkdir(parents=True, exist_ok=True)
    cluster = Cluster(args.nodes, args.base_port, run_dir)
    checks = []

    def check(name, ok):
        checks.append((name, ok))
        cluster.log(("PASS " if ok else "FAIL ") + name)
        return ok

    print(f"Run directory: {run_dir}")
    try:
        for node_id in range(args.nodes):
            cluster.start(node_id)

        cluster.log("waiting for a leader (election timeouts are 5-11 s)...")
        leader = cluster.wait_for_leader(timeout=60)
        if not check("leader elected", leader is not None):
            return 1
        cluster.log(f"node {leader} is the leader")

        follower = next(i for i in cluster.procs if i != leader)
        response = cluster.ask_with_retry(follower, "SET course DSCD")
        check(f"SET course DSCD via follower {follower} (forwarded to leader)",
              response is not None and response.Success)
        response = cluster.ask_with_retry(leader, "SET algorithm raft")
        check(f"SET algorithm raft via leader {leader}", response is not None and response.Success)

        values = {}
        for node_id in sorted(cluster.procs):
            response = cluster.ask(node_id, "GET algorithm")
            values[node_id] = response.Data if response is not None and response.Success else None
        cluster.log(f"GET algorithm from every node -> {values}")
        check("every node returns algorithm=raft", all(v == "raft" for v in values.values()))

        time.sleep(2)  # one more heartbeat so followers learn the new commit index
        for node_id in sorted(cluster.procs):
            cluster.log(f"node {node_id} log: {cluster.log_file(node_id)}")

        old_leader = leader
        cluster.kill(old_leader)
        cluster.log("waiting for re-election (followers wait for the 8 s lease to lapse, then 5-11 s)...")
        t0 = time.monotonic()
        leader = cluster.wait_for_leader(timeout=90, exclude=old_leader)
        if not check("new leader elected after leader crash", leader is not None):
            return 1
        cluster.log(f"node {leader} is the new leader ({time.monotonic() - t0:.1f}s after the crash)")

        response = cluster.ask(leader, "GET course")
        check("committed data survives leader failure (GET course -> DSCD)",
              response is not None and response.Success and response.Data == "DSCD")
        response = cluster.ask_with_retry(leader, "SET after_failover yes")
        check("SET after_failover yes on the new leader", response is not None and response.Success)

        cluster.start(old_leader)
        deadline = time.monotonic() + 30
        caught_up = False
        while time.monotonic() < deadline and not caught_up:
            caught_up = "SET after_failover yes" in " ".join(cluster.log_file(old_leader))
            time.sleep(1)
        check(f"restarted node {old_leader} caught up on the write it missed", caught_up)
        cluster.log(f"node {old_leader} log after restart: {cluster.log_file(old_leader)}")
    finally:
        cluster.stop_all()
        cluster.log("all node processes stopped")

    failed = [name for name, ok in checks if not ok]
    print()
    print(f"{len(checks) - len(failed)}/{len(checks)} checks passed.")
    print(f"Node output and data: {run_dir}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
