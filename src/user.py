import argparse
import sys
import time

import grpc
import raft_pb2
import raft_pb2_grpc
from cluster_config import parse_cluster, DEFAULT_CLUSTER

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# SET waits for the leader to replicate to its followers, so allow a generous deadline.
REQUEST_TIMEOUT = 20


def send_request(address, request_string):
    with grpc.insecure_channel(address) as channel:
        stub = raft_pb2_grpc.raft_serviceStub(channel)
        message = raft_pb2.ServeClientArgs(Request=request_string)
        return stub.serveClient(message, timeout=REQUEST_TIMEOUT)


class RaftClient:
    """Sends GET/SET requests to the cluster, following leader hints from the nodes."""

    def __init__(self, addresses):
        self.addresses = addresses
        self.leader_id = 0

    def request_once(self, request_string):
        """One attempt against the node we currently believe is the leader."""
        print("🚀 Sending request to node with ID-" + str(self.leader_id))
        try:
            response = send_request(self.addresses[self.leader_id], request_string)
        except Exception:
            print("❌ Error: Could not connect to the server. Please try again.")
            self.leader_id = (self.leader_id + 1) % len(self.addresses)
            return None
        if response.LeaderID.isdigit() and int(response.LeaderID) < len(self.addresses):
            self.leader_id = int(response.LeaderID)
        if response.Success:
            print("✅ Success: Request has been processed successfully.")
            print("Response: " + response.Data)
        else:
            print("❌ Error: Request could not be processed by server.", response.Data)
            if not response.LeaderID.isdigit():
                self.leader_id = (self.leader_id + 1) % len(self.addresses)
        return response

    def request(self, request_string, retries=1, delay=1.0):
        for attempt in range(retries):
            response = self.request_once(request_string)
            if response is not None and response.Success:
                return response
            if attempt + 1 < retries:
                time.sleep(delay)
        return None


def build_request(command, key, value=None):
    for token in (key, value):
        if token is not None and (not token or any(c.isspace() for c in token)):
            raise ValueError("Keys and values must be non-empty and contain no whitespace.")
    command = command.upper()
    if command == "GET":
        return "GET " + key
    if command == "SET":
        if value is None:
            raise ValueError("SET needs a key and a value.")
        return "SET " + key + " " + value
    raise ValueError("Unknown command " + command + " (expected GET or SET).")


def interactive(client):
    print("👋 Welcome to the Raft Consensus Algorithm Menu!")
    while True:
        command = input("Please enter a command (GET/SET):").strip().upper()
        try:
            if command == "GET":
                request_string = build_request("GET", input("Please enter the key:").strip())
            elif command == "SET":
                key = input("Please enter the key:").strip()
                value = input("Please enter the value:").strip()
                request_string = build_request("SET", key, value)
            else:
                print("Invalid command. Please try again.")
                continue
        except ValueError as error:
            print("Invalid input:", error)
            continue
        client.request_once(request_string)


def main():
    parser = argparse.ArgumentParser(
        description="Client for the Raft key-value cluster. With no command it starts an interactive menu."
    )
    parser.add_argument(
        "--cluster",
        help=f"comma-separated host:port of every node (default: $RAFT_CLUSTER or {DEFAULT_CLUSTER})",
    )
    parser.add_argument(
        "--retries", type=int, default=10, help="attempts for a one-shot command (default: 10)"
    )
    parser.add_argument("command", nargs="*", help="one-shot command: GET <key> | SET <key> <value>")
    args = parser.parse_args()

    client = RaftClient(parse_cluster(args.cluster))
    if not args.command:
        interactive(client)
        return
    try:
        request_string = build_request(*args.command[:3]) if len(args.command) in (2, 3) else None
    except ValueError as error:
        parser.error(str(error))
    if request_string is None:
        parser.error("expected GET <key> or SET <key> <value>")
    response = client.request(request_string, retries=args.retries)
    sys.exit(0 if response is not None else 1)


if __name__ == "__main__":
    try:
        main()
    except (KeyboardInterrupt, EOFError):
        print()
