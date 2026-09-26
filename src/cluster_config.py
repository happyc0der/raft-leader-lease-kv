"""Cluster membership helpers shared by the Raft node and the client.

The cluster is a comma-separated list of ``host:port`` addresses; a node's id is
its index in that list. It can be given with ``--cluster`` or the
``RAFT_CLUSTER`` environment variable. The default is the original five-node
localhost cluster on ports 4040-4044.
"""

import os

DEFAULT_CLUSTER = "localhost:4040,localhost:4041,localhost:4042,localhost:4043,localhost:4044"


def parse_cluster(value=None):
    """Return the list of node addresses from ``value``, ``$RAFT_CLUSTER`` or the default."""
    value = value or os.environ.get("RAFT_CLUSTER") or DEFAULT_CLUSTER
    addresses = [a.strip() for a in value.split(",") if a.strip()]
    for address in addresses:
        host, sep, port = address.rpartition(":")
        if not sep or not host or not port.isdigit():
            raise ValueError(f"Invalid node address {address!r}; expected host:port")
    return addresses
