"""Regression test: a leader repairs a follower that is far behind without recursing.

Log repair backs sentLength off one entry per rejected AppendEntries. It used to
do that by replicate_log and recieve_log_ack calling each other, so a follower
~500 entries behind hit Python's recursion limit. This test wires a leader and a
follower Node together in one process (call_peer is replaced by a direct call)
and checks that a single replicate_log call brings a follower 1500 entries
behind fully up to date.

Run from the repository root:
    python -m unittest discover -s tests -v
"""

import os
import shutil
import sys
import tempfile
import threading
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "src"))

import node  # noqa: E402
from metadata import metadump  # noqa: E402

GAP = 1500  # entries the follower is missing; well past the default recursion limit of 1000


def make_node(data_dir, node_id, role, entries):
    """Build a Node from on-disk state without starting its server or main loop."""
    node_dir = os.path.join(data_dir, "logs_node_" + str(node_id))
    os.makedirs(node_dir)
    with open(os.path.join(node_dir, "logs.txt"), "w", encoding="utf-8") as f:
        f.writelines(f"{command} {term}\n" for command, term in entries)
    open(os.path.join(node_dir, "dump.txt"), "w").close()
    metadata_path = os.path.join(node_dir, "metadata.txt")
    metadump(metadata_path).write_blank_metadata_file()

    n = node.Node()
    n.node_id = node_id
    n.current_term = 1
    n.max_lease_duration = 8
    n.lease_duration = 0
    n.lease_timer_alive = False
    n.has_lease = True
    n.commit_length = 0
    n.current_role = role
    n.current_leader = 0
    n.votes_recieved = set()
    n.sent_length = {}
    n.acked_length = {}
    n.election_timer = threading.Timer(3600, lambda: None)
    n.election_timer_alive = False
    n.log = node.Log(os.path.join(node_dir, "logs.txt"))
    n.dump = node.Log(os.path.join(node_dir, "dump.txt"))
    n.metadata = metadump(metadata_path)
    return n


class LogRepairTest(unittest.TestCase):
    def setUp(self):
        self.data_dir = tempfile.mkdtemp(prefix="raft-test-")
        node.configure(0, ["localhost:1", "localhost:2"], self.data_dir)
        leader_log = [("NO-OP", 1)] + [(f"SET k{i} v{i}", 1) for i in range(GAP)]
        self.leader = make_node(self.data_dir, 0, "Leader", leader_log)
        self.follower = make_node(self.data_dir, 1, "Follower", [("NO-OP", 1)])
        # A newly elected leader assumes every follower has its whole log.
        self.leader.sent_length[1] = self.leader.log.get_length()
        self.leader.acked_length[1] = 0

        self.rpc_errors = []
        self.real_call_peer = node.call_peer

        def call_peer(node_id, method_name, request, timeout=node.RPC_TIMEOUT):
            try:
                return self.follower.follower_recieving_message(request)
            except Exception as e:
                self.rpc_errors.append(repr(e)[:80])
                raise

        node.call_peer = call_peer

    def tearDown(self):
        node.call_peer = self.real_call_peer
        for n in (self.leader, self.follower):
            if n.lease_timer:
                n.lease_timer.cancel()
        shutil.rmtree(self.data_dir, ignore_errors=True)

    def test_far_behind_follower_catches_up_in_one_round(self):
        response = self.leader.replicate_log(0, 1)

        self.assertEqual(self.rpc_errors, [])
        self.assertTrue(response.success)
        self.assertEqual(self.follower.log.get_entries(), self.leader.log.get_entries())
        self.assertEqual(self.leader.acked_length[1], GAP + 1)
        self.assertEqual(self.leader.sent_length[1], GAP + 1)
        self.assertEqual(self.leader.commit_length, GAP + 1)  # both nodes of the 2-node cluster have it

    def test_higher_term_reply_stops_repair_and_steps_down(self):
        self.follower.current_term = 5
        response = self.leader.replicate_log(0, 1)

        self.assertFalse(response.success)
        self.assertEqual(self.leader.current_role, "Follower")
        self.assertEqual(self.leader.current_term, 5)
        self.assertEqual(self.follower.log.get_length(), 1)


if __name__ == "__main__":
    unittest.main()
