import argparse
import concurrent.futures as futures
import os
import random
import signal, sys
import threading
import grpc
import raft_pb2_grpc
import raft_pb2
import time
import custom_timer
from metadata import metadump
from cluster_config import parse_cluster, DEFAULT_CLUSTER

# Emoji-heavy logging must not crash when stdout is redirected on Windows (cp1252).
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")

# |--------------------------------------|
# | DEVELOPMENT VARIABLES                |
# |--------------------------------------|

lock = threading.Lock()
lease_lock = threading.Lock()

# Deadline (seconds) for every node-to-node RPC so that a dead or hung peer
# cannot block the heartbeat / election loop indefinitely.
RPC_TIMEOUT = 1.0


def signal_handler(signal, frame):
    print("➡️  Received interrupt signal...")
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)

# Cluster membership is filled in by configure() from the command line
# (--id / --cluster) or the RAFT_CLUSTER environment variable.
ID = None
ALL_ADDRESSES = []  # "host:port" of every node, indexed by node id
PORT = None
OTHER_IDS = []
DATA_DIR = "."


def configure(node_id, addresses, data_dir="."):
    global ID, ALL_ADDRESSES, PORT, OTHER_IDS, DATA_DIR
    if not 0 <= node_id < len(addresses):
        raise SystemExit(f"Node id {node_id} is out of range for a {len(addresses)}-node cluster")
    ID = node_id
    ALL_ADDRESSES = addresses
    PORT = addresses[node_id].rsplit(":", 1)[1]
    OTHER_IDS = [i for i in range(len(addresses)) if i != node_id]
    DATA_DIR = data_dir


def call_peer(node_id, method_name, request, timeout=RPC_TIMEOUT):
    """Invoke an RPC on another node over a short-lived channel with a deadline."""
    with grpc.insecure_channel(ALL_ADDRESSES[node_id]) as channel:
        stub = raft_pb2_grpc.raft_serviceStub(channel)
        return getattr(stub, method_name)(request, timeout=timeout)

# |--------------------------------------|
# | gRPC SERVICER CLASS                  |
# |--------------------------------------|


class raft_serviceServicer(raft_pb2_grpc.raft_serviceServicer):
    def __init__(self, node):
        self.node = node

    def appendEntry(self, request, copntext):
        if self.node.current_role != "Leader":
            self.node.stop_election_timeout()
        response = self.node.follower_recieving_message(request)
        return response

    def requestVote(self, request, context):
        response = self.node.vote_on_new_leader(request)
        return response

    def serveClient(self, request, context):
        print("Request Recieved:", request.Request)
        return self.node.handle_broadcast_message(request)


# |--------------------------------------|
# | LOG CLASS                            |
# |--------------------------------------|


class Log:
    def __init__(self, log_file_path):
        self.entries = []
        self.log_file_path = log_file_path
        with open(self.log_file_path, "r", encoding="utf-8") as f:
            for line in f:
                parsed_entry = line.strip().split(" ")
                command, term = " ".join(parsed_entry[:-1]), parsed_entry[-1]
                self.entries.append((command, term))
            f.close()

    def add_entry(self, term, command):
        self.entries.append((command, term))
        self.dump_log()

    def get_last_entry(self):
        return self.entries[-1][0], int(self.entries[-1][1])

    def get_entry(self, index):
        return self.entries[index][0], int(self.entries[index][1])

    def get_entries(self):
        return self.entries

    def get_entry_by_key(self, key):
        value = ""
        for entry in self.entries:
            if entry[0].split()[0] == "SET" and entry[0].split()[1] == key:
                value = entry[0].split()[2]
        return value

    def print_entries(self):
        for entry in self.entries:
            print(entry)

    def modify_log(self, entries):
        self.entries = entries
        self.dump_log()

    def get_length(self):
        return len(self.entries)

    def dump_log(self):
        open(self.log_file_path, "w").close()
        with open(self.log_file_path, "w", encoding="utf-8") as f:
            for entry in self.entries:
                f.write(str(entry[0]) + " " + str(entry[1]) + "\n")
            f.close()

    def dump_text(self,text):
        with open(self.log_file_path, "a", encoding="utf-8") as f:
            f.write(text + "\n")
            f.close()
    
    def rewrite_log(self,text):
        with open(self.log_file_path, "w", encoding="utf-8") as f:
            f.write(text + "\n")
            f.close()



# |--------------------------------------|
# | NODE CLASS                           |
# |--------------------------------------|


class Node:

    # |--------------------------------------|
    # | NODE INITIALIZATION                  |
    # |--------------------------------------|

    def __init__(self):
        self.node_id = None # meta
        self.current_term = None # meta
        self.max_lease_duration = None  
        self.lease_duration = None 
        self.lease_timer_alive = None  
        self.lease_timer = None
        self.has_lease = None
        self.voted_for = None
        self.log = None
        self.dump = None 
        self.metadata = None
        self.commit_length = None # meta
        self.current_role = None
        self.current_leader = None
        self.votes_recieved = None
        self.sent_length = None
        self.ackedLength = None
        self.election_timer = None
        self.log_file = None
        self.temp = 0
        self.last_term = None

    def initialize_node(self, node_id):
        self.node_id = node_id 
        self.current_term = 0 
        self.voted_for = None
        self.max_lease_duration = 8
        self.lease_duration = 0  
        self.lease_timer_alive = False  
        self.lease_timer = None
        self.has_lease = False
        self.commit_length = 0
        self.current_role = "Follower"
        self.current_leader = None
        self.votes_recieved = set()
        self.sent_length = {}
        self.acked_length = {}
        self.election_timeout = random.uniform(5, 11)
        self.election_timer = threading.Timer(
            self.election_timeout, self.handle_election_timeout
        )
        node_dir = os.path.join(DATA_DIR, "logs_node_" + str(self.node_id))
        os.makedirs(node_dir, exist_ok=True)
        self.log_file_path = os.path.join(node_dir, "logs.txt")
        self.dump_file_path = os.path.join(node_dir, "dump.txt")
        self.metadata_file_path = os.path.join(node_dir, "metadata.txt")
        self.election_timer_alive = False
        if not os.path.exists(self.log_file_path):
            open(self.log_file_path, "w").close()
            open(self.dump_file_path, "w").close()
        if not os.path.exists(self.metadata_file_path):
            open(self.metadata_file_path, "w").close()
            self.metadata = metadump(self.metadata_file_path)
            self.metadata.write_blank_metadata_file()
            self.metadata.update_metadata("Term",self.current_term)
            self.metadata.update_metadata("commitLength",self.commit_length)

        self.log = Log(self.log_file_path)
        self.dump = Log(self.dump_file_path)
        self.metadata = metadump(self.metadata_file_path)
        update_params=self.metadata.read_metadata()
        self.current_term=int(update_params['Term'])
        self.commit_length=int(update_params['commitLength'])
        if update_params['NodeID']!="NA":
            self.voted_for=int(update_params['NodeID'])
        if self.log.get_length() > 0:
            print("➡️  NODE RESTARTED...")
            self.current_term = self.log.get_last_entry()[1]
        else:
            print("➡️  NODE INITIALIZED...")
        print("📒  Current Log:", self.log.get_entries())
        print("⏰ Election timeout set to:", self.election_timeout)
        server_thread = threading.Thread(target=self.start_server)
        server_thread.daemon = True
        server_thread.start()
        self.start_client()

    # |--------------------------------------|
    # | INITIALIZE CLIENT AND SERVER THREADS |
    # |--------------------------------------|

    def update_metadata(self):
        temp = f"Commit Length = {self.commit_length} | Current Term = {self.current_term} | Voter for = {self.voted_for}"
        self.metadata.rewrite_log(temp)
    def start_client(self):
        while True:
            if self.current_role == "Candidate":
                if self.election_timer and self.election_timer_alive == False:
                    self.send_request_vote()
            elif self.current_role == "Follower":
                if (
                    self.election_timer
                    and self.election_timer_alive == False
                    and self.lease_timer_alive == False
                ):
                    self.start_election_timeout()
            elif self.current_role == "Leader":
                self.heartbeat()
                time.sleep(1)
                continue
            # Avoid spinning a CPU core at 100% while waiting on a timer.
            time.sleep(0.05)

    def start_server(self):
        print("PID:", os.getpid())
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_raft_serviceServicer_to_server(
            raft_serviceServicer(self), server
        )
        port = server.add_insecure_port("[::]:" + PORT)
        error = server.start()
        if error:
            print("❌ Error starting server:", error)
            return
        print("✅ Server started listening on port", port)
        server.wait_for_termination()
        return

    # |--------------------------------------|
    # | HANDLE ELECTION PROCESS              |
    # |--------------------------------------|

    def start_election_timeout(self):
        self.election_timer_alive = True
        self.election_timer = threading.Timer(
            self.election_timeout, self.handle_election_timeout
        )
        self.election_timer.daemon = True
        self.election_timer.start()
        print("⏰ Election timeout started...")

    def stop_election_timeout(self):
        self.election_timer.cancel()
        self.election_timer_alive = False
        print("⏰ Election timeout stopped...")

    def handle_election_timeout(self):
        print("⏰ Election timeout triggered...")
        lock.acquire()
        self.dump.dump_text(f"Node {self.node_id} election timer timed out, Starting election.")
        self.election_timer_alive = False
        self.current_role = "Candidate"
        lock.release()

    def renew_lease(self, renew_time):
        if self.lease_timer_alive:
            self.stop_lease()
        self.lease_timer_alive = True
        self.lease_timer = custom_timer.LeaseTimer(
            renew_time, self.handle_lease_timeout
        )
        self.lease_timer.start()
        # print("⏰ Lease renewed...")

    def toggle_has_lease(self):
        self.has_lease = True

    def stop_lease(self):
        self.lease_timer.cancel()
        self.lease_timer_alive = False

    def handle_lease_timeout(self):
        print("⏰ Lease timeout triggered...")
        lease_lock.acquire()
        self.dump.dump_text(f"Leader {self.node_id} lease renewal failed. Stepping Down.")
        if self.current_role=="Leader":
            self.dump.dump_text(f"{self.node_id} Stepping Down")
        self.current_role = "Follower"
        self.current_leader = None
        self.lease_duration = 0
        self.lease_timer_alive = False
        self.has_lease = False
        lease_lock.release()

    def vote_on_new_leader(self, request):
        cTerm = request.term
        CId = request.candidateId
        cLogLength = request.lastLogIndex
        cLogTerm = request.lastLogTerm
        if cTerm > self.current_term:
            self.current_term = cTerm
            self.metadata.update_metadata("Term",self.current_term)
            if self.current_role=="Leader":
                self.dump.dump_text(f"{self.node_id} Stepping Down")
            self.current_role = "Follower"
            self.voted_for = None
            self.metadata.update_metadata("NodeID","NA")
        self.last_term = 0
        if self.log.get_length() > 0:
            _, term = self.log.get_last_entry()
            self.last_term = term
        logOK = (cLogTerm > self.last_term) or (
            (cLogTerm == self.last_term) and (cLogLength >= self.log.get_length())
        )

        response = raft_pb2.RequestVoteResponse()

        if (
            (cTerm == self.current_term)
            and logOK
            and (self.voted_for == CId or self.voted_for == None)
        ):
            print("❎  I VOTED FOR:", CId)
            self.dump.dump_text(f"Vote granted for Node {CId} in term {cTerm}.")
            self.voted_for = CId
            self.metadata.update_metadata("NodeID",CId)
            # Granting a vote resets the election timer (Raft §5.2); otherwise a
            # voter can time out before the new leader's first heartbeat arrives
            # and start a needless competing election.
            if self.current_role != "Leader":
                self.stop_election_timeout()
            response.voteGranted = True
            response.nodeId = self.node_id
            response.term = self.current_term
        else:
            print("❎ I DID NOT VOTE FOR:", CId)
            self.dump.dump_text(f"Vote denied for Node {CId} in term {cTerm}.")
            response.voteGranted = False
            response.nodeId = self.node_id
            response.term = self.current_term

        return response

    def send_request_vote(self):
        print("🗳  Requesting votes...")
        request_vote_request = raft_pb2.RequestVoteRequest()
        self.voted_for = self.node_id
        self.metadata.update_metadata("NodeID",self.voted_for)
        # Votes only count for the term they were cast in: start every election afresh.
        self.votes_recieved = {self.node_id}
        self.current_term = self.current_term + 1
        self.metadata.update_metadata("Term",self.current_term)
        last_term = 0
        if self.log.get_length() > 0:
            last_term = self.log.get_last_entry()[1]
        request_vote_request.term = self.current_term
        request_vote_request.candidateId = self.node_id
        request_vote_request.lastLogIndex = max(0, self.log.get_length())
        request_vote_request.lastLogTerm = last_term
        responses = {}
        self.lease_duration = 0
        for ID in OTHER_IDS:
            try:
                response = call_peer(ID, "requestVote", request_vote_request)
                responses[response.nodeId] = response
                if (
                    self.lease_timer
                    and response.leaseDuration > self.lease_timer.time_left()
                ):
                    self.lease_duration = response.leaseDuration
                    self.renew_lease(self.lease_duration)
                print("❎ Response recieved from Node-", response.nodeId)
                self.handle_vote_reponse(response)
            except:
                self.dump.dump_text(f"Error occurred while sending RPC to Node {ID}.")
                print("❌ Error sending request to:", ALL_ADDRESSES[ID])

        if self.current_role == "Candidate":
            self.start_election_timeout()

    def handle_vote_reponse(self, response):

        responder_id = response.nodeId
        responder_term = response.term
        responder_vote_granted = response.voteGranted
        if (
            self.current_role == "Candidate"
            and responder_term == self.current_term
            and responder_vote_granted
        ):
            self.votes_recieved.add(responder_id)
            if len(self.votes_recieved) > len(ALL_ADDRESSES) / 2:

                self.current_role = "Leader"
                self.current_leader = self.node_id
                if (not self.has_lease):
                    self.dump.dump_text("New Leader waiting for Old Leader Lease to timeout.")
                    self.dump.dump_text(f"Node {self.node_id} became the leader for term {self.current_term}.")
                self.stop_election_timeout()
                check_thread=threading.Timer(self.lease_duration, self.toggle_has_lease)
                check_thread.daemon=True
                check_thread.start()
                self.log.add_entry(self.current_term, "NO-OP")
                print("🎉 Leader elected:", self.current_leader)

                for ID in OTHER_IDS:
                    try:
                        self.sent_length[ID] = self.log.get_length()
                        self.acked_length[ID] = 0
                        self.replicate_log(self.current_leader, ID)
                    except:
                        print("❌ Error sending request to:", ALL_ADDRESSES[ID])
        elif responder_term > self.current_term:
            # A voter is ahead of us: adopt its term and step back to follower.
            self.current_term = responder_term
            self.metadata.update_metadata("Term",self.current_term)
            self.current_role = "Follower"
            self.voted_for = None
            self.metadata.update_metadata("NodeID","NA")
            self.stop_election_timeout()

    # |--------------------------------------|
    # | LEADER FUNCTIONALITY                 |
    # |--------------------------------------|

    def replicate_log(self, leaderId, followerId):
        # Log repair backs sentLength off one entry per rejected AppendEntries.
        # Retry in a loop until the follower accepts or recieve_log_ack says to
        # stop (we are no longer leader, or the term changed); recursing through
        # recieve_log_ack instead overflowed the stack for a follower ~500
        # entries behind.
        while True:
            if followerId not in self.sent_length.keys():
                self.sent_length[followerId] = 0
            prefixLen = self.sent_length[followerId]
            suffix = [
                self.log.get_entry(i) for i in range(prefixLen, self.log.get_length())
            ]

            prefixTerm = 0
            if prefixLen > 0:
                prefixTerm = self.log.get_entry(prefixLen - 1)[1]

            suffix_entry = raft_pb2.Entry()
            for entry in suffix:
                suffix_entry.commands.append(f"{entry[0]} {entry[1]}")

            append_entry_request = raft_pb2.AppendEntryRequest(
                term=self.current_term,
                leaderId=leaderId,
                prevLogIndex=prefixLen,
                prevLogTerm=prefixTerm,
                entries=suffix_entry,
                leaderCommit=self.commit_length,
                leaseDuration=self.max_lease_duration,
            )

            try:
                response = call_peer(followerId, "appendEntry", append_entry_request)
                print(f"✅ Log replicated to Node-{followerId}")
            except Exception as e:
                self.dump.dump_text(f"Error occurred while sending RPC to Node {followerId}.")
                print("❌ Error sending request to:", ALL_ADDRESSES[followerId])
                response = raft_pb2.AppendEntryResponse()
                response.term = self.current_term
                response.success = False
                response.nodeId = self.node_id
                response.ack = 0
                return response

            retry = self.recieve_log_ack(
                response.nodeId, response.term, response.ack, response.success
            )
            if not retry:
                return response

    def recieve_log_ack(self, follower, term, ack, success):
        # Function 8 out of 9
        # Returns True when the follower rejected the entries and sentLength was
        # decremented, so replicate_log should send again from the earlier index.
        if term == self.current_term and self.current_role == "Leader":
            if success == True and ack >= self.acked_length[follower]:
                self.sent_length[follower] = ack
                self.acked_length[follower] = ack
                self.commit_log_entries()
            elif self.sent_length[follower] > 0:
                self.sent_length[follower] -= 1
                return True
        elif term > self.current_term:
            self.current_term = term
            self.metadata.update_metadata("Term",self.current_term)
            self.current_role = "Follower"
            self.dump.dump_text(f"{self.node_id} Stepping Down")
            self.voted_for = None
            self.metadata.update_metadata("NodeID","NA")
            self.stop_election_timeout()
        return False

    def set_of_acks(self, length):
        # Helper Function 9 out of 9
        count = 0
        for i in self.acked_length.keys():
            if self.acked_length[i] >= length:
                count += 1
        return count

    def commit_log_entries(self):
        # Function 9 out of 9
        # An entry is committed once a majority of the cluster stores it; the
        # leader always has its own entries, so count it alongside follower acks.
        ready = []
        for i in range(1, self.log.get_length() + 1):
            if self.set_of_acks(i) + 1 > len(ALL_ADDRESSES) / 2:
                ready.append(i)
        if (
            len(ready) > 0
            and max(ready) > self.commit_length
            and self.log.get_entry(max(ready) - 1)[1] == self.current_term
        ):
            for i in range(self.commit_length, max(ready)):
                print("STATE COMMITED:", self.log.get_entry(i)[0])
            self.commit_length = max(ready)
            self.metadata.update_metadata("commitLength",self.commit_length)

        return

    def handle_broadcast_message(self, message):
        user_response = raft_pb2.ServeClientReply()
        user_response.LeaderID = str(self.current_leader)
        if self.current_role == "Leader":
            self.dump.dump_text(f"Node {self.node_id} (leader) received an {message.Request} request.")
            if message.Request.split()[0] == "GET":
                if not self.has_lease:
                    user_response.Success = False
                    user_response.Data = "Leader does not have lease!."
                else:
                    user_response.Success = True
                    user_response.Data = self.log.get_entry_by_key(
                        message.Request.split()[1]
                    )

            elif message.Request.split()[0] == "SET":
                print("GOT MESSAGE SET", self.has_lease)
                if not self.has_lease:
                    user_response.Success = False
                    user_response.Data = "Leader does not have lease!."
                else:
                    count_success = 0
                    self.log.add_entry(term=self.current_term, command=message.Request)
                    for follower_id in OTHER_IDS:
                        response = self.replicate_log(self.node_id, follower_id)
                        if response.success:
                            count_success += 1
                    if count_success + 1 > len(ALL_ADDRESSES) / 2:  # +1: the leader itself
                        user_response.Success = True
                        user_response.Data = (
                            str(message.Request) + " successfully committed."
                        )
                        self.dump.dump_text(f"Node {self.node_id} (leader) received an {message.Request} request.")
                    else:
                        user_response.Success = False
                        user_response.Data = "Could not replicate to a majority of nodes."



            print("✉️  Returning response to user", user_response)
            return user_response

        else:
            print("📝  Recieved client request...")
            leader = self.current_leader
            if leader is None or leader == self.node_id:
                user_response.LeaderID = "None"
                user_response.Success = False
                user_response.Data = "No leader known yet; retry shortly."
                return user_response
            print("⏩ Redirecting to Leader-", leader)
            try:
                # SET waits for replication to every follower, so allow more time.
                response = call_peer(leader, "serveClient", message, timeout=15)
                print("📝  Response from Leader-", leader, ":", response)
                return response
            except:
                print("❌ Error forwarding request to leader:", ALL_ADDRESSES[leader])
                user_response.Success = False
                user_response.Data = f"Could not reach leader {leader}; retry shortly."
                return user_response

    def heartbeat(self):
        if self.current_role == "Leader":
            self.dump.dump_text(f"Leader {self.node_id} sending heartbeat and Renewing Lease")
            responses = []
            for follower_id in OTHER_IDS:
                response = self.replicate_log(self.node_id, follower_id)
                if response:
                    responses.append(response)

            count_success = 0
            for response in responses:
                if response.success:
                    count_success += 1
            print("♥ Successful Heartbeat Count:", count_success)

            if count_success + 1 > len(ALL_ADDRESSES) / 2:  # +1: the leader itself
                self.renew_lease(self.max_lease_duration)

    # |--------------------------------------|
    # | FOLLOWER FUNCTIONALITY               |
    # |--------------------------------------|

    def follower_recieving_message(self, message):
        # Function 6 out of 9
        leader_id = message.leaderId
        term = message.term
        prefixLen = message.prevLogIndex
        prefixTerm = message.prevLogTerm
        leaderCommit = message.leaderCommit
        suffix = message.entries.commands
        self.lease_duration = message.leaseDuration

        self.renew_lease(self.lease_duration)
        if term > self.current_term:
            self.current_term = term
            self.metadata.update_metadata("Term",self.current_term)
            self.voted_for = None
            self.metadata.update_metadata("NodeID","NA")
            self.stop_election_timeout()

        if term == self.current_term:
            print("👂 Recieved message from Leader-", leader_id)
            self.current_role = "Follower"
            self.current_leader = leader_id

        logOK = (self.log.get_length() >= prefixLen) and (
            prefixLen == 0 or self.log.get_entry(prefixLen - 1)[1] == prefixTerm
        )
        append_entry_response = raft_pb2.AppendEntryResponse()
        append_entry_response.nodeId = self.node_id
        append_entry_response.term = self.current_term
        if term == self.current_term and logOK:
            self.dump.dump_text(f"Node {self.node_id} accepted AppendEntries RPC from {leader_id}.")
            self.append_entries(prefixLen, leaderCommit, suffix)
            ack = prefixLen + len(suffix)
            append_entry_response.ack = ack
            append_entry_response.success = True
        else:
            self.dump.dump_text(f"Node {self.node_id} rejected AppendEntries RPC from {leader_id}.")
            append_entry_response.ack = 0
            append_entry_response.success = False

        return append_entry_response

    def append_entries(self, prefixLen, leaderCommit, suffix):
        # Function 7 out of 9
        if len(suffix) > 0 and self.log.get_length() > prefixLen:
            index = min(self.log.get_length(), prefixLen + len(suffix)) - 1
            if self.log.get_entry(index)[1] != int(suffix[index - prefixLen].split()[-1]):
                self.log.modify_log(self.log.get_entries()[0:prefixLen])
        if prefixLen + len(suffix) > self.log.get_length():
            for i in range(self.log.get_length() - prefixLen, len(suffix)):
                command = " ".join(suffix[i].split()[0:-1])
                term = suffix[i].split()[-1]
                self.log.add_entry(term, command)
        if leaderCommit > self.commit_length:
            for i in range(self.commit_length, leaderCommit):
                print("COMMITTED", self.log.get_entry(i)[0])
                self.dump.dump_text(f"Node {self.node_id} (follower) committed the entry {self.log.get_entry(i)[0]} to the state machine.")
            self.commit_length = leaderCommit
            self.metadata.update_metadata("commitLength",self.commit_length)
        return


def parse_args():
    parser = argparse.ArgumentParser(description="Run one node of the Raft key-value cluster.")
    parser.add_argument(
        "--id", type=int, help="this node's index in the cluster list (prompted for if omitted)"
    )
    parser.add_argument(
        "--cluster",
        help="comma-separated host:port of every node, in id order "
        f"(default: $RAFT_CLUSTER or {DEFAULT_CLUSTER})",
    )
    parser.add_argument(
        "--data-dir",
        default=os.environ.get("RAFT_DATA_DIR", "."),
        help="directory that holds logs_node_<id>/ (default: $RAFT_DATA_DIR or the current directory)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    node_id = args.id if args.id is not None else int(input("ENTER ID:"))
    configure(node_id, parse_cluster(args.cluster), args.data_dir)
    node = Node()
    node.initialize_node(ID)
