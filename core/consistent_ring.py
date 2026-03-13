# — Standard library —
import hashlib
import logging
import threading
import time
from typing import List, Dict, Optional

# — Third-party packages —
import etcd3

# — This project —
from core.exceptions import DiscoveryError

log = logging.getLogger(__name__)


# WHY A CONSISTENT HASH RING?
# ────────────────────────────
# Problem: with N servers, which one stores account "ACC123"?
# Simple solution: account_number % N. But if you add a server,
# N changes and almost every account moves — chaos.
#
# Consistent hashing: each server owns a section of a 0–2^256 circle.
# An account maps to the circle point nearest clockwise to its hash.
# When a server joins or leaves, only the accounts in its section move.
# 150 virtual points per server keeps the load evenly distributed.

VIRTUAL_NODES = 150
# How many virtual nodes per server on the ring. More = better load balancing,
# but more computation. 150 is a good balance for most cluster sizes.

RING_SIZE = 2**32
# Size of the hash ring. Using 32-bit (2^32) for fast modulo arithmetic.
# Larger values (e.g., 2^128) give better distribution but slower hashing.


class NodeRing:
    """
    Consistent hash ring for distributed account storage.
    Determines which server should own each account, and handles server
    joins/leaves with minimal account migration.

    Why it exists: in a distributed system, you need to decide which server
    stores account ACC123. Consistent hashing minimizes data movement when
    servers join or leave, and keeps load balanced across servers.

    Usage:
      ring = NodeRing(etcd_host="localhost", etcd_port=2379, cluster_name="mmoney")
      owner = ring.find_owner("ACC123")        # Which server owns this account?
      print(owner["node_id"])                  # "node-1"
      ring.register_node("node-1", {"ip": "192.168.1.1", "port": 18861})
      ring.watch_changes(on_add=lambda n: print(f"Added: {n}"),
                        on_remove=lambda n: print(f"Removed: {n}"))
    """

    def __init__(self, etcd_host: str, etcd_port: int, cluster_name: str):
        """
        Initialize the consistent ring.

        Args:
            etcd_host: IP/hostname of etcd server
            etcd_port: Port etcd listens on (usually 2379)
            cluster_name: Logical cluster name (e.g., "mmoney-cluster")
        """
        self.etcd_host = etcd_host
        self.etcd_port = etcd_port
        self.cluster_name = cluster_name
        self.etcd = None
        self._connect_etcd()

        # ring_points: dict of hash_value -> node dict
        # Stores ring position for each virtual node across all servers
        self.ring_points = {}

        # nodes: dict of node_id -> node dict {"ip": ..., "port": ...}
        self.nodes = {}

        # Callbacks for topology changes (used by discovery)
        self.topology_changed = threading.Event()
        self._change_callbacks = {"added": [], "removed": []}

        # Load initial nodes from etcd
        self._sync_ring_from_etcd()

    def _connect_etcd(self) -> None:
        """Establish connection to etcd cluster with exponential backoff retry."""
        max_retries = 15
        base_delay = 0.5
        
        for attempt in range(max_retries):
            try:
                self.etcd = etcd3.client(
                    host=self.etcd_host,
                    port=self.etcd_port,
                    timeout=5,
                )
                # Test the connection
                self.etcd.status()
                log.info("event=etcd_connected host=%s port=%s", self.etcd_host, self.etcd_port)
                return
            except Exception as e:
                if attempt == max_retries - 1:
                    raise DiscoveryError(
                        f"Failed to connect to etcd at {self.etcd_host}:{self.etcd_port} after {max_retries} attempts: {e}. "
                        f"Is etcd running?"
                    )
                # Exponential backoff: 0.5s, 1s, 2s, 4s, etc., max 10s
                delay = min(base_delay * (2 ** attempt), 10)
                log.warning(
                    "event=etcd_connect_failed attempt=%s max_attempts=%s retry_in_s=%.1f error=%s",
                    attempt + 1,
                    max_retries,
                    delay,
                    e,
                )
                time.sleep(delay)

    def _hash_point(self, key: str) -> int:
        """
        Hash a string to a point on the ring (0 to RING_SIZE-1).

        Uses SHA-256 hashed and takes the first 32 bits for fast modulo.

        Args:
            key: The value to hash (account ID, node name, etc)

        Returns:
            Integer position on the ring
        """
        digest = hashlib.sha256(key.encode()).digest()
        # Take first 4 bytes, interpret as big-endian unsigned int, modulo ring size
        return int.from_bytes(digest[:4], byteorder="big") % RING_SIZE

    def register_node(self, node_id: str, node_info: dict) -> None:
        """
        Register a new server in the cluster.

        Creates VIRTUAL_NODES virtual nodes on the ring and stores the node info
        in etcd. All registered nodes become available for account routing.

        Args:
            node_id: Unique server name (e.g., "node-1")
            node_info: Dict with at least {"ip": ..., "port": ...}
        """
        if node_id in self.nodes:
            log.warning("event=node_register_update node=%s", node_id)

        self.nodes[node_id] = node_info

        # Create virtual nodes on the ring
        for i in range(VIRTUAL_NODES):
            virtual_key = f"{node_id}:vnode-{i}"
            hash_point = self._hash_point(virtual_key)
            self.ring_points[hash_point] = node_id

        # Store in etcd so other servers see this node
        etcd_key = f"/{self.cluster_name}/nodes/{node_id}"
        try:
            self.etcd.put(etcd_key, json_encode(node_info))
            log.info("event=node_registered node=%s ip=%s port=%s", node_id, node_info.get("ip"), node_info.get("port"))
        except Exception as e:
            log.error("event=node_register_failed node=%s error=%s", node_id, e)
            raise DiscoveryError(f"Failed to register node: {e}")

    def deregister_node(self, node_id: str) -> None:
        """
        Remove a server from the cluster.

        Removes all virtual nodes and etcd entry. After this, accounts owned by
        this node will be reassigned to the next node on the ring.

        Args:
            node_id: Server ID to remove
        """
        if node_id not in self.nodes:
            log.warning("event=node_deregister_missing node=%s", node_id)
            return

        # Remove from etcd
        etcd_key = f"/{self.cluster_name}/nodes/{node_id}"
        try:
            self.etcd.delete(etcd_key)
        except Exception as e:
            log.error("event=node_deregister_etcd_failed node=%s error=%s", node_id, e)

        # Remove virtual nodes from ring
        hash_points_to_remove = [
            hp for hp, nid in self.ring_points.items() if nid == node_id
        ]
        for hp in hash_points_to_remove:
            del self.ring_points[hp]

        # Remove from local node list
        del self.nodes[node_id]
        log.info("event=node_deregistered node=%s", node_id)

    def find_owner(self, account_id: str) -> dict:
        """
        Find which server owns a specific account.

        Hashes the account ID to a ring point, then walks clockwise to find
        the nearest server. Raises DiscoveryError if ring is empty.

        Args:
            account_id: The account to locate (e.g., "ACC123")

        Returns:
            Dict with keys: {"node_id": ..., "ip": ..., "port": ...}

        Raises:
            DiscoveryError: if no servers are registered
        """
        if not self.ring_points:
            raise DiscoveryError("No servers registered in the ring")

        account_hash = self._hash_point(account_id)

        # Find the nearest ring point >= account_hash (walk clockwise)
        ring_sorted = sorted(self.ring_points.keys())

        # Find first point >= account_hash
        for ring_point in ring_sorted:
            if ring_point >= account_hash:
                owner_id = self.ring_points[ring_point]
                return {"node_id": owner_id, **self.nodes[owner_id]}

        # Wrap around: closest is the first point in the sorted list
        owner_id = self.ring_points[ring_sorted[0]]
        return {"node_id": owner_id, **self.nodes[owner_id]}

    def get_replicas(self, account_id: str, n_replicas: int = 3) -> List[dict]:
        """
        Find the N closest replicas for an account (primary + backups).

        Same as find_owner but returns multiple servers, walking further
        clockwise to find backups. Useful for replication and failover.

        Args:
            account_id: The account to locate
            n_replicas: How many replicas to return (primary + backups)

        Returns:
            List of dicts [primary, backup1, backup2, ...], up to n_replicas long
        """
        if not self.ring_points:
            raise DiscoveryError("No servers registered in the ring")

        account_hash = self._hash_point(account_id)
        ring_sorted = sorted(self.ring_points.keys())

        replicas = []
        seen_nodes = set()

        # Walk clockwise from account_hash, collecting unique servers
        for offset in range(len(ring_sorted) * 2):  # 2× to handle wrap-around
            ring_point = ring_sorted[(ring_sorted.index(min([p for p in ring_sorted if p >= account_hash], default=ring_sorted[0])) + offset) % len(ring_sorted)]
            owner_id = self.ring_points[ring_point]

            if owner_id not in seen_nodes:
                seen_nodes.add(owner_id)
                replicas.append({"node_id": owner_id, **self.nodes[owner_id]})

                if len(replicas) >= n_replicas:
                    break

        return replicas

    def _sync_ring_from_etcd(self) -> None:
        """
        Load all registered nodes from etcd and rebuild the ring.
        Called on startup and when topology changes.
        """
        try:
            self.nodes = {}
            self.ring_points = {}
            etcd_prefix = f"/{self.cluster_name}/nodes/"
            for value, metadata in self.etcd.get_prefix(etcd_prefix):
                if value:
                    node_id = metadata.key.decode().split("/")[-1]
                    node_info = json_decode(value.decode())
                    # Ensure node_id is always present in the dict
                    # (it's stored in the etcd key, but we need it in the value too)
                    if "node_id" not in node_info:
                        node_info["node_id"] = node_id
                    self.nodes[node_id] = node_info

                    # Add virtual nodes to ring
                    for i in range(VIRTUAL_NODES):
                        virtual_key = f"{node_id}:vnode-{i}"
                        hash_point = self._hash_point(virtual_key)
                        self.ring_points[hash_point] = node_id

            log.info(
                "event=ring_synced nodes=%s virtual_points=%s",
                len(self.nodes),
                len(self.ring_points),
            )
        except Exception as e:
            log.error("event=ring_sync_failed error=%s", e)

    def watch_changes(self, on_add=None, on_remove=None) -> None:
        """
        Watch for topology changes (nodes joining/leaving).
        Runs in a background thread. Calls on_add / on_remove callbacks.

        Args:
            on_add: Callback function(node_dict) when a node joins
            on_remove: Callback function(node_id) when a node leaves
        """
        if on_add:
            self._change_callbacks["added"].append(on_add)
        if on_remove:
            self._change_callbacks["removed"].append(on_remove)

        # Start watcher thread
        watch_thread = threading.Thread(
            target=self._watch_etcd_loop,
            daemon=True
        )
        watch_thread.start()

    def _watch_etcd_loop(self) -> None:
        """
        Background loop watching etcd for membership changes.
        Calls registered callbacks when nodes join or leave.
        """
        etcd_prefix = f"/{self.cluster_name}/nodes/"
        prev_nodes = set(self.nodes.keys())

        while True:
            try:
                current_nodes = set(self.nodes.keys())
                self._sync_ring_from_etcd()
                current_nodes = set(self.nodes.keys())

                # Detect joins (new nodes)
                for node_id in current_nodes - prev_nodes:
                    node_info = self.nodes[node_id]
                    log.info("event=node_joined node=%s", node_id)
                    for callback in self._change_callbacks["added"]:
                        try:
                            callback(node_info)
                        except Exception as e:
                            log.error("event=ring_on_add_callback_failed node=%s error=%s", node_id, e)

                # Detect leaves (removed nodes)
                for node_id in prev_nodes - current_nodes:
                    log.info("event=node_left node=%s", node_id)
                    for callback in self._change_callbacks["removed"]:
                        try:
                            callback(node_id)
                        except Exception as e:
                            log.error("event=ring_on_remove_callback_failed node=%s error=%s", node_id, e)

                prev_nodes = current_nodes
                threading.Event().wait(5)  # Check every 5 seconds

            except Exception as e:
                log.error("event=ring_watch_failed error=%s retry_in=5s", e)
                threading.Event().wait(5)


def json_encode(obj: dict) -> str:
    """Helper: serialize dict to JSON string."""
    import json
    return json.dumps(obj)


def json_decode(s: str) -> dict:
    """Helper: deserialize JSON string to dict."""
    import json
    return json.loads(s)
