# — Standard library —
import logging

# — Third-party packages —
# (none)

# — This project —
# (none)

log = logging.getLogger(__name__)


class AdaptiveRouter:
    """
    Routes RPC calls to the best available server based on latency and distance.
    All routing intelligence is delegated to NearestServerDiscovery (in client.py).
    This class bridges the server-side and client-side code.

    Why it exists: servers need to query each other with automatic failover.
    This router handles the decision logic without duplicating discovery code.

    Usage:
      router = AdaptiveRouter(ring, discovery)
      result = router.route(account_id, lambda conn: conn.root.get_balance(...))
    """

    def __init__(self, ring, discovery):
        """
        Initialize the router.

        Args:
            ring: NodeRing instance (for consistent hashing)
            discovery: NearestServerDiscovery instance (for routing decisions)
        """
        self.ring = ring
        self.discovery = discovery

    def get_primary(self) -> dict:
        """
        Return the current primary server chosen by discovery.

        Returns:
            Dict with {"node_id": ..., "ip": ..., "port": ...}
        """
        return self.discovery.get_primary()

    def route(self, account_id: str, rpc_callable) -> any:
        """
        Execute an RPC call on the best server for this account.
        If the first server fails, automatically tries backups.

        Args:
            account_id: The account being accessed (for routing decisions)
            rpc_callable: Function(rpyc_connection) that performs the RPC call

        Returns:
            Result from rpc_callable

        Raises:
            Any exception from rpc_callable if all servers fail
        """
        return self.discovery.execute_with_fallback(rpc_callable)

    def get_neighbors(self, node_id: str) -> dict:
        """
        Find the K-closest servers to a given node (for fallback routing).

        Args:
            node_id: The node to find neighbors of

        Returns:
            Dict of neighbor node IDs and their connection info
        """
        if node_id not in self.ring.nodes:
            return {}
        return self.ring.nodes
