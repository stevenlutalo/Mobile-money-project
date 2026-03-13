"""
node_server.py — Run this on every machine in your cluster.

Quick start:
  NODE_ID=node-1 NODE_PORT=18861 python services/node_server.py

What this does:
  - Registers this machine with the cluster (via etcd)
  - Listens for client requests (transfer, balance check, etc.)
  - Keeps account data in sync with other nodes via gossip
  - Monitors cluster health and handles node failures
"""

# — Standard library —
import logging
import logging.handlers
import os
import sys
import threading
import time
from typing import Dict

# — Third-party packages —
import rpyc
from dotenv import load_dotenv

# — This project —
from core.crdt import PNCounter
from core.consistent_ring import NodeRing
from core.delta_gossip import DeltaGossipService
from core.exceptions import (
    AuthError, InsufficientFundsError, InvalidRequest,
    NodeUnreachableError
)
from services.auth_service import AuthService
from services.health_monitor import HealthMonitor
from storage.shard_store import ShardStore
from storage.audit_log import AuditLog

log = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


# — Configuration constants —
NODE_ID = os.environ.get("NODE_ID", "node-1")
NODE_PORT = int(os.environ.get("NODE_PORT", 18861))
NODE_HOST = os.environ.get("NODE_HOST", "localhost")  # Hostname for inter-node communication
ETCD_HOST = os.environ.get("ETCD_HOST", "localhost")
ETCD_PORT = int(os.environ.get("ETCD_PORT", 2379))
CLUSTER_NAME = os.environ.get("CLUSTER_NAME", "mmoney-cluster")
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")

CERT_FILE = os.environ.get("CERT_FILE", f"certs/{NODE_ID}/cert.pem")
KEY_FILE = os.environ.get("KEY_FILE", f"certs/{NODE_ID}/key.pem")
CA_CERT_FILE = os.environ.get("CA_CERT_FILE", "certs/ca.pem")

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)


def setup_logging():
    """Configure logging to console and rotating file."""
    # Console handler (colorless for compatibility)
    console_handler = logging.StreamHandler()
    console_formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    console_handler.setFormatter(console_formatter)

    # File handler (rotating: max 5 MB per file, keep 3 backups)
    log_file = os.path.join(LOG_DIR, f"node_{NODE_ID}.log")
    file_handler = logging.handlers.RotatingFileHandler(
        log_file,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3,
    )
    file_formatter = logging.Formatter(
        "%(asctime)s | %(name)s | %(levelname)s | %(message)s"
    )
    file_handler.setFormatter(file_formatter)

    # Root logger configuration
    root_logger = logging.getLogger()
    root_logger.setLevel(getattr(logging, LOG_LEVEL))
    root_logger.addHandler(console_handler)
    root_logger.addHandler(file_handler)

    log.info(f"Logging configured: level={LOG_LEVEL}, file={log_file}")


class SecureNodeService(rpyc.Service):
    """
    RPC service exposed by each node. Clients call methods on this service
    to perform money transfers, check balances, create accounts, etc.

    All public methods are prefixed with 'exposed_' (rpyc convention).
    """

    def on_connect(self, conn):
        """Called when a client connects."""
        try:
            # Try to get peer address from the underlying socket
            peer_addr = conn._channel.stream.sock.getpeername()
            log.debug(f"Client connected: {peer_addr}")
        except (AttributeError, TypeError):
            log.debug("Client connected")

    def exposed_get_balance(self, token: str, account_id: str) -> float:
        """
        Return the current balance of an account.

        Args:
            token: JWT token from logged-in user
            account_id: Account to check

        Returns:
            Float balance in currency units

        Raises:
            AuthError: if token is invalid
        """
        self._verify_token(token)
        account = self.store.get(account_id)
        if account is None:
            return 0.0
        return account.balance()

    def exposed_transfer(
        self,
        token: str,
        from_account: str,
        to_account: str,
        amount: float,
    ) -> dict:
        """
        Transfer money from one account to another.
        Uses atomic batch write to ensure consistency.

        Args:
            token: JWT token
            from_account: Source account ID
            to_account: Destination account ID
            amount: Amount to transfer

        Returns:
            Dict of {"status": "ok", "from_balance": ..., "to_balance": ...}

        Raises:
            AuthError: if token invalid
            InsufficientFundsError: if source account doesn't have enough money
        """
        claims = self._verify_token(token)
        user_id = claims["sub"]

        # Load both accounts
        from_acc = self.store.get(from_account)
        to_acc = self.store.get(to_account)

        # Create if don't exist
        if from_acc is None:
            from_acc = PNCounter(NODE_ID)
        if to_acc is None:
            to_acc = PNCounter(NODE_ID)

        # Execute transfer
        from_acc.debit(amount)  # Raises InsufficientFundsError if balance < amount
        to_acc.credit(amount)

        # Atomic batch write
        self.store.put_batch({
            from_account: from_acc,
            to_account: to_acc,
        })

        # Log transaction
        self.audit_log.append(
            from_account, "debit", amount,
            f"{from_account}→{to_account} transfer",
            user_id
        )
        self.audit_log.append(
            to_account, "credit", amount,
            f"{from_account}→{to_account} transfer",
            user_id
        )

        log.info(f"Transfer: {from_account} → {to_account}, amount={amount}")

        return {
            "status": "ok",
            "from_balance": from_acc.balance(),
            "to_balance": to_acc.balance(),
        }

    def exposed_create_account(
        self,
        token: str,
        account_id: str,
        initial_deposit: float,
    ) -> dict:
        """
        Create a new account.

        Args:
            token: JWT token
            account_id: New account ID
            initial_deposit: Starting balance

        Returns:
            Dict of {"status": "ok", "account_id": ..., "balance": ...}

        Raises:
            AuthError: if token invalid
        """
        claims = self._verify_token(token)
        user_id = claims["sub"]

        account = PNCounter(NODE_ID, initial_balance=initial_deposit)
        self.store.put(account_id, account)

        self.audit_log.append(
            account_id, "create", initial_deposit,
            f"Account created",
            user_id
        )

        log.info(f"Created account {account_id} with balance={account.balance()}")

        return {
            "status": "ok",
            "account_id": account_id,
            "balance": account.balance(),
        }

    def exposed_receive_delta(self, payload: bytes) -> None:
        """
        Called by peer servers to send us delta updates (gossip).

        Args:
            payload: Compressed delta from peer
        """
        self.gossip_service.receive_delta(payload)

    def exposed_receive_full_state(self, payload: bytes) -> None:
        """
        Called by peer servers to send us full state (anti-entropy).

        Args:
            payload: Compressed state snapshot from peer
        """
        self.gossip_service.receive_full_state(payload)

    def exposed_get_neighbour_latencies(self) -> dict:
        """
        Return measured RTT (round-trip time) to all other servers in the cluster.
        Used by clients for server discovery (Phase 3).

        Returns:
            Dict of {node_id: rtt_ms, ...}
        """
        # In a real implementation, we'd measure actual RTTs.
        # For now, return empty dict (clients measure via TCP ping).
        return {}

    def exposed_promote_to_primary(self, failed_node_id: str) -> None:
        """
        Called by clients when a server fails, to request replication priority.
        Optional optimization for failover (not used in basic implementation).

        Args:
            failed_node_id: The node that just failed
        """
        log.warning(f"Received promotion request due to failure of {failed_node_id}")

    def exposed_tail_log(self, n: int = 20) -> list:
        """
        Return the last N audit log entries (for debugging/monitoring).

        Args:
            n: Number of entries to return

        Returns:
            List of transaction dicts
        """
        return self.audit_log.tail(n)

    def _verify_token(self, token: str) -> dict:
        """
        Verify a JWT token and return its claims.

        Args:
            token: JWT string

        Returns:
            Claims dict

        Raises:
            AuthError: if token is invalid
        """
        try:
            # In a real system, this would use the CA's public key.
            # For now, use the server's secret key (all servers share it).
            claims = self.auth_service.verify_token(token)
            return claims
        except Exception as e:
            raise AuthError(f"Token verification failed: {e}")


def get_peers_for_gossip():
    """
    Get list of peer server connections for gossip.
    Returns empty in this basic implementation (gossip not fully wired).
    """
    return []


def main():
    """Main entry point: initialize services and start the server."""
    setup_logging()

    # Print startup banner
    print("\n" + "=" * 50)
    print(f"  mmoney {NODE_ID} is starting...")
    print("=" * 50 + "\n")

    try:
        # 1. Connect to consistent ring (cluster membership)
        log.info(f"Connecting to cluster at {ETCD_HOST}:{ETCD_PORT}...")
        ring = NodeRing(ETCD_HOST, ETCD_PORT, CLUSTER_NAME)

        # 2. Initialize storage
        db_path = f"data/{NODE_ID}.db"
        log.info(f"Opening database at {db_path}...")
        store = ShardStore(db_path=db_path, node_id=NODE_ID)

        # 3. Initialize audit log
        audit_log_path = f"data/{NODE_ID}.audit"
        log.info(f"Opening audit log at {audit_log_path}...")
        audit_log = AuditLog(node_id=NODE_ID, log_path=audit_log_path)

        # 4. Initialize auth service
        auth_service = AuthService(secret_key="your-shared-secret-key-min-32-chars!")

        # 5. Initialize gossip service
        gossip_service = DeltaGossipService(
            local_node_id=NODE_ID,
            store=store,
            get_peers=get_peers_for_gossip,
            on_receive=lambda aid, delta: store.get(aid).merge_delta(delta) if store.get(aid) else None,
        )
        gossip_service.start()

        # 6. Register this node in the ring
        node_info = {
            "node_id": NODE_ID,
            "ip": NODE_HOST,  # Use configured hostname/IP
            "port": NODE_PORT,
        }
        ring.register_node(NODE_ID, node_info)

        # 7. Start health monitor
        health_monitor = HealthMonitor(ring, NODE_ID)
        health_monitor.start()

        # 8. Attach services to the RPC class
        SecureNodeService.store = store
        SecureNodeService.audit_log = audit_log
        SecureNodeService.auth_service = auth_service
        SecureNodeService.gossip_service = gossip_service
        SecureNodeService.ring = ring

        # 9. Start RPC server
        log.info(f"Starting RPC server on port {NODE_PORT}...")
        server = rpyc.ThreadedServer(
            SecureNodeService,
            port=NODE_PORT,
            protocol_config={"allow_pickle": False}
        )

        print("\n" + "=" * 50)
        print(f"  ✓ {NODE_ID} is running")
        print(f"  ✓ Listening on port {NODE_PORT}")
        print(f"  ✓ Cluster: {CLUSTER_NAME}")
        print("=" * 50 + "\n")

        # Start server (blocks until interrupted)
        server.start()

    except KeyboardInterrupt:
        log.info("Server shutting down...")
        print("\n✓ Shutdown complete\n")
        sys.exit(0)

    except Exception as e:
        log.error(f"Fatal error: {e}", exc_info=True)
        print(f"\n✗ Error: {e}\n")
        sys.exit(1)


# Missing exception class (should be in core/exceptions.py)
class InvalidRequest(Exception):
    pass


if __name__ == "__main__":
    main()
