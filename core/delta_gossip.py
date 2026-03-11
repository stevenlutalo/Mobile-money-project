# — Standard library —
import json
import logging
import random
import threading
import time
import zlib
from typing import Dict, Callable

# — Third-party packages —
# (none)

# — This project —
from core.exceptions import SyncError

log = logging.getLogger(__name__)


# WHY DELTA GOSSIP?
# ─────────────────
# Problem: when an account changes on Server A, how do Servers B and C
# find out without flooding the network?
#
# Gossip protocol: every 2 seconds, each server tells 3 random peers
# about ONLY what changed since the last round (the "delta"). Peers
# forward it on. Within a few rounds, every server knows.
# zlib compression reduces each message to a fraction of its raw size.
#
# Anti-entropy: every 5 minutes, one server sends its FULL state to
# one random peer. This heals any gaps that gossip might have missed.

GOSSIP_INTERVAL_SECS = 2
# How often to send deltas to peers. Lower = faster propagation,
# higher network traffic. 2 seconds is a good balance.

GOSSIP_PEERS = 3
# How many random peers to send deltas to each round.
# With 10 servers, reaching all peers takes ~log(10) = 3–4 rounds.

ANTI_ENTROPY_INTERVAL_SECS = 300
# Full state sync interval (5 minutes). Ensures durability even if
# gossip drops some messages (which it can, because UDP is unreliable).
# In our implementation, this runs per-server.

COMPRESSION_THRESHOLD = 100
# Compress deltas larger than this many bytes.


class DeltaGossipService:
    """
    Synchronizes account changes across the server cluster.
    Uses delta gossip (send only changes) + anti-entropy (periodic full sync).

    Why it exists: in a distributed cluster, changes on one server must
    eventually reach all others. Sending the full state every time would
    waste bandwidth. Sending only deltas (what changed) is much cheaper.

    Usage:
      gossip = DeltaGossipService(
          local_node_id="node-1",
          store=shard_store,
          get_peers=lambda: [node2, node3],
          on_receive=my_apply_function
      )
      gossip.start()            # Start background gossip rounds
      # ... later ...
      gossip.stop()             # Clean shutdown
    """

    def __init__(
        self,
        local_node_id: str,
        store,  # shard_store.ShardStore instance
        get_peers: Callable,  # Function that returns available peer connections
        on_receive: Callable,  # Function to apply received deltas
    ):
        """
        Initialize the gossip service.

        Args:
            local_node_id: This server's unique ID
            store: ShardStore instance with get/put/get_delta
            get_peers: Callable that returns list of (node_id, rpyc_conn) tuples
            on_receive: Callable(account_id, delta) to apply external changes
        """
        self.local_node_id = local_node_id
        self.store = store
        self.get_peers = get_peers
        self.on_receive = on_receive

        self._stop_event = threading.Event()
        self._gossip_thread = None
        self._anti_entropy_thread = None

    def start(self) -> None:
        """Start background gossip and anti-entropy threads."""
        if self._gossip_thread is not None:
            log.warning("Gossip service already running")
            return

        self._stop_event.clear()
        self._gossip_thread = threading.Thread(
            target=self._gossip_loop,
            daemon=True,
            name=f"gossip-{self.local_node_id}"
        )
        self._gossip_thread.start()

        self._anti_entropy_thread = threading.Thread(
            target=self._anti_entropy_loop,
            daemon=True,
            name=f"anti-entropy-{self.local_node_id}"
        )
        self._anti_entropy_thread.start()

        log.info(f"Gossip service started for {self.local_node_id}")

    def stop(self) -> None:
        """Stop all gossip activity and wait for threads to finish."""
        log.info(f"Stopping gossip service for {self.local_node_id}")
        self._stop_event.set()

        if self._gossip_thread:
            self._gossip_thread.join(timeout=5)
        if self._anti_entropy_thread:
            self._anti_entropy_thread.join(timeout=5)

    def _gossip_loop(self) -> None:
        """
        Main gossip loop: every GOSSIP_INTERVAL_SECS, send deltas to random peers.
        Runs in background thread until stop() is called.
        """
        while not self._stop_event.is_set():
            try:
                self._gossip_round()
            except Exception as e:
                log.error(f"Error in gossip round: {e}")

            # Sleep in small increments so stop() responds quickly
            for _ in range(int(GOSSIP_INTERVAL_SECS * 10)):
                if self._stop_event.is_set():
                    break
                time.sleep(0.1)

    def _gossip_round(self) -> None:
        """
        Send delta updates to GOSSIP_PEERS random peers.
        Each peer receives only what this server has changed since last round.
        """
        try:
            peers = self.get_peers()
            if not peers:
                return

            # Select random peers to gossip with (at most GOSSIP_PEERS)
            selected = random.sample(peers, min(len(peers), GOSSIP_PEERS))

            # For each account, get its delta
            for account_id in self.store.list_accounts():
                try:
                    delta = self.store.get_delta(account_id)
                    if not delta:
                        continue  # No changes for this account

                    # Send to each selected peer
                    for peer_conn in selected:
                        try:
                            payload = self._prepare_payload(account_id, delta)
                            peer_conn.root.receive_delta(payload)
                        except Exception as e:
                            log.debug(f"Failed to send delta to peer: {e}")

                except Exception as e:
                    log.debug(f"Error getting delta for {account_id}: {e}")

        except Exception as e:
            log.error(f"Gossip round failed: {e}")

    def _anti_entropy_loop(self) -> None:
        """
        Every ANTI_ENTROPY_INTERVAL_SECS, send full state to one random peer.
        This "heals" any gaps that gossip might have missed.
        """
        while not self._stop_event.is_set():
            try:
                time.sleep(ANTI_ENTROPY_INTERVAL_SECS)
                if self._stop_event.is_set():
                    break

                self._anti_entropy_round()

            except Exception as e:
                log.error(f"Error in anti-entropy round: {e}")

    def _anti_entropy_round(self) -> None:
        """Send full state snapshot to one random peer."""
        try:
            peers = self.get_peers()
            if not peers:
                return

            peer_conn = random.choice(peers)

            # Collect full state of all accounts
            full_state = {}
            for account_id in self.store.list_accounts():
                account_data = self.store.get(account_id)
                if account_data:
                    full_state[account_id] = account_data

            if not full_state:
                return

            payload = self._compress(json.dumps(full_state))
            try:
                peer_conn.root.receive_full_state(payload)
                log.debug(f"Sent anti-entropy update to peer (size: {len(payload)} bytes)")
            except Exception as e:
                log.debug(f"Anti-entropy send failed: {e}")

        except Exception as e:
            log.error(f"Anti-entropy round failed: {e}")

    def _prepare_payload(self, account_id: str, delta: dict) -> bytes:
        """
        Package an account delta for transmission.
        Structure: {"account_id": ..., "delta": {...}}
        Compresses if size > COMPRESSION_THRESHOLD.

        Args:
            account_id: Account ID
            delta: CRDT delta dict

        Returns:
            Bytes to send over network
        """
        payload_dict = {
            "account_id": account_id,
            "delta": delta,
            "source_node": self.local_node_id,
            "timestamp": time.time(),
        }
        raw = json.dumps(payload_dict).encode()
        if len(raw) > COMPRESSION_THRESHOLD:
            return self._compress(raw)
        return raw

    def _compress(self, data: bytes) -> bytes:
        """
        Compress data using zlib.

        Args:
            data: Bytes to compress

        Returns:
            Compressed bytes, prefixed with "ZLIB:" marker
        """
        if isinstance(data, str):
            data = data.encode()
        compressed = zlib.compress(data, level=6)
        return b"ZLIB:" + compressed

    def _decompress(self, data: bytes) -> dict:
        """
        Decompress zlib data if present, then parse JSON.

        Args:
            data: Bytes from network

        Returns:
            Dict from JSON
        """
        if data.startswith(b"ZLIB:"):
            data = zlib.decompress(data[5:])
        if isinstance(data, bytes):
            data = data.decode()
        return json.loads(data)

    def receive_delta(self, payload: bytes) -> None:
        """
        Called by peer servers to send us an account delta.
        Merges it into the local store.

        Args:
            payload: Compressed (or uncompressed) delta package from peer
        """
        try:
            message = self._decompress(payload)
            account_id = message.get("account_id")
            delta = message.get("delta")

            if not account_id or not delta:
                log.warning(f"Malformed delta message: {message}")
                return

            # Load local state and merge
            local_account = self.store.get(account_id)
            if local_account:
                local_account.merge_delta(delta)
            else:
                log.warning(f"Received delta for unknown account {account_id}")

            log.debug(f"Merged delta for {account_id} from {message.get('source_node')}")

        except Exception as e:
            log.error(f"Error processing received delta: {e}")
            raise SyncError(f"Failed to merge delta: {e}")

    def receive_full_state(self, payload: bytes) -> None:
        """
        Called by peer servers to send us a full state snapshot (anti-entropy).
        Merges all accounts.

        Args:
            payload: Compressed (or uncompressed) full state from peer
        """
        try:
            full_state_dict = self._decompress(payload)

            for account_id, account_data in full_state_dict.items():
                # Load local and merge full state
                local_account = self.store.get(account_id)
                if local_account:
                    local_account.merge_full(account_data)
                    self.store.put(account_id, local_account)
                else:
                    log.debug(f"Received full state for new account {account_id}")

            log.debug(f"Merged anti-entropy snapshot ({len(full_state_dict)} accounts)")

        except Exception as e:
            log.error(f"Error processing full state: {e}")
            raise SyncError(f"Failed to merge full state: {e}")
