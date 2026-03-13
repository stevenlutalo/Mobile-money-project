# — Standard library —
import logging
import threading
import time

# — Third-party packages —
import rpyc

# — This project —
# (none)

log = logging.getLogger(__name__)


# Heartbeat monitor — detects when servers go down and notifies the system.
# If a server misses 3 consecutive heartbeats, it's marked as offline.


HEARTBEAT_INTERVAL_SECS = 10
# How often each server sends heartbeats. Lower = faster failure detection,
# but higher network traffic. 10 seconds is a good balance.

HEARTBEAT_TIMEOUT_SECS = 35
# If a server hasn't been heard from in this long, mark it down.
# Should be >= 3× HEARTBEAT_INTERVAL_SECS so transient network issues don't cause false positives.

HEARTBEAT_RETRIES = 3
# How many times to retry heartbeat before giving up.


def run_heartbeat(ring, node_id: str) -> None:
    """
    Periodically send heartbeats to etcd to signal that this node is alive.

    Runs in a background thread. If a node doesn't send a heartbeat within
    HEARTBEAT_TIMEOUT_SECS, it's considered down.

    Args:
        ring: NodeRing instance
        node_id: This server's node ID
    """
    while True:
        try:
            etcd_key = f"/{ring.cluster_name}/heartbeat/{node_id}"
            # Simple heartbeat: just update timestamp
            ring.etcd.put(etcd_key, str(time.time()))
            log.debug("event=heartbeat_put node=%s key=%s", node_id, etcd_key)
            time.sleep(HEARTBEAT_INTERVAL_SECS)
        except Exception as e:
            log.warning("event=heartbeat_put_failed node=%s error=%s retry_in=5s", node_id, e)
            time.sleep(5)


def on_node_down(node_id: str, ring) -> None:
    """
    Called when a node is detected as offline.
    Triggers cluster reconfiguration and notifies peers.

    Args:
        node_id: ID of the node that went down
        ring: NodeRing instance
    """
    log.warning("event=node_down_detected node=%s action=deregister", node_id)
    try:
        ring.deregister_node(node_id)
        log.info("event=node_deregistered node=%s", node_id)
    except Exception as e:
        log.error("event=node_deregister_failed node=%s error=%s", node_id, e)


def watch_ring(ring) -> None:
    """
    Watch etcd heartbeats and detect dead servers.
    Runs in a background thread.

    Args:
        ring: NodeRing instance
    """
    while True:
        try:
            etcd_prefix = f"/{ring.cluster_name}/heartbeat/"
            current_time = time.time()
            live_nodes = set()

            # Check all heartbeat entries in etcd
            for value, metadata in ring.etcd.get_prefix(etcd_prefix):
                try:
                    node_id = metadata.key.decode().split("/")[-1]
                    heartbeat_time = float(value.decode())
                    
                    # Only consider nodes with recent heartbeats as live
                    if current_time - heartbeat_time <= HEARTBEAT_TIMEOUT_SECS:
                        live_nodes.add(node_id)
                    else:
                        # Stale heartbeat - mark node as down
                        age = current_time - heartbeat_time
                        log.warning(
                            "event=heartbeat_stale node=%s age_s=%.1f timeout_s=%s",
                            node_id,
                            age,
                            HEARTBEAT_TIMEOUT_SECS,
                        )
                        on_node_down(node_id, ring)
                        
                except (ValueError, AttributeError) as e:
                    log.debug("event=heartbeat_parse_failed error=%s", e)
                    continue

            # Check for nodes registered but missing heartbeats
            current_nodes = set(ring.nodes.keys())
            for dead_node in current_nodes - live_nodes:
                on_node_down(dead_node, ring)

            time.sleep(HEARTBEAT_INTERVAL_SECS)

        except Exception as e:
            log.error("event=health_watch_failed error=%s retry_in=5s", e)
            time.sleep(5)


class HealthMonitor:
    """
    Monitors cluster health and detects node failures.
    Runs heartbeat and watch threads.

    Why it exists: when a server crashes, other servers need to know immediately
    so they can stop routing requests to it. This monitors heartbeats and
    triggers recovery.

    Usage:
      monitor = HealthMonitor(ring, node_id="node-1")
      monitor.start()
      # ... later ...
      monitor.stop()
    """

    def __init__(self, ring, node_id: str):
        """
        Initialize the health monitor.

        Args:
            ring: NodeRing instance
            node_id: This server's node ID
        """
        self.ring = ring
        self.node_id = node_id
        self._heartbeat_thread = None
        self._watch_thread = None

    def start(self) -> None:
        """Start heartbeat and watch threads."""
        log.info("event=health_monitor_start node=%s interval_s=%s timeout_s=%s", self.node_id, HEARTBEAT_INTERVAL_SECS, HEARTBEAT_TIMEOUT_SECS)

        self._heartbeat_thread = threading.Thread(
            target=run_heartbeat,
            args=(self.ring, self.node_id),
            daemon=True,
            name=f"heartbeat-{self.node_id}"
        )
        self._heartbeat_thread.start()

        self._watch_thread = threading.Thread(
            target=watch_ring,
            args=(self.ring,),
            daemon=True,
            name="health-watch"
        )
        self._watch_thread.start()

    def stop(self) -> None:
        """Stop monitoring (threads are daemons, so they exit with the process)."""
        log.info("event=health_monitor_stop node=%s", self.node_id)
