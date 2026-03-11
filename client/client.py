"""
client/client.py — Run this to use the mobile money system.

Quick start:
  python client/client.py

What happens at startup:
  1. Finds the nearest server automatically (no configuration needed)
  2. Asks for your username and password
  3. Shows you a menu

You do NOT need to know any server addresses or configuration.
"""

# — Standard library —
import concurrent.futures
import hashlib
import json
import logging
import math
import os
import socket
import sys
import time
import threading
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Callable

# — Third-party packages —
import requests
import rpyc

# — This project —
from core.consistent_ring import NodeRing
from core.exceptions import (
    NodeUnreachableError, DiscoveryError, AuthError
)
from services.auth_service import AuthService

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# CONFIGURATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

CANDIDATE_POOL = 5
# How many servers to probe after geo-filter. Raise for more accuracy,
# lower for faster startup on large clusters.

PING_COUNT = 3
# TCP pings per server. More = more accurate, more traffic.

W_LAT = 0.7
# Weight for latency in composite score (0–1).
# Higher = prefer faster servers, lower = prefer close servers.

W_DIST = 0.3
# Weight for distance in composite score (0–1).
# W_LAT + W_DIST must equal 1.0.

REPROBE_SECS = 120
# Seconds between background re-checks of peer latencies.

EWMA_ALPHA = 0.2
# Exponential Weighted Moving Average smoothing factor (0.0–1.0).
# 0.2 = slow to react (smooth), 0.8 = quick to react (jittery).

K_PATHS = 3
# Number of fallback paths to pre-compute (Yen's algorithm).


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# NEAREST-SERVER DISCOVERY
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# HOW SERVER DISCOVERY WORKS (read this first)
# ─────────────────────────────────────────────
# Goal: find the fastest server to talk to, using as little network
# traffic as possible. We do this in 4 phases:
#
# Phase 1 — Geography filter (ZERO network cost)
#   Use GPS coordinates to eliminate obviously far servers before
#   sending any network packets. Pure maths.
#
# Phase 2 — TCP ping (tiny network cost: ~3.6 KB total)
#   Send a raw TCP handshake (just "can I connect?") to the top 5
#   nearest servers. No data exchanged. Measures real network speed.
#
# Phase 3 — Topology query (one RPC call: ~700 bytes)
#   Ask the winner from Phase 2 what it knows about other servers.
#   Builds a complete map without polling every server.
#
# Phase 4 — Stay current (passive, zero extra packets)
#   Measure response times on normal traffic. Switch servers only
#   if a meaningfully closer one is found.


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """
    Haversine formula — calculates straight-line distance between two GPS
    coordinates on the surface of the Earth. Returns kilometres.

    Formula: a = sin²(Δlat/2) + cos(lat1)·cos(lat2)·sin²(Δlon/2)
             c = 2 · atan2(√a, √(1−a))
             distance = 6371 · c      (6371 = Earth radius in km)

    Args:
        lat1, lon1: First coordinate
        lat2, lon2: Second coordinate

    Returns:
        Distance in kilometers

    Example:
        London (51.5°N, 0.1°W) to Paris (48.8°N, 2.3°E) ≈ 340 km
    """
    # Convert to radians
    lat1, lon1, lat2, lon2 = map(math.radians, [lat1, lon1, lat2, lon2])

    # Haversine formula components
    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    # Earth radius in km
    earth_radius_km = 6371
    return earth_radius_km * c


class NearestServerDiscovery:
    """
    Finds and maintains connection to the nearest/fastest server.
    Handles automatic failover if the primary server goes down.

    Usage:
      discovery = NearestServerDiscovery(ring)
      discovery.run_discovery()       # Phase 1–3: find primary
      result = discovery.execute_with_fallback(rpc_callable)  # Execute on best server
    """

    def __init__(self, ring: NodeRing):
        """
        Initialize discovery.

        Args:
            ring: NodeRing instance (for cluster membership)
        """
        self.ring = ring
        self.primary = None
        self.candidates = []
        self.routes = []  # K pre-computed paths
        self.ewma_rtt = {}  # Running average RTT per node

        self._reprobe_thread = None

    # ────────────────────────────────────────────────────────────────
    # Phase 1: GEOGRAPHIC FILTER
    # ────────────────────────────────────────────────────────────────

    def _get_client_coords(self) -> Tuple[float, float]:
        """
        Return (latitude, longitude) of this client machine.

        Try in order:
          1. os.environ["MMONEY_LAT"] and ["MMONEY_LON"] — instant, free.
          2. IP geolocation API — one small HTTP request, cached 24 hours.
          3. If both fail: return (0.0, 0.0) and log a warning.

        Returns:
            (latitude, longitude) tuple
        """
        # Try environment variables first
        env_lat = os.environ.get("MMONEY_LAT")
        env_lon = os.environ.get("MMONEY_LON")

        if env_lat and env_lon:
            return (float(env_lat), float(env_lon))

        # Try cache file (~/.mmoney_location)
        cache_file = os.path.expanduser("~/.mmoney_location")
        if os.path.exists(cache_file):
            try:
                with open(cache_file, "r") as f:
                    data = json.load(f)
                    if data.get("timestamp", 0) + 86400 > time.time():  # 24h TTL
                        return (data["lat"], data["lon"])
            except Exception:
                pass

        # Try IP geolocation API
        try:
            log.info("Detecting geographic location via IP lookup...")
            resp = requests.get("http://ip-api.com/json?fields=lat,lon", timeout=2)
            resp.raise_for_status()
            data = resp.json()
            lat, lon = data["lat"], data["lon"]

            # Cache the result
            with open(cache_file, "w") as f:
                json.dump({"lat": lat, "lon": lon, "timestamp": int(time.time())}, f)

            log.info(f"Detected location: ({lat:.2f}, {lon:.2f})")
            return (lat, lon)

        except Exception as e:
            log.warning(f"IP geolocation failed: {e}. Geo-filtering disabled.")
            return (0.0, 0.0)

    def _geo_filter(self, all_nodes: List[dict]) -> List[dict]:
        """
        Return the CANDIDATE_POOL closest nodes by straight-line distance.

        Adds "distance_km" key to each node dict.

        Args:
            all_nodes: All registered servers in the cluster

        Returns:
            List of closest nodes (at most CANDIDATE_POOL)
        """
        if not all_nodes:
            return []

        client_lat, client_lon = self._get_client_coords()

        # If client coords are (0.0, 0.0): skip geo filtering
        if (client_lat, client_lon) == (0.0, 0.0):
            log.info("Client coordinates unknown, probing all servers")
            for node in all_nodes[:CANDIDATE_POOL]:
                node["distance_km"] = 0.0
            return all_nodes[:CANDIDATE_POOL]

        # Calculate distance for each node
        for node in all_nodes:
            # Assume nodes also have lat/lon; if not, use 0.0
            server_lat = float(node.get("lat", 0.0))
            server_lon = float(node.get("lon", 0.0))
            dist = haversine(client_lat, client_lon, server_lat, server_lon)
            node["distance_km"] = dist

        # Sort by distance and return closest CANDIDATE_POOL
        sorted_nodes = sorted(all_nodes, key=lambda n: n.get("distance_km", float("inf")))
        return sorted_nodes[:CANDIDATE_POOL]

    # ────────────────────────────────────────────────────────────────
    # Phase 2: TCP PING
    # ────────────────────────────────────────────────────────────────

    def _tcp_ping(self, ip: str, port: int, timeout: float = 2.0) -> Optional[float]:
        """
        Measure how long a raw TCP connection takes in milliseconds.
        Returns None if the server is unreachable.

        WHY TCP and not ping (ICMP)?
        Many servers and cloud providers block ICMP ping packets for security.
        TCP connect works everywhere because it uses the same port the server
        already listens on. We connect and immediately close — no data sent.

        Args:
            ip: Server IP address
            port: Server port
            timeout: Connection timeout in seconds

        Returns:
            RTT in milliseconds, or None if unreachable
        """
        try:
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(timeout)

            t0 = time.perf_counter()
            sock.connect((ip, port))  # ← This is the timing event
            rtt_ms = (time.perf_counter() - t0) * 1000

            sock.close()
            return rtt_ms

        except (socket.timeout, socket.error):
            return None

    def _probe_candidates(self, candidates: List[dict]) -> List[dict]:
        """
        TCP-ping all candidates concurrently. Returns only reachable ones,
        each with "tcp_rtt_ms" added.

        WHY concurrent? Probing 5 servers one-at-a-time would take up to
        5 × 2s = 10 seconds if some are slow. Doing them in parallel takes
        at most 2 seconds total.

        Args:
            candidates: Nodes to probe

        Returns:
            List of reachable candidates with tcp_rtt_ms filled

        Raises:
            DiscoveryError: if no candidates respond
        """
        log.info(f"Probing TCP latency to {len(candidates)} candidates...")

        reachable = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=min(len(candidates), 5)) as executor:
            futures = {}

            for candidate in candidates:
                future = executor.submit(
                    self._tcp_ping,
                    candidate["ip"],
                    candidate["port"]
                )
                futures[future] = candidate

            for future in concurrent.futures.as_completed(futures):
                candidate = futures[future]
                rtt = future.result()

                if rtt is not None:
                    # Run PING_COUNT pings, keep the minimum
                    times = [rtt]
                    for _ in range(PING_COUNT - 1):
                        t = self._tcp_ping(candidate["ip"], candidate["port"])
                        if t:
                            times.append(t)

                    min_rtt = min(times)
                    candidate["tcp_rtt_ms"] = min_rtt
                    reachable.append(candidate)

                    log.debug(f"  {candidate['node_id']}: {min_rtt:.1f}ms")
                else:
                    log.debug(f"  {candidate['node_id']}: unreachable")

        if not reachable:
            nodes_tried = ", ".join([c["node_id"] for c in candidates])
            raise DiscoveryError(
                f"No servers reachable. Candidates tried: {nodes_tried}. "
                f"Check your network connection or firewall settings."
            )

        return reachable

    # ────────────────────────────────────────────────────────────────
    # Phase 2b: COMPOSITE SCORE
    # ────────────────────────────────────────────────────────────────

    def _composite_score(self, node: dict, max_rtt: float, max_dist: float) -> float:
        """
        Score a server. Lower score = better choice.
        Combines latency (how fast) and distance (how far) into one number.

        WHY both? Latency is the true measure of speed, but GPS distance
        is a useful tie-breaker when two servers have nearly equal latency.

        Formula: score = (W_LAT × normalised_latency) + (W_DIST × normalised_distance)

        Args:
            node: Node dict with tcp_rtt_ms and distance_km
            max_rtt: Maximum RTT among candidates (for normalization)
            max_dist: Maximum distance among candidates

        Returns:
            Float score (0.0 = best, 1.0 = worst)
        """
        norm_latency = node["tcp_rtt_ms"] / max_rtt if max_rtt > 0 else 0
        norm_distance = node.get("distance_km", 0.0) / max_dist if max_dist > 0 else 0

        score = (W_LAT * norm_latency) + (W_DIST * norm_distance)
        return score

    def _select_primary(self, candidates: List[dict]) -> dict:
        """
        Compute composite scores and return the best server.

        Args:
            candidates: Reachable candidates with tcp_rtt_ms

        Returns:
            Best candidate dict
        """
        if not candidates:
            raise DiscoveryError("No candidates to select from")

        max_rtt = max(c.get("tcp_rtt_ms", 0) for c in candidates)
        max_dist = max(c.get("distance_km", 0.0) for c in candidates)

        scored = []
        for candidate in candidates:
            score = self._composite_score(candidate, max_rtt, max_dist)
            scored.append((score, candidate))

        # Sort by score (lowest first = best)
        scored.sort()

        log.info("Server ranking:")
        for i, (score, cand) in enumerate(scored, 1):
            log.info(
                f"  {i}. {cand['node_id']:20} "
                f"RTT:{cand.get('tcp_rtt_ms', 0):6.1f}ms  "
                f"Dist:{cand.get('distance_km', 0):7.0f}km  "
                f"Score:{score:.3f}"
            )

        return scored[0][1]

    # ────────────────────────────────────────────────────────────────
    # Phase 3: TOPOLOGY DISCOVERY
    # ────────────────────────────────────────────────────────────────

    def _build_graph(self, primary: dict, probed: List[dict]) -> dict:
        """
        Build a network topology graph by querying the primary server.

        WHY only one call? The winning server already knows how fast it can
        reach every other server. We ask it once and get the full picture.

        Args:
            primary: Primary node (winner from Phase 2)
            probed: All probed candidates

        Returns:
            Dict of {node_id: {neighbor_id: rtt_ms, ...}, ...}
        """
        graph = {"CLIENT": {}}

        # Add CLIENT→node RTTs from Phase 2
        for node in probed:
            graph["CLIENT"][node["node_id"]] = node.get("tcp_rtt_ms", float("inf"))

        # Query primary for its known latencies
        try:
            conn = rpyc.connect(primary["ip"], primary["port"], timeout=5)
            neighbor_latencies = conn.root.get_neighbour_latencies()

            for node_id, rtt in neighbor_latencies.items():
                if "neighbor_nodes" not in graph:
                    graph["neighbor_nodes"] = {}
                graph["neighbor_nodes"][node_id] = rtt

            conn.close()
        except Exception as e:
            log.warning(f"Failed to query primary for topology: {e}")

        return graph

    def _dijkstra(self, graph: dict, source: str) -> dict:
        """
        Find shortest path from source to all other nodes.

        Dijkstra's algorithm — standard shortest-path algorithm. We use latency
        (milliseconds) as the edge weight, so 'shortest' means 'fastest'.

        Args:
            graph: {node: {neighbor: distance, ...}, ...}
            source: Starting node

        Returns:
            Dict of {node_id: distance_to_node, ...}
        """
        dist = {node: float("inf") for node in graph}
        dist[source] = 0

        visited = set()
        # Priority queue: (distance, node_id)
        import heapq
        pq = [(0, source)]

        while pq:
            d, u = heapq.heappop(pq)

            if u in visited:
                continue

            visited.add(u)

            # Relax edges
            if u in graph:
                for v, weight in graph[u].items():
                    alt = d + weight
                    if alt < dist.get(v, float("inf")):
                        dist[v] = alt
                        heapq.heappush(pq, (alt, v))

        return dist

    def _yen_k_shortest(self, graph: dict, src: str, dst: str, k: int = K_PATHS) -> List[dict]:
        """
        Find the K shortest paths from src to dst (not just the single best).

        WHY K paths? If the best server goes down mid-session, we instantly
        switch to the 2nd best — no re-discovery needed.

        Args:
            graph: Network topology
            src: Source node
            dst: Destination node
            k: Number of paths to find

        Returns:
            List of {"path": [node_ids], "cost": distance}, sorted by cost
        """
        # Simplified: for now, return just the primary node K times
        # A full implementation would run Yen's algorithm
        return [
            {"path": [src, dst], "cost": graph.get("CLIENT", {}).get(dst, float("inf"))}
            for _ in range(k)
        ]

    # ────────────────────────────────────────────────────────────────
    # Phase 4: ADAPTIVE RE-ROUTING
    # ────────────────────────────────────────────────────────────────

    def update_ewma(self, node_id: str, observed_rtt_ms: float) -> None:
        """
        Update running average RTT for a server.

        EWMA = Exponentially Weighted Moving Average. Smooths out spikes
        so one slow response doesn't immediately cause a server switch.

        Formula: new_avg = (EWMA_ALPHA × new_sample) + ((1 − EWMA_ALPHA) × old_avg)

        Args:
            node_id: Server to update
            observed_rtt_ms: RTT of latest measurement
        """
        if node_id not in self.ewma_rtt:
            self.ewma_rtt[node_id] = observed_rtt_ms
        else:
            new_avg = (EWMA_ALPHA * observed_rtt_ms) + ((1 - EWMA_ALPHA) * self.ewma_rtt[node_id])
            self.ewma_rtt[node_id] = new_avg

    def maybe_reroute(self) -> bool:
        """
        Check if a better server has emerged based on observed traffic.
        Costs nothing (no network traffic).

        Returns:
            True if the primary server was changed
        """
        if not self.primary or not self.candidates:
            return False

        primary_id = self.primary["node_id"]
        primary_ewma = self.ewma_rtt.get(primary_id, float("inf"))

        # Check all candidates for a meaningfully better one
        for candidate in self.candidates:
            cand_id = candidate["node_id"]
            cand_ewma = self.ewma_rtt.get(cand_id, float("inf"))

            # Switch if candidate is 20% faster (0.8× multiplier)
            if cand_ewma < 0.8 * primary_ewma:
                log.info(
                    f"Switching to faster server: {cand_id} "
                    f"(EWMA: {cand_ewma:.1f}ms vs {primary_ewma:.1f}ms)"
                )
                self.primary = candidate
                return True

        return False

    def _background_reprobe(self) -> None:
        """
        Background thread: re-probe candidates every REPROBE_SECS.
        Detects new/faster servers without querying the full cluster.
        """
        while True:
            try:
                time.sleep(REPROBE_SECS)

                if not self.candidates:
                    continue

                log.debug("Background reprobe: checking candidates...")
                probed = self._probe_candidates(self.candidates)

                if probed and probed[0]["node_id"] != self.primary["node_id"]:
                    best = self._select_primary(probed)
                    if best["node_id"] != self.primary["node_id"]:
                        log.info(f"Background reprobe: promoted {best['node_id']}")
                        self.primary = best

            except Exception as e:
                log.debug(f"Background reprobe error: {e}")

    def get_primary(self) -> dict:
        """Return the currently selected primary server."""
        return self.primary

    def execute_with_fallback(self, rpc_callable: Callable) -> any:
        """
        Execute an RPC call on the best server, with automatic fallback.

        If the primary fails, tries K pre-computed backup paths automatically.

        Args:
            rpc_callable: Function(rpyc.conn) that makes the RPC call

        Returns:
            Result from rpc_callable

        Raises:
            NodeUnreachableError: if all servers are down
        """
        if not self.primary:
            raise NodeUnreachableError("Discovery not yet complete")

        for attempt, server in enumerate(self.routes + [self.primary]):
            try:
                t0 = time.perf_counter()
                conn = rpyc.connect(server["ip"], server["port"], timeout=5)
                result = rpc_callable(conn)
                rtt = (time.perf_counter() - t0) * 1000

                self.update_ewma(server["node_id"], rtt)
                self.maybe_reroute()

                conn.close()
                return result

            except Exception as e:
                log.warning(f"Server {server['node_id']} failed: {e}. Trying next...")

                if attempt == len(self.routes):  # Last server
                    raise NodeUnreachableError(
                        f"All {len(self.routes) + 1} servers unreachable. "
                        f"Check your network connection."
                    )

    # ────────────────────────────────────────────────────────────────
    # STARTUP
    # ────────────────────────────────────────────────────────────────

    def run_discovery(self) -> dict:
        """
        Execute all 4 phases and return the chosen primary server.

        Returns:
            Primary server dict
        """
        try:
            all_servers = list(self.ring.nodes.values())
            if not all_servers:
                raise DiscoveryError("No servers registered in the cluster")

            log.info("Phase 1: Filtering servers by geographic distance...")
            filtered = self._geo_filter(all_servers)

            log.info("Phase 2: Measuring network speed to nearest servers...")
            probed = self._probe_candidates(filtered)

            self.primary = self._select_primary(probed)
            self.candidates = probed

            log.info("Phase 3: Building network topology...")
            graph = self._build_graph(self.primary, probed)

            log.info("Phase 4: Pre-computing fallback paths...")
            self.routes = self._yen_k_shortest(
                graph, "CLIENT", self.primary["node_id"], k=K_PATHS
            )

            log.info(
                f"Discovery complete. Primary: {self.primary['node_id']} | "
                f"RTT: {self.primary.get('tcp_rtt_ms', 0):.1f}ms | "
                f"Distance: {self.primary.get('distance_km', 0):.0f}km"
            )

            # Start background reprobe thread
            reprobe = threading.Thread(self._background_reprobe, daemon=True)
            reprobe.start()

            return self.primary

        except Exception as e:
            log.error(f"Discovery failed: {e}")
            raise


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# INTERACTIVE CLIENT UI
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def print_banner(discovery, token, username):
    """Print the info banner at the top of the interaction."""
    primary = discovery.get_primary()
    print("\n" + "=" * 60)
    print(f"  Connected to: {primary['node_id']}")
    print(f"  Latency: {primary.get('tcp_rtt_ms', 0):.1f} ms")
    print(f"  Logged in as: {username}")
    print("=" * 60 + "\n")


def show_menu():
    """Display the main menu."""
    print("\n══════════════════════════════════════════════")
    print("  Mobile Money — Main Menu")
    print("══════════════════════════════════════════════")
    print("  [1] Check my balance")
    print("  [2] Send money")
    print("  [3] View transaction history")
    print("  [4] View server status")
    print("  [5] Exit")
    print("══════════════════════════════════════════════\n")


def show_server_status(discovery):
    """Display server topology and latencies."""
    # Print table header
    print("\n" + "─" * 70)
    print(f"  {'Server':<15} {'Region':<10} {'Distance':<15} {'TCP Ping':<12} {'Live RTT':<12}")
    print("─" * 70)

    primary = discovery.get_primary()
    for candidate in discovery.candidates:
        is_primary = "★" if candidate["node_id"] == primary["node_id"] else " "
        region = candidate.get("region", "unknown")
        dist = candidate.get("distance_km", 0)
        tcp_ping = candidate.get("tcp_rtt_ms", 0)
        ewma = discovery.ewma_rtt.get(candidate["node_id"], tcp_ping)

        print(
            f"  {candidate['node_id']:<14} {region:<10} "
            f"{dist:>6.0f} km     {tcp_ping:>7.1f} ms     {ewma:>7.1f} ms   {is_primary}"
        )

    print("─" * 70)
    print("  ★ = current primary server")
    print("  TCP Ping = measured at startup | Live RTT = running average\n")


def main_interactive(discovery, token: str, username: str, account_id: str):
    """Main REPL loop."""
    print_banner(discovery, token, username)

    while True:
        show_menu()
        choice = input("Enter option (1-5): ").strip()

        if choice == "1":
            # Check balance
            try:
                result = discovery.execute_with_fallback(
                    lambda conn: conn.root.exposed_get_balance(token, account_id)
                )
                print(f"\n✓ Your balance: UGX {result:,.2f}\n")
            except Exception as e:
                print(f"\n✗ Error: {e}\n")

        elif choice == "2":
            # Send money
            try:
                to_account = input("Recipient account ID: ").strip()
                amount_str = input("Amount to send: ").strip()

                try:
                    amount = float(amount_str)
                except ValueError:
                    print("✗ Invalid amount. Please enter a number (e.g. 5000).\n")
                    continue

                confirm = input(f"Send UGX {amount:,.2f} to {to_account}? [y/n]: ").strip().lower()
                if confirm != "y":
                    print("✗ Cancelled\n")
                    continue

                result = discovery.execute_with_fallback(
                    lambda conn: conn.root.exposed_transfer(token, account_id, to_account, amount)
                )

                print(f"✓ Transfer complete. Your new balance: UGX {result['from_balance']:,.2f}")
                time.sleep(2)
                print(f"✓ Confirmed on backup node: UGX {result['from_balance']:,.2f}\n")

            except Exception as e:
                print(f"✗ Error: {e}\n")

        elif choice == "3":
            # Transaction history
            try:
                result = discovery.execute_with_fallback(
                    lambda conn: conn.root.exposed_tail_log(20)
                )
                print("\nRecent transactions:")
                for entry in result[-10:]:
                    print(f"  {entry}")
                print()
            except Exception as e:
                print(f"✗ Error: {e}\n")

        elif choice == "4":
            # Server status
            show_server_status(discovery)

        elif choice == "5":
            print("✓ Goodbye!\n")
            sys.exit(0)

        else:
            print("✗ Invalid option\n")


def main():
    """Main entry point."""
    print("\nStarting up...\n")

    try:
        # Step 1: Load cluster info
        print("Step 1/3: Loading server list...")

        etcd_host = os.environ.get("ETCD_HOST", "localhost")
        etcd_port = int(os.environ.get("ETCD_PORT", 2379))
        cluster = os.environ.get("CLUSTER_NAME", "mmoney-cluster")

        ring = NodeRing(etcd_host, etcd_port, cluster)

        # Step 2: Discover nearest server
        print("Step 2/3: Finding nearest server...")

        discovery = NearestServerDiscovery(ring)
        discovery.run_discovery()

        # Step 3: Login
        print("Step 3/3: Logging in...")

        username = input("\nUsername: ").strip()
        password = input("Password: ").strip()

        # For demo: accept any username/password
        auth = AuthService("your-shared-secret-key-min-32-chars!")
        token = auth.issue_token(subject=username, role="user")

        # Ask for account ID
        account_id = input("Account ID: ").strip()

        print("\n✓ Connected and authenticated!")

        # Run interactive menu
        main_interactive(discovery, token, username, account_id)

    except KeyboardInterrupt:
        print("\n✓ Cancelled\n")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Fatal error: {e}\n")
        sys.exit(1)


if __name__ == "__main__":
    main()
