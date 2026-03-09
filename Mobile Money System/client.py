# =============================================================================
# client.py — Interactive Console Client with Dijkstra Auto-Routing
# =============================================================================
# Run this on any PC on the same LAN.
#
# At startup the client:
#   1. Probes every known node, measuring round-trip latency.
#   2. Asks each reachable node for its own inter-node latency measurements.
#   3. Builds a weighted graph: CLIENT + all nodes as vertices,
#      measured latencies as edge weights.
#   4. Runs Dijkstra's algorithm from "CLIENT" to find the lowest-latency
#      path to every node.
#   5. Automatically connects to the nearest (lowest total latency) node.
#
# No location prompt — routing is fully automatic.
# =============================================================================

import sys
import time
import heapq
import rpyc

from config import NODES


# ---------------------------------------------------------------------------
# Formatting helpers
# ---------------------------------------------------------------------------

def _banner():
    print()
    print("=" * 55)
    print("  DISTRIBUTED MOBILE MONEY SYSTEM")
    print("  Auto-Routing via Dijkstra's Algorithm")
    print("=" * 55)


def _divider():
    print("-" * 55)


def _fmt(amount: float) -> str:
    """Format a number as UGX with comma separators."""
    return f"UGX {amount:,.0f}"


# ---------------------------------------------------------------------------
# Step 1 — Measure client-to-node latency
# ---------------------------------------------------------------------------

def _measure_latency(host: str, port: int, samples: int = 3) -> float | None:
    """
    Open an rpyc connection to (host, port), send `samples` pings, and
    return the minimum round-trip time in milliseconds.
    Returns None if the node is unreachable.
    """
    try:
        conn = rpyc.connect(
            host, port,
            config={"sync_request_timeout": 3},
        )
        times = []
        for _ in range(samples):
            t0 = time.perf_counter()
            conn.root.ping()
            times.append((time.perf_counter() - t0) * 1000.0)
        conn.close()
        return round(min(times), 3)
    except Exception:
        return None


def _scan_client_to_nodes() -> dict:
    """
    Probe every node listed in config.NODES.
    Returns: {"ug.hoi": 8.42, "ug.mba": 12.17, "ug.kam": None, ...}
    Prints one line per node as it probes.
    """
    print("\n[NETWORK SCAN] Probing all nodes from this client...")
    _divider()

    latencies = {}
    for key, node_cfg in NODES.items():
        host = node_cfg["host"]
        port = node_cfg["port"]
        name = node_cfg["name"]
        loc  = node_cfg["location"]

        print(f"  Probing {name} ({loc}) at {host}:{port} ...", end="  ", flush=True)
        lat = _measure_latency(host, port)
        latencies[key] = lat

        if lat is not None:
            print(f"{lat:.3f} ms")
        else:
            print("UNREACHABLE")

    _divider()
    return latencies


# ---------------------------------------------------------------------------
# Step 2 — Fetch inter-node latencies from each reachable node
# ---------------------------------------------------------------------------

def _fetch_neighbour_latencies(client_latencies: dict) -> dict:
    """
    For every node the client can reach, call get_neighbour_latencies()
    to learn that node's measured distances to its own neighbours.

    Returns:
        {
          "A": {"B": 5.83, "C": None},
          "B": {"A": 5.91, "C": 9.12},
          ...
        }
    Only reachable nodes appear as outer keys.
    """
    print("[TOPOLOGY]  Fetching inter-node latency table from each reachable node...")
    _divider()

    nbr_latencies = {}
    for key, client_lat in client_latencies.items():
        if client_lat is None:
            continue                          # can't ask an offline node

        node_cfg = NODES[key]
        host     = node_cfg["host"]
        port     = node_cfg["port"]
        name     = node_cfg["name"]

        try:
            conn = rpyc.connect(
                host, port,
                config={"sync_request_timeout": 5},
            )
            # get_neighbour_latencies() returns a plain dict via rpyc
            raw = conn.root.get_neighbour_latencies()
            # rpyc may return a netref; convert to a plain Python dict
            nbrs = {k: v for k, v in raw.items()}
            conn.close()
            nbr_latencies[key] = nbrs

            parts = []
            for nbr_key, lat in nbrs.items():
                label = f"{NODES[nbr_key]['name']}={lat:.3f}ms" if lat is not None \
                        else f"{NODES[nbr_key]['name']}=OFFLINE"
                parts.append(label)
            print(f"  {name} neighbours: {',  '.join(parts) if parts else 'none'}")

        except Exception as exc:
            print(f"  {name}: could not fetch neighbours ({exc})")

    _divider()
    return nbr_latencies


# ---------------------------------------------------------------------------
# Step 3 — Build weighted graph
# ---------------------------------------------------------------------------

def _build_graph(client_latencies: dict, nbr_latencies: dict) -> dict:
    """
    Construct an adjacency-list weighted graph suitable for Dijkstra.

    Vertices  : "CLIENT", "A", "B", "C", ...
    Edge weight: measured latency in milliseconds (None edges are omitted).

    CLIENT -> node  edges come from client_latencies.
    node   -> node  edges come from nbr_latencies (each reachable node's view).
    """
    graph: dict[str, dict[str, float]] = {"CLIENT": {}}

    # CLIENT -> each node
    for key, lat in client_latencies.items():
        if lat is not None:
            graph["CLIENT"][key] = lat

    # node -> node (as reported by each node)
    for src_key, neighbours in nbr_latencies.items():
        if src_key not in graph:
            graph[src_key] = {}
        for dst_key, lat in neighbours.items():
            if lat is not None:
                graph[src_key][dst_key] = lat

    # Ensure every node key exists in the graph (even if no outgoing edges)
    for key in NODES:
        if key not in graph:
            graph[key] = {}

    return graph


# ---------------------------------------------------------------------------
# Step 4 — Dijkstra's algorithm
# ---------------------------------------------------------------------------

def _dijkstra(graph: dict, start: str) -> tuple[dict, dict]:
    """
    Standard Dijkstra using a min-heap priority queue.

    Parameters
    ----------
    graph : adjacency dict  {vertex: {neighbour: weight, ...}, ...}
    start : source vertex label (e.g. "CLIENT")

    Returns
    -------
    dist : {vertex: shortest_distance_from_start}   (inf if unreachable)
    prev : {vertex: predecessor_vertex}             (None for start / unreachable)
    """
    # Collect all vertices (graph keys + all neighbours referenced)
    all_vertices: set[str] = set(graph.keys())
    for edges in graph.values():
        all_vertices.update(edges.keys())

    dist: dict[str, float] = {v: float("inf") for v in all_vertices}
    prev: dict[str, str | None] = {v: None for v in all_vertices}

    dist[start] = 0.0
    # heap entries: (current_distance, vertex)
    heap = [(0.0, start)]

    while heap:
        d, u = heapq.heappop(heap)

        if d > dist[u]:
            continue                      # stale entry — skip

        for v, weight in graph.get(u, {}).items():
            alt = dist[u] + weight
            if alt < dist[v]:
                dist[v]  = alt
                prev[v]  = u
                heapq.heappush(heap, (alt, v))

    return dist, prev


def _reconstruct_path(prev: dict, target: str) -> list[str]:
    """Walk the predecessor map backwards to produce a source→target path."""
    path = []
    cur  = target
    while cur is not None:
        path.append(cur)
        cur = prev.get(cur)
    path.reverse()
    return path


# ---------------------------------------------------------------------------
# Step 5 — Print graph, Dijkstra results, and choose best node
# ---------------------------------------------------------------------------

def _print_graph(graph: dict) -> None:
    print("[DIJKSTRA]  Weighted graph edges:")
    _divider()
    for src, edges in sorted(graph.items()):
        if not edges:
            print(f"  {src:<8}  (no outgoing edges)")
            continue
        for dst, w in sorted(edges.items(), key=lambda x: x[1]):
            src_label = src if src == "CLIENT" else NODES[src]["name"]
            dst_label = NODES[dst]["name"] if dst in NODES else dst
            print(f"  {src_label:<12}  ->  {dst_label:<12}  {w:.3f} ms")
    _divider()


def _select_nearest_node(client_latencies: dict,
                          nbr_latencies: dict) -> tuple[str, dict]:
    """
    Build graph, run Dijkstra, print results table, and return the key of
    the nearest reachable node plus its node_cfg dict.
    """
    graph = _build_graph(client_latencies, nbr_latencies)
    _print_graph(graph)

    dist, prev = _dijkstra(graph, "CLIENT")

    print("[DIJKSTRA]  Shortest paths from CLIENT:")
    _divider()
    print(f"  {'Node':<14}  {'Total Latency':>14}  Path")
    _divider()

    reachable = {}
    for key in NODES:
        d    = dist.get(key, float("inf"))
        path = _reconstruct_path(prev, key)

        if d == float("inf"):
            latency_str = "UNREACHABLE"
            path_str    = "—"
        else:
            latency_str = f"{d:.3f} ms"
            path_str    = " -> ".join(
                NODES[n]["name"] if n in NODES else n for n in path
            )
            reachable[key] = d

        print(f"  {NODES[key]['name']:<14}  {latency_str:>14}  {path_str}")

    _divider()

    if not reachable:
        return None, None

    best_key = min(reachable, key=lambda k: reachable[k])
    return best_key, NODES[best_key]


# ---------------------------------------------------------------------------
# Auto-connect entry point (replaces the old location prompt)
# ---------------------------------------------------------------------------

def auto_connect() -> tuple:
    """
    Full Dijkstra routing pipeline.
    Scans nodes, fetches topology, runs Dijkstra, connects to nearest node.
    Returns (rpyc_conn, node_cfg_dict) or (None, None) on total failure.
    """
    client_latencies = _scan_client_to_nodes()

    if all(v is None for v in client_latencies.values()):
        print("  [ERROR] All nodes are unreachable. Is the network up?")
        return None, None

    nbr_latencies = _fetch_neighbour_latencies(client_latencies)

    best_key, node_cfg = _select_nearest_node(client_latencies, nbr_latencies)

    if best_key is None:
        print("  [ERROR] Dijkstra found no reachable node.")
        return None, None

    name = node_cfg["name"]
    host = node_cfg["host"]
    port = node_cfg["port"]
    lat  = client_latencies.get(best_key)
    lat_str = f"{lat:.3f} ms" if lat is not None else "via relay"

    print(f"\n[DECISION]  Nearest node: {name}  ({lat_str} total latency)")
    print(f"            Connecting to {name} ({host}:{port})...", end="  ", flush=True)

    try:
        conn = rpyc.connect(
            host, port,
            config={
                "allow_public_attrs":  True,
                "sync_request_timeout": 10,
            },
        )
        conn.root.ping()
        print("Connected.\n")
        return conn, node_cfg

    except Exception as exc:
        print(f"FAILED.\n  [ERROR] {exc}")
        return None, None


# ---------------------------------------------------------------------------
# Menu actions  (unchanged from original)
# ---------------------------------------------------------------------------

def check_balance(conn, node_cfg, account_id: str) -> None:
    result = conn.root.get_balance(account_id)

    _divider()
    if not result["success"]:
        print(f"  [ERROR] {result['message']}")
    else:
        print(f"  Account   : {result['account_id']} — {result['name']}")
        print(f"  Balance   : {_fmt(result['balance'])}")
        print(f"  Served by : {result['node']}")
    _divider()


def do_deposit(conn, node_cfg, account_id: str) -> None:
    try:
        raw    = input("  Enter amount to deposit: UGX ").strip()
        amount = float(raw.replace(",", ""))
    except ValueError:
        print("  [ERROR] Invalid amount.")
        return

    print()
    print(f"  [{node_cfg['name']}] Processing deposit of {_fmt(amount)} to {account_id}...")

    result = conn.root.deposit(account_id, amount)

    _divider()
    if not result["success"]:
        print(f"  [FAILED] {result['message']}")
    else:
        print(f"  [{node_cfg['name']}] {result['message']}")
        print(f"  Account     : {result['account_id']} — {result['name']}")
        print(f"  New Balance : {_fmt(result['new_balance'])}")

        sync_results = result.get("sync_results", [])
        if not sync_results:
            print("  [SYNC] No other nodes configured.")
        for sr in sync_results:
            status = "Success" if sr["success"] else f"OFFLINE — {sr['message']}"
            print(f"  [SYNC] -> {NODES[sr['node']]['name']} ({sr['host']}): {status}")
    _divider()


def do_withdraw(conn, node_cfg, account_id: str) -> None:
    try:
        raw    = input("  Enter amount to withdraw: UGX ").strip()
        amount = float(raw.replace(",", ""))
    except ValueError:
        print("  [ERROR] Invalid amount.")
        return

    print()
    print(f"  [{node_cfg['name']}] Processing withdrawal of {_fmt(amount)} from {account_id}...")

    result = conn.root.withdraw(account_id, amount)

    _divider()
    if not result["success"]:
        print(f"  [FAILED] {result['message']}")
    else:
        print(f"  [{node_cfg['name']}] {result['message']}")
        print(f"  Account     : {result['account_id']} — {result['name']}")
        print(f"  New Balance : {_fmt(result['new_balance'])}")

        sync_results = result.get("sync_results", [])
        if not sync_results:
            print("  [SYNC] No other nodes configured.")
        for sr in sync_results:
            status = "Success" if sr["success"] else f"OFFLINE — {sr['message']}"
            print(f"  [SYNC] -> {NODES[sr['node']]['name']} ({sr['host']}): {status}")
    _divider()


def list_accounts(conn) -> None:
    accounts = conn.root.list_accounts()
    _divider()
    print(f"  {'Account ID':<10}  {'Name':<20}  {'Balance':>15}")
    _divider()
    for acc_id, info in accounts.items():
        print(f"  {acc_id:<10}  {info['name']:<20}  {_fmt(info['balance']):>15}")
    _divider()


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------

def main():
    _banner()

    # -- Auto-routing via Dijkstra (no user location prompt) ----------------
    conn, node_cfg = auto_connect()

    if conn is None:
        print("  Exiting — no node reachable.")
        sys.exit(1)

    # -- Account ID ----------------------------------------------------------
    account_id = input("  Enter Account ID: ").strip().upper()
    if not account_id:
        print("  [ERROR] Account ID cannot be empty.")
        conn.close()
        sys.exit(1)

    probe = conn.root.get_balance(account_id)
    if not probe["success"]:
        print(f"  [ERROR] {probe['message']}")
        conn.close()
        sys.exit(1)

    print(f"  Welcome, {probe['name']}!")
    print(f"  Current balance: {_fmt(probe['balance'])}")
    print(f"  Connected via  : {node_cfg['name']} ({node_cfg['location']})")

    # -- Transaction menu ----------------------------------------------------
    while True:
        print()
        print("  Choose operation:")
        print("    1. Deposit")
        print("    2. Withdraw")
        print("    3. Check Balance")
        print("    4. List All Accounts")
        print("    5. Exit")
        print()

        choice = input("  > ").strip()

        if choice == "1":
            do_deposit(conn, node_cfg, account_id)
        elif choice == "2":
            do_withdraw(conn, node_cfg, account_id)
        elif choice == "3":
            check_balance(conn, node_cfg, account_id)
        elif choice == "4":
            list_accounts(conn)
        elif choice == "5":
            print("\n  Goodbye.\n")
            conn.close()
            break
        else:
            print("  [ERROR] Invalid choice. Enter 1–5.")


if __name__ == "__main__":
    main()
