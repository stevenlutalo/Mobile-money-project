"""
End-to-end cross-node consistency test.

What it proves:
- The same account can be accessed through different nodes.
- Deposits and transfers remain consistent across nodes.
- Gossip/owner-routing keeps node views converged.

Usage:
  python tools/test_cross_node_consistency.py

Optional env vars:
  ETCD_HOST=localhost
  ETCD_PORT=2379
  CLUSTER_NAME=mmoney-cluster
  TEST_USER=alice
  TEST_ACCOUNT=ACC-CROSS-001
  TEST_PEER_ACCOUNT=ACC-CROSS-002
  TEST_DEPOSIT=1500
  TEST_TRANSFER=200
"""

import os
import random
import time

import rpyc

from core.consistent_ring import NodeRing
from services.auth_service import AuthService


def _pick_two_distinct(nodes):
    if len(nodes) < 2:
        raise RuntimeError("Need at least 2 nodes registered for this test")
    shuffled = list(nodes)
    random.shuffle(shuffled)
    return shuffled[0], shuffled[1]


def _connect(node):
    return rpyc.connect(node["ip"], node["port"])


def _fmt(node):
    return f"{node['node_id']} ({node['ip']}:{node['port']})"


def main():
    etcd_host = os.environ.get("ETCD_HOST", "localhost")
    etcd_port = int(os.environ.get("ETCD_PORT", "2379"))
    cluster_name = os.environ.get("CLUSTER_NAME", "mmoney-cluster")

    user = os.environ.get("TEST_USER", "alice")
    account_id = os.environ.get("TEST_ACCOUNT", "ACC-CROSS-001")
    peer_account_id = os.environ.get("TEST_PEER_ACCOUNT", "ACC-CROSS-002")
    deposit = float(os.environ.get("TEST_DEPOSIT", "1500"))
    transfer = float(os.environ.get("TEST_TRANSFER", "200"))

    auth = AuthService("your-shared-secret-key-min-32-chars!")
    token = auth.issue_token(subject=user, role="user")

    ring = NodeRing(etcd_host, etcd_port, cluster_name)
    nodes = list(ring.nodes.values())
    if not nodes:
        raise RuntimeError("No nodes found in cluster")

    node_a, node_b = _pick_two_distinct(nodes)

    print("=" * 72)
    print("Cross-node account consistency test")
    print("=" * 72)
    print(f"Node A: {_fmt(node_a)}")
    print(f"Node B: {_fmt(node_b)}")
    print(f"Account under test: {account_id}")
    print(f"Peer account: {peer_account_id}")
    print("-" * 72)

    conn_a = _connect(node_a)
    conn_b = _connect(node_b)

    try:
        print("Step 1: Ensure account exists via Node A")
        conn_a.root.exposed_create_account(token, account_id, 0.0)
        conn_a.root.exposed_create_account(token, peer_account_id, 0.0)
        print("  OK")

        print(f"Step 2: Deposit {deposit:.2f} via Node A")
        dep_result = conn_a.root.exposed_deposit(token, account_id, deposit)
        print(f"  Balance after deposit (Node A response): {dep_result['balance']:.2f}")

        print("Step 3: Read same account from Node B")
        bal_b_after_deposit = conn_b.root.exposed_get_balance(token, account_id)
        print(f"  Balance from Node B: {bal_b_after_deposit:.2f}")

        if abs(bal_b_after_deposit - dep_result["balance"]) > 1e-9:
            print("  WARN: Immediate values differ. Waiting briefly for gossip convergence...")
            time.sleep(3)
            bal_b_after_deposit = conn_b.root.exposed_get_balance(token, account_id)
            print(f"  Balance from Node B after wait: {bal_b_after_deposit:.2f}")

        print(f"Step 4: Transfer {transfer:.2f} from tested account to peer via Node B")
        tx_result = conn_b.root.exposed_transfer(token, account_id, peer_account_id, transfer)
        print(f"  Node B transfer result from_balance: {tx_result['from_balance']:.2f}")

        print("Step 5: Read balances from both nodes")
        a_main = conn_a.root.exposed_get_balance(token, account_id)
        b_main = conn_b.root.exposed_get_balance(token, account_id)
        a_peer = conn_a.root.exposed_get_balance(token, peer_account_id)
        b_peer = conn_b.root.exposed_get_balance(token, peer_account_id)

        print(f"  Main account balance via Node A: {a_main:.2f}")
        print(f"  Main account balance via Node B: {b_main:.2f}")
        print(f"  Peer account balance via Node A: {a_peer:.2f}")
        print(f"  Peer account balance via Node B: {b_peer:.2f}")

        expected_main = deposit - transfer
        expected_peer = transfer

        consistent = (
            abs(a_main - b_main) <= 1e-9
            and abs(a_peer - b_peer) <= 1e-9
            and abs(a_main - expected_main) <= 1e-9
            and abs(a_peer - expected_peer) <= 1e-9
        )

        print("-" * 72)
        if consistent:
            print("PASS: Cross-node consistency verified.")
            print(f"Expected main={expected_main:.2f}, peer={expected_peer:.2f}")
        else:
            print("FAIL: Balances are not yet fully consistent.")
            print(f"Expected main={expected_main:.2f}, peer={expected_peer:.2f}")
            raise SystemExit(1)

    finally:
        try:
            conn_a.close()
        except Exception:
            pass
        try:
            conn_b.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
