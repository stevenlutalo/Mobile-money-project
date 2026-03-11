"""
tools/migrate_accounts.py — Import accounts from legacy systems.

Usage:
  python tools/migrate_accounts.py --input accounts.json --node-id node-1

Reads old account data ({account_id: balance, ...}) and imports into
all nodes in the cluster using concurrent writes.
"""

# — Standard library —
import argparse
import json
import logging
import sys
from pathlib import Path

# — Third-party packages —
from tqdm import tqdm

# — This project —
from storage.shard_store import ShardStore
from core.crdt import PNCounter
from core.consistent_ring import NodeRing

logging.basicConfig(level=logging.INFO)
log = logging.getLogger(__name__)


def migrate_accounts(
    input_file: str,
    node_id: str,
    etcd_host: str = "localhost",
    etcd_port: int = 2379,
):
    """
    Migrate accounts from a JSON file to the distributed cluster.

    Args:
        input_file: Path to JSON file with accounts
        node_id: This server's ID
        etcd_host: etcd hostname
        etcd_port: etcd port
    """
    # Load legacy data
    log.info(f"Reading accounts from {input_file}...")
    with open(input_file, "r") as f:
        legacy_accounts = json.load(f)

    log.info(f"Found {len(legacy_accounts)} accounts to migrate")

    # Connect to ring
    log.info(f"Connecting to cluster at {etcd_host}:{etcd_port}...")
    ring = NodeRing(etcd_host, etcd_port, "mmoney-cluster")

    # Open local store
    db_path = f"data/{node_id}.db"
    log.info(f"Opening database at {db_path}...")
    store = ShardStore(db_path=db_path, node_id=node_id)

    # Migrate accounts
    log.info("Importing accounts...")
    total_balance = 0.0
    failures = []

    for account_id in tqdm(legacy_accounts, desc="Migrating"):
        try:
            balance = legacy_accounts[account_id]
            if not isinstance(balance, (int, float)):
                raise ValueError(f"Invalid balance for {account_id}: {balance}")

            # Determine which node owns this account
            owner = ring.find_owner(account_id)

            # If not ours, skip (other nodes will import their accounts)
            if owner["node_id"] != node_id:
                continue

            # Create CRDT counter and store
            counter = PNCounter(node_id, initial_balance=balance)
            store.put(account_id, counter)
            total_balance += balance

        except Exception as e:
            log.error(f"Failed to migrate {account_id}: {e}")
            failures.append((account_id, str(e)))

    # Summary
    print("\n" + "=" * 60)
    print(f"Migration complete for {node_id}")
    print("=" * 60)
    print(f"Accounts migrated to this node: {len(legacy_accounts) - len(failures)}")
    print(f"Total balance: {total_balance:,.2f}")

    if failures:
        print(f"Failures: {len(failures)}")
        for acc_id, error in failures[:5]:
            print(f"  - {acc_id}: {error}")
        if len(failures) > 5:
            print(f"  ... and {len(failures) - 5} more")

    store.close()


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(
        description="Migrate accounts from legacy system to mmoney cluster"
    )
    parser.add_argument("--input", required=True, help="Path to legacy accounts.json")
    parser.add_argument("--node-id", default="node-1", help="This node's ID")
    parser.add_argument("--etcd-host", default="localhost", help="etcd hostname")
    parser.add_argument("--etcd-port", type=int, default=2379, help="etcd port")

    args = parser.parse_args()

    try:
        if not Path(args.input).exists():
            print(f"✗ Input file not found: {args.input}")
            sys.exit(1)

        migrate_accounts(args.input, args.node_id, args.etcd_host, args.etcd_port)

    except Exception as e:
        print(f"✗ Migration failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
