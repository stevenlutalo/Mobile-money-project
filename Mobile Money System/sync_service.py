# =============================================================================
# sync_service.py — Replication / Synchronisation Service
# =============================================================================
# Called internally by node_server.py after every successful transaction.
# It connects to every OTHER node via RPC and pushes the latest balance.
# If a remote node is offline, the error is caught, logged, and the local
# transaction is NOT rolled back (availability over strict consistency).
# =============================================================================

import rpyc
import json
import os
import time
from config import NODES, THIS_NODE
import accounts_db

# Initialize database
db_instance = accounts_db.AccountsDB()


def replicate_transaction(tx: dict) -> list[dict]:
    """
    Replicate a transaction to all other nodes using vector clocks.
    """
    results = []

    for key, node_cfg in NODES.items():
        if key == THIS_NODE:
            continue

        host = node_cfg["host"]
        port = node_cfg["port"]
        name = node_cfg["name"]

        print(f"  [SYNC] Replicating transaction to {name} ({host}:{port})...", end=" ", flush=True)

        try:
            conn = rpyc.connect(
                host, port,
                config={"sync_request_timeout": 5}
            )
            result = conn.root.apply_transaction(tx)
            conn.close()

            if result["success"]:
                print("Success.")
            else:
                print(f"Ignored ({result['message']})")

            results.append({
                "node":    key,
                "host":    host,
                "success": result["success"],
                "message": result["message"],
            })

        except Exception as exc:
            msg = str(exc)
            print(f"FAILED ({msg})")
            results.append({
                "node":    key,
                "host":    host,
                "success": False,
                "message": msg,
            })

    return results


def sync_all() -> list[dict]:
    """
    Incremental sync: pull pending transactions from other nodes.
    """
    results = []
    last_sync = float(time.time() - 3600)  # Last hour for safety

    for key, node_cfg in NODES.items():
        if key == THIS_NODE:
            continue

        host = node_cfg["host"]
        port = node_cfg["port"]
        name = node_cfg["name"]

        print(f"  [SYNC-ALL] Pulling transactions from {name} ({host}:{port})...", end=" ", flush=True)

        try:
            conn = rpyc.connect(
                host, port,
                config={"sync_request_timeout": 10}
            )
            # Assume we add exposed_get_pending_transactions
            pending_txs = conn.root.get_pending_transactions(last_sync)
            conn.close()

            applied = 0
            for tx in pending_txs:
                if db_instance.apply_transaction(tx):
                    applied += 1

            print(f"Applied {applied} transactions.")
            results.append({
                "node":    key,
                "host":    host,
                "success": True,
                "message": f"Synced {applied} transactions",
            })

        except Exception as exc:
            msg = str(exc)
            print(f"FAILED ({msg})")
            results.append({
                "node":    key,
                "host":    host,
                "success": False,
                "message": msg,
            })

    return results
