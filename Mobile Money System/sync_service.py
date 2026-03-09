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
from config import NODES, THIS_NODE


def replicate(account_id: str, new_balance: float, account_name: str, last_updated: float) -> list[dict]:
    """
    Push an updated balance to all nodes except this one.

    Returns a list of result dicts, one per remote node:
        {"node": "ug.mba", "host": "...", "success": True/False, "message": "..."}
    """
    results = []

    for key, node_cfg in NODES.items():
        if key == THIS_NODE:
            continue                         # skip ourselves

        host = node_cfg["host"]
        port = node_cfg["port"]
        name = node_cfg["name"]

        print(f"  [SYNC] Replicating to {name} ({host}:{port})...", end=" ", flush=True)

        try:
            conn = rpyc.connect(
                host, port,
                config={"sync_request_timeout": 5}   # don't hang if node is slow
            )
            conn.root.update_balance(account_id, new_balance, account_name, last_updated)
            conn.close()

            print("Success.")
            print(f"  [{name}] Balance updated: UGX {new_balance:,.0f}")

            results.append({
                "node":    key,
                "host":    host,
                "success": True,
                "message": "Replicated successfully",
            })

        except Exception as exc:
            msg = str(exc)
            print(f"FAILED ({msg})")
            print(f"  [SYNC] WARNING: {name} is offline. "
                  f"Balance will sync when it comes back online.")

            results.append({
                "node":    key,
                "host":    host,
                "success": False,
                "message": msg,
            })

    return results


def sync_all() -> list[dict]:
    """
    Pull accounts from all other nodes and merge into local accounts.
    Uses last_updated timestamps for conflict resolution.
    """
    results = []

    for key, node_cfg in NODES.items():
        if key == THIS_NODE:
            continue

        host = node_cfg["host"]
        port = node_cfg["port"]
        name = node_cfg["name"]

        print(f"  [SYNC-ALL] Pulling accounts from {name} ({host}:{port})...", end=" ", flush=True)

        try:
            conn = rpyc.connect(
                host, port,
                config={"sync_request_timeout": 10}
            )
            remote_accounts = conn.root.list_accounts()
            conn.close()

            # Merge
            accounts_file = os.path.join(os.path.dirname(__file__), "accounts.json")
            with open(accounts_file, "r") as f:
                local_accounts = json.load(f)

            merged = False
            for acc_id, remote_acc in remote_accounts.items():
                if acc_id not in local_accounts:
                    local_accounts[acc_id] = remote_acc
                    merged = True
                elif remote_acc.get("last_updated", 0) > local_accounts[acc_id].get("last_updated", 0):
                    local_accounts[acc_id] = remote_acc
                    merged = True

            if merged:
                with open(accounts_file, "w") as f:
                    json.dump(local_accounts, f, indent=4)
                print("Merged.")
            else:
                print("No changes.")

            results.append({
                "node":    key,
                "host":    host,
                "success": True,
                "message": "Synced successfully",
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
