# =============================================================================
# sync_service.py — Replication / Synchronisation Service
# =============================================================================
# Called internally by node_server.py after every successful transaction.
# It connects to every OTHER node via RPC and pushes the latest balance.
# If a remote node is offline, the error is caught, logged, and the local
# transaction is NOT rolled back (availability over strict consistency).
# =============================================================================

import rpyc
from config import NODES, THIS_NODE


def replicate(account_id: str, new_balance: float, account_name: str) -> list[dict]:
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
            conn.root.update_balance(account_id, new_balance, account_name)
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
