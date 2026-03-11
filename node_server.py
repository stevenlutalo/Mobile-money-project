# =============================================================================
# node_server.py — Distributed Mobile Money RPC Server
# =============================================================================
# Run this on EACH PC (ug.hoi, ug.mba, or ug.kam).  It:
#   - Loads accounts from accounts.json on startup.
#   - Exposes deposit / withdraw / get_balance over rpyc RPC.
#   - After every successful transaction, calls sync_service.replicate()
#     to push the new balance to ALL other nodes.
#   - Exposes get_neighbour_latencies() so the client's Dijkstra router
#     can discover inter-node latencies and build the full network graph.
#   - Uses ThreadedServer so multiple clients can connect simultaneously.
# =============================================================================

import json
import os
import threading
import datetime
import time
import socket
import rpyc
from rpyc.utils.server import ThreadedServer

from config import NODES, THIS_NODE
import sync_service
import accounts_db

# Initialize database
db_instance = accounts_db.AccountsDB()

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR      = os.path.dirname(os.path.abspath(__file__))
ACCOUNTS_FILE = os.path.join(BASE_DIR, "accounts.json")
LOG_FILE      = os.path.join(BASE_DIR, "transactions.log")

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _log(message: str) -> None:
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{timestamp}] {message}"
    print(line)
    with open(LOG_FILE, "a") as f:
        f.write(line + "\n")


# ---------------------------------------------------------------------------
# RPC Service
# ---------------------------------------------------------------------------

class MobileMoneyService(rpyc.Service):
    """
    All methods prefixed with `exposed_` are callable by remote clients.
    A single file-level lock serialises writes so concurrent clients can't
    corrupt accounts.json.
    """

    _lock = threading.Lock()
    _node_name = NODES[THIS_NODE]["name"]

    # ------------------------------------------------------------------
    # exposed_ping  — health-check used by client and sync_service
    # ------------------------------------------------------------------
    def exposed_ping(self) -> str:
        return f"PONG from {self._node_name}"

    # ------------------------------------------------------------------
    # exposed_get_neighbour_latencies
    #   Called by the client's Dijkstra router to learn this node's
    #   measured round-trip latency to every other node.
    #
    #   Process per neighbour:
    #     1. Open an rpyc connection (2 s timeout).
    #     2. Send 3 back-to-back pings and record each round-trip time.
    #     3. Return the minimum (best-case path latency) in milliseconds.
    #     4. If the neighbour is unreachable return None for that key.
    #
    #   Returns: {"ug.mba": 5.83, "ug.kam": None, ...}  (excludes THIS_NODE itself)
    # ------------------------------------------------------------------
    def exposed_get_neighbour_latencies(self) -> dict:
        results = {}

        for key, node_cfg in NODES.items():
            if key == THIS_NODE:
                continue                      # don't measure ourselves

            host = node_cfg["host"]
            port = node_cfg["port"]

            try:
                conn = rpyc.connect(
                    host, port,
                    config={"sync_request_timeout": 2},
                )

                samples = []
                for _ in range(3):
                    t0 = time.perf_counter()
                    conn.root.ping()
                    samples.append((time.perf_counter() - t0) * 1000.0)

                conn.close()
                results[key] = round(min(samples), 3)

            except Exception:
                results[key] = None          # neighbour offline / unreachable

        return results

    # ------------------------------------------------------------------
    # exposed_deposit
    # ------------------------------------------------------------------
    def exposed_deposit(self, account_id: str, amount: float) -> dict:
        """
        Deposit `amount` into `account_id`.

        Returns:
            {"success": bool, "message": str,
             "account_id": str, "name": str, "new_balance": float,
             "sync_results": list}
        """
        account_id = str(account_id).strip().upper()

        if amount <= 0:
            return {"success": False, "message": "Deposit amount must be positive."}

        with self._lock:
            acc = db_instance.get_account(account_id)
            if not acc:
                return {"success": False,
                        "message": f"Account {account_id} does not exist."}

            old_balance = acc["balance"]
            new_balance = old_balance + amount

            # Update vector clock
            vector_clock = acc["vector_clock"].copy()
            vector_clock[self._node_name] = time.time()

            if not db_instance.update_account(account_id, acc["name"], new_balance, vector_clock):
                return {"success": False, "message": "Conflict detected, transaction aborted."}

            # Log transaction
            db_instance.log_transaction(account_id, "deposit", amount, old_balance, new_balance, vector_clock)

        _log(f"[{self._node_name}] DEPOSIT  | {account_id} ({acc['name']}) | "
             f"+UGX {amount:,.0f} | {old_balance:,.0f} -> {new_balance:,.0f}")

        # Replicate to other nodes (fault-tolerant — won't raise on failure)
        sync_results = sync_service.replicate_transaction({
            "account_id": account_id,
            "operation": "deposit",
            "amount": amount,
            "old_balance": old_balance,
            "new_balance": new_balance,
            "name": acc["name"],
            "vector_clock": vector_clock,
            "timestamp": time.time(),
            "node_id": self._node_name
        })

        return {
            "success":      True,
            "message":      "Deposit successful.",
            "account_id":   account_id,
            "name":         acc["name"],
            "new_balance":  new_balance,
            "sync_results": sync_results,
        }

    # ------------------------------------------------------------------
    # exposed_withdraw
    # ------------------------------------------------------------------
    def exposed_withdraw(self, account_id: str, amount: float) -> dict:
        """
        Withdraw `amount` from `account_id` (balance check enforced).

        Returns same shape as exposed_deposit.
        """
        account_id = str(account_id).strip().upper()

        if amount <= 0:
            return {"success": False, "message": "Withdrawal amount must be positive."}

        with self._lock:
            acc = db_instance.get_account(account_id)
            if not acc:
                return {"success": False,
                        "message": f"Account {account_id} does not exist."}

            old_balance = acc["balance"]

            if amount > old_balance:
                return {
                    "success": False,
                    "message": (f"Insufficient funds. "
                                f"Available: UGX {old_balance:,.0f}, "
                                f"Requested: UGX {amount:,.0f}"),
                }

            new_balance = old_balance - amount

            # Update vector clock
            vector_clock = acc["vector_clock"].copy()
            vector_clock[self._node_name] = time.time()

            if not db_instance.update_account(account_id, acc["name"], new_balance, vector_clock):
                return {"success": False, "message": "Conflict detected, transaction aborted."}

            # Log transaction
            db_instance.log_transaction(account_id, "withdraw", amount, old_balance, new_balance, vector_clock)

        _log(f"[{self._node_name}] WITHDRAW | {account_id} ({acc['name']}) | "
             f"-UGX {amount:,.0f} | {old_balance:,.0f} -> {new_balance:,.0f}")

        # Replicate to other nodes
        sync_results = sync_service.replicate_transaction({
            "account_id": account_id,
            "operation": "withdraw",
            "amount": amount,
            "old_balance": old_balance,
            "new_balance": new_balance,
            "name": acc["name"],
            "vector_clock": vector_clock,
            "timestamp": time.time(),
            "node_id": self._node_name
        })

        return {
            "success":      True,
            "message":      "Withdrawal successful.",
            "account_id":   account_id,
            "name":         acc["name"],
            "new_balance":  new_balance,
            "sync_results": sync_results,
        }

    # ------------------------------------------------------------------
    # exposed_get_balance
    # ------------------------------------------------------------------
    def exposed_get_balance(self, account_id: str) -> dict:
        account_id = str(account_id).strip().upper()
        acc = db_instance.get_account(account_id)

        if not acc:
            return {"success": False,
                    "message": f"Account {account_id} does not exist."}

        return {
            "success":    True,
            "account_id": account_id,
            "name":       acc["name"],
            "balance":    acc["balance"],
            "node":       self._node_name,
        }

    # ------------------------------------------------------------------
    # exposed_apply_transaction  — called by sync_service for replication
    # ------------------------------------------------------------------
    def exposed_apply_transaction(self, tx: dict) -> dict:
        with self._lock:
            success = db_instance.apply_transaction(tx)
            if success:
                _log(f"[{self._node_name}] SYNC-IN  | {tx['account_id']} | "
                     f"balance set to UGX {tx['new_balance']:,.0f}")
                return {"success": True, "message": "Transaction applied."}
            else:
                return {"success": False, "message": "Transaction ignored (conflict or already applied)."}

        _log(f"[{self._node_name}] SYNC-IN  | {account_id} | "
             f"balance set to UGX {new_balance:,.0f}")

        return {"success": True}

    # ------------------------------------------------------------------
    # exposed_list_accounts  — utility for client "list" view
    # ------------------------------------------------------------------
    def exposed_list_accounts(self) -> dict:
        return db_instance.get_all_accounts()

    # ------------------------------------------------------------------
    # exposed_get_pending_transactions  — for incremental sync
    # ------------------------------------------------------------------
    def exposed_get_pending_transactions(self, since: float) -> list:
        return db_instance.get_pending_transactions(since)

    # ------------------------------------------------------------------
    # exposed_sync_all  — pull and merge accounts from all other nodes
    # ------------------------------------------------------------------
    def exposed_sync_all(self) -> dict:
        results = sync_service.sync_all()
        return {"results": results}


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    node_cfg = NODES[THIS_NODE]
    host     = "0.0.0.0"      # listen on all interfaces
    port     = node_cfg["port"]

    print("=" * 55)
    print("  DISTRIBUTED MOBILE MONEY — RPC SERVER")
    print("=" * 55)
    print(f"  Node ID   : {THIS_NODE}")
    print(f"  Node Name : {node_cfg['name']}")
    print(f"  Location  : {node_cfg['location']}")
    print(f"  Listening : {host}:{port}")
    print(f"  Accounts  : {ACCOUNTS_FILE}")
    print(f"  Log file  : {LOG_FILE}")
    print("=" * 55)
    print("  Server running. Press Ctrl+C to stop.\n")

    server = ThreadedServer(
        MobileMoneyService,
        hostname=host,
        port=port,
        protocol_config={
            "allow_public_attrs": True,
            "allow_pickle":       True,
        },
    )
    
    # Enable SO_REUSEADDR to allow quick restarts
    server.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    # Start periodic sync thread
    def periodic_sync():
        while True:
            time.sleep(300)  # Sync every 5 minutes
            try:
                print("  [PERIODIC SYNC] Starting...")
                sync_service.sync_all()
                print("  [PERIODIC SYNC] Completed.")
            except Exception as e:
                print(f"  [PERIODIC SYNC] Failed: {e}")

    sync_thread = threading.Thread(target=periodic_sync, daemon=True)
    sync_thread.start()

    server.start()


if __name__ == "__main__":
    main()
