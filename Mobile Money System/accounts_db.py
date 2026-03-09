# =============================================================================
# accounts_db.py — SQLite Database Handler for Accounts
# =============================================================================
# Replaces JSON file with SQLite for better performance, concurrency, and integrity.
# Uses WAL mode for high availability and concurrent reads/writes.
# Includes vector clock support for conflict resolution.
# =============================================================================

import sqlite3
import os
import json
import time
from typing import Dict, List, Tuple, Optional

class AccountsDB:
    def __init__(self, db_path: str = "accounts.db"):
        self.db_path = db_path
        self.node_id = os.environ.get("NODE_ID", "unknown")  # Set per node
        self.init_db()
        self.migrate_from_json()  # One-time migration

    def init_db(self):
        """Initialize database schema."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("PRAGMA journal_mode=WAL")  # Write-Ahead Logging for concurrency
            conn.execute("PRAGMA synchronous=NORMAL")  # Balance performance/safety
            conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
            conn.execute("PRAGMA foreign_keys=ON")

            # Accounts table with vector clock
            conn.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    balance REAL NOT NULL DEFAULT 0.0,
                    vector_clock TEXT NOT NULL DEFAULT '{}',  -- JSON string of node->timestamp
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL
                )
            """)

            # Transaction log for replication
            conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    account_id TEXT NOT NULL,
                    operation TEXT NOT NULL,  -- 'deposit', 'withdraw'
                    amount REAL NOT NULL,
                    old_balance REAL NOT NULL,
                    new_balance REAL NOT NULL,
                    vector_clock TEXT NOT NULL,
                    timestamp REAL NOT NULL,
                    node_id TEXT NOT NULL,
                    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
                )
            """)

            # Sync metadata
            conn.execute("""
                CREATE TABLE IF NOT EXISTS sync_state (
                    key TEXT PRIMARY KEY,
                    value TEXT
                )
            """)

            conn.commit()

    def migrate_from_json(self):
        """Migrate from accounts.json to database (run once)."""
        json_path = "accounts.json"
        if not os.path.exists(json_path):
            return

        try:
            with open(json_path, "r") as f:
                data = json.load(f)

            with sqlite3.connect(self.db_path) as conn:
                for acc_id, acc in data.items():
                    vector_clock = {self.node_id: acc.get("last_updated", time.time())}
                    conn.execute("""
                        INSERT OR REPLACE INTO accounts
                        (account_id, name, balance, vector_clock, created_at, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (
                        acc_id,
                        acc["name"],
                        acc["balance"],
                        json.dumps(vector_clock),
                        acc.get("last_updated", time.time()),
                        acc.get("last_updated", time.time())
                    ))
                conn.commit()

            # Backup and remove old file
            os.rename(json_path, f"{json_path}.backup")
            print("Migrated from JSON to database.")

        except Exception as e:
            print(f"Migration failed: {e}")

    def get_account(self, account_id: str) -> Optional[Dict]:
        """Get account with vector clock."""
        with sqlite3.connect(self.db_path) as conn:
            row = conn.execute("""
                SELECT account_id, name, balance, vector_clock, updated_at
                FROM accounts WHERE account_id = ?
            """, (account_id,)).fetchone()

            if row:
                return {
                    "account_id": row[0],
                    "name": row[1],
                    "balance": row[2],
                    "vector_clock": json.loads(row[3]),
                    "last_updated": row[4]
                }
        return None

    def get_all_accounts(self) -> Dict[str, Dict]:
        """Get all accounts for sync."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("""
                SELECT account_id, name, balance, vector_clock, updated_at
                FROM accounts
            """).fetchall()

            accounts = {}
            for row in rows:
                accounts[row[0]] = {
                    "name": row[1],
                    "balance": row[2],
                    "vector_clock": json.loads(row[3]),
                    "last_updated": row[4]
                }
            return accounts

    def update_account(self, account_id: str, name: str, balance: float,
                      vector_clock: Dict[str, float]) -> bool:
        """Update account with vector clock conflict resolution."""
        with sqlite3.connect(self.db_path) as conn:
            # Check for conflicts
            existing = self.get_account(account_id)
            if existing and not self._vector_clock_allows_update(
                existing["vector_clock"], vector_clock
            ):
                return False  # Conflict detected

            now = time.time()
            conn.execute("""
                INSERT OR REPLACE INTO accounts
                (account_id, name, balance, vector_clock, created_at, updated_at)
                VALUES (?, ?, ?, ?, COALESCE((SELECT created_at FROM accounts WHERE account_id = ?), ?), ?)
            """, (
                account_id, name, balance, json.dumps(vector_clock),
                account_id, now, now
            ))
            conn.commit()
            return True

    def _vector_clock_allows_update(self, local_vc: Dict[str, float],
                                   remote_vc: Dict[str, float]) -> bool:
        """Check if remote vector clock allows update (no conflicts)."""
        # Simple check: remote must dominate local
        for node, ts in local_vc.items():
            if remote_vc.get(node, 0) < ts:
                return False
        return True

    def log_transaction(self, account_id: str, operation: str, amount: float,
                       old_balance: float, new_balance: float, vector_clock: Dict):
        """Log transaction for replication."""
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                INSERT INTO transactions
                (account_id, operation, amount, old_balance, new_balance, vector_clock, timestamp, node_id)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                account_id, operation, amount, old_balance, new_balance,
                json.dumps(vector_clock), time.time(), self.node_id
            ))
            conn.commit()

    def get_pending_transactions(self, since: float) -> List[Dict]:
        """Get transactions since timestamp for replication."""
        with sqlite3.connect(self.db_path) as conn:
            rows = conn.execute("""
                SELECT account_id, operation, amount, old_balance, new_balance, vector_clock, timestamp, node_id
                FROM transactions WHERE timestamp > ?
                ORDER BY timestamp
            """, (since,)).fetchall()

            return [{
                "account_id": row[0],
                "operation": row[1],
                "amount": row[2],
                "old_balance": row[3],
                "new_balance": row[4],
                "vector_clock": json.loads(row[5]),
                "timestamp": row[6],
                "node_id": row[7]
            } for row in rows]

    def apply_transaction(self, tx: Dict) -> bool:
        """Apply replicated transaction."""
        account_id = tx["account_id"]
        with sqlite3.connect(self.db_path) as conn:
            # Get current state
            current = self.get_account(account_id)
            if not current:
                # Create account if doesn't exist
                self.update_account(account_id, tx.get("name", "Unknown"),
                                   tx["new_balance"], tx["vector_clock"])
                return True

            # Check if already applied
            if self._vector_clock_allows_update(current["vector_clock"], tx["vector_clock"]):
                self.update_account(account_id, current["name"],
                                   tx["new_balance"], tx["vector_clock"])
                return True
        return False

# Global instance - create when needed
# db = AccountsDB()</content>
