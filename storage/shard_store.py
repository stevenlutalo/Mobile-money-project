# — Standard library —
import json
import logging
import sqlite3
from typing import List, Optional

# — Third-party packages —
# (none)

# — This project —
from core.crdt import PNCounter
from core.exceptions import StorageError

log = logging.getLogger(__name__)


# SQLite — an embedded relational database (built-in to Python, no compilation needed).
# Replaces plyvel/LevelDB for Windows compatibility.
# Keys = account IDs (strings). Values = CRDT state (JSON).


class ShardStore:
    """
    Embedded database for storing account balances using SQLite.
    Each server manages a subset of all accounts (its shard).

    Why it exists: accounts must persist across server restarts and
    process failures. SQLite provides fast, reliable storage without
    requiring a separate database server and compiles on all platforms.

    Usage:
      store = ShardStore(db_path="data/node-1.db")
      store.put("ACC123", counter)
      balance = store.get("ACC123").balance()
      store.close()
    """

    def __init__(self, db_path: str = "data/shard.db", node_id: str = "node-1"):
        """
        Initialize or open an existing SQLite database.

        Args:
            db_path: Path to the database file
            node_id: This server's ID (for CRDT initialization)
        """
        self.db_path = db_path
        self.node_id = node_id
        self._deltas = {}  # Track changes since last get_delta()

        try:
            self.db = sqlite3.connect(db_path, check_same_thread=False)
            self.db.execute(
                """
                CREATE TABLE IF NOT EXISTS accounts (
                    id TEXT PRIMARY KEY,
                    data TEXT NOT NULL
                )
                """
            )
            self.db.commit()
            log.info(f"Opened database at {db_path}")
        except Exception as e:
            raise StorageError(f"Failed to open database at {db_path}: {e}")

    def get(self, account_id: str) -> Optional[PNCounter]:
        """
        Retrieve an account's balance from storage.

        Returns None if the account doesn't exist (normal for new accounts).

        Args:
            account_id: The account to retrieve

        Returns:
            PNCounter instance, or None if not found
        """
        try:
            cursor = self.db.execute(
                "SELECT data FROM accounts WHERE id = ?", (account_id,)
            )
            row = cursor.fetchone()
            if row is None:
                return None
            return PNCounter.from_json(self.node_id, row[0])
        except Exception as e:
            log.error(f"Failed to get account {account_id}: {e}")
            raise StorageError(f"Failed to read account {account_id}: {e}")

    def put(self, account_id: str, counter: PNCounter) -> None:
        """
        Store an account's balance to persistent storage.

        Also marks the account as having a delta for gossip.

        Args:
            account_id: The account to store
            counter: PNCounter with the new balance
        """
        try:
            value = counter.to_json()
            self.db.execute(
                "INSERT OR REPLACE INTO accounts (id, data) VALUES (?, ?)",
                (account_id, value)
            )
            self.db.commit()
            # Mark as changed for delta gossip
            self._deltas[account_id] = counter.get_delta()
            log.debug(f"Stored account {account_id}, balance={counter.balance()}")
        except Exception as e:
            log.error(f"Failed to put account {account_id}: {e}")
            raise StorageError(f"Failed to write account {account_id}: {e}")

    def put_batch(self, accounts: dict) -> None:
        """
        Atomically store multiple accounts in one write.
        Used for multi-account transfers to ensure consistency.

        Args:
            accounts: Dict of {account_id: PNCounter, ...}
        """
        try:
            with self.db:
                for account_id, counter in accounts.items():
                    value = counter.to_json()
                    self.db.execute(
                        "INSERT OR REPLACE INTO accounts (id, data) VALUES (?, ?)",
                        (account_id, value)
                    )
                    self._deltas[account_id] = counter.get_delta()
            log.debug(f"Batch stored {len(accounts)} accounts")
        except Exception as e:
            log.error(f"Batch put failed: {e}")
            raise StorageError(f"Batch write failed: {e}")

    def delete(self, account_id: str) -> None:
        """
        Remove an account from storage.
        Generally not used (closed accounts usually just have zero balance),
        but provided for cleanup.

        Args:
            account_id: The account to delete
        """
        try:
            self.db.execute("DELETE FROM accounts WHERE id = ?", (account_id,))
            self.db.commit()
            log.info(f"Deleted account {account_id}")
        except Exception as e:
            log.error(f"Failed to delete account {account_id}: {e}")

    def list_accounts(self) -> List[str]:
        """
        Return all account IDs currently in storage.
        Used for gossip and full state sync.

        Returns:
            List of account ID strings
        """
        try:
            cursor = self.db.execute("SELECT id FROM accounts")
            accounts = [row[0] for row in cursor.fetchall()]
            return accounts
        except Exception as e:
            log.error(f"Failed to list accounts: {e}")
            raise StorageError(f"Failed to list accounts: {e}")

    def get_delta(self, account_id: str) -> dict:
        """
        Get the incremental changes for one account since last call.
        Used by delta gossip to send only what's new.

        Returns None if account has no changes.

        Args:
            account_id: Account to get delta for

        Returns:
            Dict of {"P": {...}, "N": {...}} or {} if no changes
        """
        delta = self._deltas.get(account_id, {})
        # Clear the delta after reading (important: only send once)
        if account_id in self._deltas:
            del self._deltas[account_id]
        return delta

    def close(self) -> None:
        """Close the database connection."""
        try:
            self.db.close()
            log.info(f"Closed database at {self.db_path}")
        except Exception as e:
            log.error(f"Error closing database: {e}")
