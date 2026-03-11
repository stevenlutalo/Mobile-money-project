# — Standard library —
import hashlib
import json
import logging
import os
import time
from typing import List, Dict

# — Third-party packages —
# (none)

# — This project —
from core.exceptions import AuditLogError

log = logging.getLogger(__name__)


# The audit log is a tamper-proof record of every transaction.
# Each entry links to the previous one via a hash (like a blockchain).
# If anyone edits a past entry, every subsequent hash breaks —
# verify_chain() will catch it immediately.


class AuditLog:
    """
    Immutable transaction log for compliance and debugging.
    Each entry links to the previous via a hash chain.
    If any entry is tampered with, verification fails.

    Why it exists: mobile money systems must be fully auditable for
    regulatory compliance. The audit log proves what happened, in order,
    with no way to hide or rewrite history.

    Usage:
      log = AuditLog(node_id="node-1", log_path="data/node-1.audit")
      log.append("ACC001", "debit", 500, "ACC001→ACC002 transfer", "user-123")
      log.append("ACC002", "credit", 500, "ACC001→ACC002 transfer", "user-123")
      if log.verify_chain():
          print("Audit log is clean — no tampering detected")
    """

    def __init__(self, node_id: str, log_path: str = "data/audit.log"):
        """
        Initialize the audit log.

        Args:
            node_id: This server's ID (included in every entry)
            log_path: Path to the audit log file
        """
        self.node_id = node_id
        self.log_path = log_path
        self._entries = []
        self._last_hash = "0" * 64  # Empty initial hash (64 hex chars for SHA-256)

        # Create directory if needed
        os.makedirs(os.path.dirname(log_path) or ".", exist_ok=True)

        # Load existing entries
        self._load_from_disk()

    def append(
        self,
        account_id: str,
        operation: str,  # "credit", "debit", "create", etc
        amount: float,
        description: str,
        user_id: str,
    ) -> str:
        """
        Record a transaction in the audit log.

        Each entry is cryptographically linked to the previous one.
        Returns the hash of the new entry for verification.

        Args:
            account_id: Account involved
            operation: Type of operation
            amount: Transaction amount
            description: Human-readable summary
            user_id: Who initiated this transaction

        Returns:
            SHA-256 hash of this entry (for verification)
        """
        entry = {
            "timestamp": time.time(),
            "node_id": self.node_id,
            "account_id": account_id,
            "operation": operation,
            "amount": amount,
            "description": description,
            "user_id": user_id,
            "prev_hash": self._last_hash,
        }

        # Compute hash of this entry (excludes the hash itself)
        entry_hash = self._hash_entry(entry)
        entry["hash"] = entry_hash
        self._last_hash = entry_hash

        self._entries.append(entry)

        # Write to disk immediately (durability)
        try:
            with open(self.log_path, "a") as f:
                f.write(json.dumps(entry) + "\n")
        except Exception as e:
            log.error(f"Failed to write audit log: {e}")

        return entry_hash

    def verify_chain(self) -> bool:
        """
        Verify that the entire audit log is intact and unmodified.
        Walks the hash chain from start to end.

        Returns:
            True if chain is valid, False if tampering is detected
        """
        if not self._entries:
            return True  # Empty chain is valid

        prev_hash = "0" * 64

        for i, entry in enumerate(self._entries):
            if entry.get("prev_hash") != prev_hash:
                log.error(
                    f"Hash chain break at entry {i}: "
                    f"expected prev_hash={prev_hash}, got {entry.get('prev_hash')}"
                )
                return False

            # Recompute hash (excluding the stored hash)
            recomputed = self._hash_entry(entry)
            if recomputed != entry.get("hash"):
                log.error(
                    f"Entry {i} hash mismatch: "
                    f"stored={entry.get('hash')}, computed={recomputed}"
                )
                return False

            prev_hash = entry.get("hash")

        log.info(f"Audit chain verified: {len(self._entries)} entries, chain intact")
        return True

    def tail(self, n: int = 20) -> List[Dict]:
        """
        Return the last N entries from the audit log.
        Used for debugging and status displays.

        Args:
            n: Number of entries to return

        Returns:
            List of entry dicts (most recent last)
        """
        return self._entries[-n:]

    def _hash_entry(self, entry: dict) -> str:
        """
        Compute SHA-256 hash of an entry for chain linking.
        The hash field itself is excluded from the computation.

        Args:
            entry: Entry dict (may include "hash" key which is temporarily removed)

        Returns:
            64-character hex string
        """
        # Copy and remove hash and timestamp to compute consistent hash
        entry_copy = dict(entry)
        entry_copy.pop("hash", None)

        # Use JSON with sorted keys for deterministic hash
        json_str = json.dumps(entry_copy, sort_keys=True)
        return hashlib.sha256(json_str.encode()).hexdigest()

    def _load_from_disk(self) -> None:
        """Load all audit log entries from disk on startup."""
        if not os.path.exists(self.log_path):
            log.info(f"Audit log {self.log_path} does not exist yet")
            return

        try:
            with open(self.log_path, "r") as f:
                for line in f:
                    if line.strip():
                        entry = json.loads(line)
                        self._entries.append(entry)
                        self._last_hash = entry.get("hash", self._last_hash)

            log.info(f"Loaded {len(self._entries)} audit entries from {self.log_path}")

            # Verify chain integrity on startup
            if not self.verify_chain():
                log.warning("Audit log verification failed — possible corruption!")

        except Exception as e:
            log.error(f"Failed to load audit log: {e}")
            raise AuditLogError(f"Cannot read audit log at {self.log_path}: {e}")
