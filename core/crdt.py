# — Standard library —
import json
import logging
from typing import Dict

# — Third-party packages —
# (none)

# — This project —
from core.exceptions import InsufficientFundsError, SyncError

log = logging.getLogger(__name__)


# WHY CRDTs? (read this before the code)
# ────────────────────────────────────────
# Problem: two servers process transactions at the same time. When
# they sync, whose version wins? "Last write wins" loses money.
#
# Solution: a PN-Counter CRDT. Instead of storing one balance, we store
# every credit and debit each server has ever processed. The true balance
# is always: sum(all credits) − sum(all debits). When two servers sync,
# they take the maximum of each other's values — no conflict possible.
#
# Example:
#   Server A: credits node-1=100  debits node-1=30  → balance=70
#   Server B: credits node-1=100  debits node-2=10  → balance=90
#   After merge: credits={node-1:100}  debits={node-1:30, node-2:10}
#   Merged balance = 100 − 30 − 10 = 60  ✓  (correct in all cases)


class PNCounter:
    """
    A partition-safe account balance that can be updated on multiple servers
    simultaneously without causing data corruption or disagreement.

    Why it exists: In a distributed system without a central authority, two
    servers might process transactions at the exact same time. A normal
    balance field would corrupt (the last write overwrites the other, losing
    money). PNCounter uses per-server increment counters so merging always
    produces the mathematically correct result.

    Usage:
      counter = PNCounter(node_id="node-1", initial_balance=1000.0)
      counter.credit(200.0)                  # received money
      counter.debit(50.0)                    # sent money
      print(counter.balance())               # 1150.0
      delta = counter.get_delta()            # changes since last get_delta()
      remote_counter.merge_delta(delta)      # apply changes from other server
    """

    def __init__(self, node_id: str, initial_balance: float = 0.0):
        """
        Initialize a new counter for a specific server node.

        Args:
            node_id: The server ID that will manage this counter (e.g., "node-1")
            initial_balance: Starting balance in currency units
        """
        self.node_id = node_id
        # P = positive counts (credits) per node. Structure: {node_id: sum}
        self.P = {node_id: initial_balance if initial_balance > 0 else 0.0}
        # N = negative counts (debits) per node. Structure: {node_id: sum}
        self.N = {node_id: abs(initial_balance) if initial_balance < 0 else 0.0}
        # Track what was new since last get_delta() so we send only changes
        self._delta_P = dict(self.P)
        self._delta_N = dict(self.N)

    def credit(self, amount: float) -> None:
        """
        Record a deposit (incoming money) from another account.
        Always succeeds — no balance check needed for credits.

        Args:
            amount: Positive amount to add to this account
        """
        if amount < 0:
            raise ValueError("Credit amount must be positive")
        self.P[self.node_id] = self.P.get(self.node_id, 0.0) + amount

    def debit(self, amount: float) -> None:
        """
        Record a withdrawal (outgoing money) to another account.
        Checks balance first — raises InsufficientFundsError if impossible.

        Args:
            amount: Positive amount to subtract from this account

        Raises:
            InsufficientFundsError: if balance < amount
        """
        if amount < 0:
            raise ValueError("Debit amount must be positive")
        if self.balance() < amount:
            raise InsufficientFundsError(
                f"Insufficient funds: balance={self.balance()}, "
                f"requested debit={amount}"
            )
        self.N[self.node_id] = self.N.get(self.node_id, 0.0) + amount

    def balance(self) -> float:
        """
        Return the current balance: sum(all credits) − sum(all debits).
        This is always correct, even during sync or after merges.

        Returns:
            Float balance in currency units (may be negative if system is corrupted)
        """
        total_credits = sum(self.P.values())
        total_debits = sum(self.N.values())
        return total_credits - total_debits

    def get_delta(self) -> dict:
        """
        Return only the incremental changes since the last get_delta() call.
        Used by gossip protocol to send minimal data between servers.

        The delta is a dict with structure:
          {"P": {node_id: amount, ...}, "N": {node_id: amount, ...}}

        After this call, the delta is reset so subsequent calls only include new changes.

        Returns:
            Dict of {"P": {...}, "N": {...}} with only changed entries
        """
        delta = {
            "P": dict(self._delta_P),
            "N": dict(self._delta_N),
        }
        # Reset delta tracking after copying (important: copy first, then flush)
        self._delta_P = {}
        self._delta_N = {}
        return delta

    def merge_delta(self, delta: dict) -> dict:
        """
        Merge incremental changes from another server. Takes element-wise max
        (never overwrites with a lower value). Idempotent — calling twice with
        the same delta produces the same result.

        Args:
            delta: Dict of {"P": {...}, "N": {...}} from another server's get_delta()

        Returns:
            Dict of changes applied (same structure as delta) for tracking

        Raises:
            SyncError: if delta format is invalid
        """
        if not isinstance(delta, dict) or "P" not in delta or "N" not in delta:
            raise SyncError(f"Invalid delta format: {delta}")

        applied = {"P": {}, "N": {}}

        # Merge positive counts (credits)
        for node_id, amount in delta["P"].items():
            current = self.P.get(node_id, 0.0)
            new_value = max(current, amount)  # ← CRDT rule: always take the maximum
            if new_value != current:
                self.P[node_id] = new_value
                applied["P"][node_id] = new_value

        # Merge negative counts (debits)
        for node_id, amount in delta["N"].items():
            current = self.N.get(node_id, 0.0)
            new_value = max(current, amount)  # ← CRDT rule: element-wise max
            if new_value != current:
                self.N[node_id] = new_value
                applied["N"][node_id] = new_value

        return applied

    def full_state(self) -> dict:
        """
        Return the complete state (P and N counters) for anti-entropy sync.
        Used when a server has been offline and needs a full state copy.

        Returns:
            Dict of {"P": {...}, "N": {...}} with all accumulated values
        """
        return {
            "P": dict(self.P),
            "N": dict(self.N),
        }

    def merge_full(self, remote: dict) -> None:
        """
        Merge a complete state from another server (anti-entropy).
        Applies the CRDT merge rule: take element-wise maximum of all counters.

        Args:
            remote: Dict of {"P": {...}, "N": {...}} from another server
        """
        if not isinstance(remote, dict) or "P" not in remote or "N" not in remote:
            raise SyncError(f"Invalid full state format: {remote}")

        # Merge P counters: take maximum for each node_id
        for node_id, amount in remote["P"].items():
            self.P[node_id] = max(self.P.get(node_id, 0.0), amount)

        # Merge N counters: take maximum for each node_id
        for node_id, amount in remote["N"].items():
            self.N[node_id] = max(self.N.get(node_id, 0.0), amount)

    def to_json(self) -> str:
        """
        Serialize the complete state to JSON for storage in LevelDB.

        Returns:
            JSON string of {"P": {...}, "N": {...}}
        """
        return json.dumps(self.full_state())

    @classmethod
    def from_json(cls, node_id: str, data: str) -> "PNCounter":
        """
        Deserialize a counter from JSON (recover from LevelDB).

        Args:
            node_id: The server that owns this counter
            data: JSON string from to_json()

        Returns:
            PNCounter instance with the same state as when it was serialized
        """
        counter = cls(node_id=node_id, initial_balance=0.0)
        state = json.loads(data)
        counter.merge_full(state)
        return counter
