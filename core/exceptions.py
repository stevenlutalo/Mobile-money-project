# — Standard library —
# (none)

# — Third-party packages —
# (none)

# — This project —
# (none)


"""
Custom exceptions for the distributed mobile money system.
All error types are defined here so beginners can find them in one place.
"""


class MoneyError(Exception):
    """Base exception for all money-related errors."""
    pass


class InsufficientFundsError(MoneyError):
    """
    Raised when an account doesn't have enough money to cover a debit.
    This is a normal business error, not a bug.
    """
    pass


class AuthError(MoneyError):
    """
    Raised when login fails or a token is invalid/expired.
    The user should be prompted to log in again.
    """
    pass


class SecurityError(MoneyError):
    """
    Raised when a security check fails: tampered token, invalid signature,
    certificate verification failure, etc. This may indicate an attack.
    """
    pass


class NodeUnreachableError(MoneyError):
    """
    Raised when no server in the cluster responds.
    The network may be down, or the client is not connected to the internet.
    """
    pass


class DiscoveryError(MoneyError):
    """
    Raised when server discovery fails after exhausting all candidates.
    This includes cases where no servers have the desired account (ring ownership).
    """
    pass


class SyncError(MoneyError):
    """
    Raised when merging accounts from different servers fails.
    This should never happen with correct CRDT logic.
    """
    pass


class StorageError(MoneyError):
    """
    Raised when the local LevelDB fails (corrupted, permission denied, etc.).
    """
    pass


class AuditLogError(MoneyError):
    """
    Raised when the audit log is tampered with or verification fails.
    """
    pass


class InvalidRequest(MoneyError):
    """
    Raised when an RPC request has invalid parameters or structure.
    """
    pass
