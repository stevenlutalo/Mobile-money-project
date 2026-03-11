# — Standard library —
import logging
import ssl
from datetime import datetime, timedelta
from typing import Dict, Tuple

# — Third-party packages —
import jwt
from cryptography import x509
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa

# — This project —
from core.exceptions import AuthError, SecurityError

log = logging.getLogger(__name__)


# JWT (JSON Web Token) — a signed credential, like a tamper-proof ID card.
# The server issues one when you log in. You attach it to every request.
# The server verifies the signature to confirm it's real and hasn't expired.


# Keys for JWT claims
JWT_EXPIRY_MINUTES = 15
# How long a login token is valid. After this, user must log in again.

JWT_ALGORITHM = "HS256"
# Algorithm for signing. HS256 uses a shared secret (simple, fast).
# RS256 would use public/private keys (more complex, better for public APIs).

JWT_NONCE_TTL_SECS = 30
# How long to keep track of used nonces (prevents replay attacks).


class AuthService:
    """
    Manages user authentication using JWT tokens.
    Issues tokens on login and verifies them on every request.

    Why it exists: in a distributed system, you can't use server-side
    sessions (they don't replicate). JWTs are self-contained — the token
    itself proves the user's identity via cryptographic signature.

    Usage:
      auth = AuthService(secret_key="your-secret-key-min-32-chars")
      token = auth.issue_token(subject="user-123", role="admin")
      claims = auth.verify_token(token)
      print(claims["sub"])  # "user-123"
    """

    def __init__(self, secret_key: str):
        """
        Initialize the auth service.

        Args:
            secret_key: Secret key for signing JWTs (must be >= 32 characters for HS256)
        """
        if len(secret_key) < 32:
            raise SecurityError("JWT secret key must be at least 32 characters")

        self.secret_key = secret_key
        self._used_nonces = {}  # Nonce -> expiry time (for replay attack prevention)

    def issue_token(
        self,
        subject: str,
        role: str = "user",
        expiry_minutes: int = JWT_EXPIRY_MINUTES,
    ) -> str:
        """
        Issue a new JWT token for a logged-in user.

        Args:
            subject: User identifier (e.g., "user-123")
            role: User role (e.g., "user", "admin")
            expiry_minutes: How long the token is valid

        Returns:
            JWT string (safe to send over the network)
        """
        now = datetime.utcnow()
        expiry = now + timedelta(minutes=expiry_minutes)

        payload = {
            "sub": subject,  # Subject (who is this token for?)
            "role": role,    # Role (what can they do?)
            "iat": int(now.timestamp()),  # Issued at (when was it created?)
            "exp": int(expiry.timestamp()),  # Expiry (when does it expire?)
        }

        token = jwt.encode(payload, self.secret_key, algorithm=JWT_ALGORITHM)
        log.info(f"Issued token for {subject} (role={role}, expires in {expiry_minutes}m)")
        return token

    def verify_token(self, token_str: str) -> dict:
        """
        Verify a JWT and extract claims (user ID, role, etc).

        Args:
            token_str: JWT string to verify

        Returns:
            Dict of claims: {"sub": user_id, "role": role, "iat": ..., "exp": ...}

        Raises:
            AuthError: if token is invalid, expired, or tampered with
        """
        try:
            claims = jwt.decode(token_str, self.secret_key, algorithms=[JWT_ALGORITHM])
            now = datetime.utcnow().timestamp()

            if claims.get("exp", 0) < now:
                raise AuthError("Token has expired")

            return claims

        except jwt.InvalidTokenError as e:
            raise AuthError(f"Invalid token: {e}")
        except AuthError:
            raise
        except Exception as e:
            raise AuthError(f"Token verification failed: {e}")

    @staticmethod
    def generate_keypair() -> Tuple[str, str]:
        """
        Generate an RSA public/private key pair for TLS certificates.
        Used by node_server.py to create mTLS credentials.

        Returns:
            Tuple of (private_key_pem, public_key_pem) as strings
        """
        # Generate 4096-bit RSA key
        private_key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=4096,
            backend=default_backend(),
        )

        # Serialize to PEM (Privacy Enhanced Mail format)
        private_pem = private_key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ).decode()

        public_key = private_key.public_key()
        public_pem = public_key.public_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PublicFormat.SubjectPublicKeyInfo,
        ).decode()

        return private_pem, public_pem

    @staticmethod
    def create_ssl_context(
        cert_file: str,
        key_file: str,
        ca_cert_file: str,
    ) -> ssl.SSLContext:
        """
        Create an SSL context for mTLS (mutual TLS) connections.
        Both client and server authenticate each other.

        Args:
            cert_file: Path to this node's certificate (PEM format)
            key_file: Path to this node's private key (PEM format)
            ca_cert_file: Path to Certificate Authority's certificate

        Returns:
            Configured ssl.SSLContext ready for rpyc

        Raises:
            SecurityError: if certificates are missing or invalid
        """
        try:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

            # Load this server's certificate and key
            context.load_cert_chain(cert_file, keyfile=key_file)

            # Load CA certificate for client verification
            # CERT_REQUIRED = verify client certificate
            context.verify_mode = ssl.CERT_REQUIRED
            context.load_verify_locations(ca_cert_file)

            # Optional: set strong cipher suites (for security)
            # context.set_ciphers("HIGH:!aNULL:!MD5")

            log.info(f"SSL context created with cert={cert_file}, ca={ca_cert_file}")
            return context

        except FileNotFoundError as e:
            raise SecurityError(f"Certificate file not found: {e}. "
                              f"Run: bash certs/gen_certs.sh node-1")
        except Exception as e:
            raise SecurityError(f"Failed to create SSL context: {e}")
