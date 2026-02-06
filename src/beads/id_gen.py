"""ID generation matching Go's idgen package.

Two algorithms are supported:
1. Hash-based IDs (types/id_generator.go): SHA256 hex, used for new issue creation
2. Base36 hash IDs (idgen/hash.go): SHA256 base36, used by the newer hash algorithm

The Python port uses algorithm #1 (hex-based) which is the primary path used by
the Go version's types.GenerateHashID.
"""

from __future__ import annotations

import hashlib
import math
from datetime import datetime


def generate_hash_id(prefix: str, title: str, description: str,
                     created: datetime, workspace_id: str) -> str:
    """Generate a full SHA256 hex hash for progressive collision handling.

    Matches Go's types.GenerateHashID exactly:
    - Concatenates title + description + RFC3339Nano timestamp + workspace_id
    - Returns full 64-char hex hash

    The caller extracts hash[:6] initially, then hash[:7], hash[:8] on collisions.
    """
    h = hashlib.sha256()
    h.update(title.encode("utf-8"))
    h.update(description.encode("utf-8"))
    # Match Go's time.RFC3339Nano format
    ts = created.isoformat()
    if ts.endswith("+00:00"):
        ts = ts[:-6] + "Z"
    h.update(ts.encode("utf-8"))
    h.update(workspace_id.encode("utf-8"))
    return h.hexdigest()


def make_issue_id(prefix: str, full_hash: str, length: int = 6) -> str:
    """Create an issue ID from prefix and hash.

    Format: prefix-{hex_chars}
    Example: bd-a3f2dd (6 chars), bd-a3f2dda (7 chars)
    """
    return f"{prefix}-{full_hash[:length]}"


# --- Base36 ID generation (matches idgen/hash.go) ---

BASE36_ALPHABET = "0123456789abcdefghijklmnopqrstuvwxyz"


def encode_base36(data: bytes, length: int) -> str:
    """Convert bytes to base36 string of specified length.

    Matches Go's idgen.EncodeBase36 exactly.
    """
    # Convert bytes to big integer
    num = int.from_bytes(data, byteorder="big")

    if num == 0:
        return "0" * length

    # Convert to base36
    chars: list[str] = []
    while num > 0:
        num, remainder = divmod(num, 36)
        chars.append(BASE36_ALPHABET[remainder])

    # Reverse to get most-significant first
    chars.reverse()
    result = "".join(chars)

    # Pad with zeros if needed
    if len(result) < length:
        result = "0" * (length - len(result)) + result

    # Truncate to exact length (keep least significant digits)
    if len(result) > length:
        result = result[len(result) - length:]

    return result


def generate_base36_hash_id(prefix: str, title: str, description: str,
                            creator: str, timestamp: datetime,
                            length: int = 6, nonce: int = 0) -> str:
    """Generate a base36 hash-based ID.

    Matches Go's idgen.GenerateHashID exactly.
    """
    content = f"{title}|{description}|{creator}|{int(timestamp.timestamp() * 1e9)}|{nonce}"
    hash_bytes = hashlib.sha256(content.encode("utf-8")).digest()

    # Determine bytes to use based on desired output length
    num_bytes_map = {3: 2, 4: 3, 5: 4, 6: 4, 7: 5, 8: 5}
    num_bytes = num_bytes_map.get(length, 3)

    short_hash = encode_base36(hash_bytes[:num_bytes], length)
    return f"{prefix}-{short_hash}"


# --- Hierarchical (child) IDs ---

def generate_child_id(parent_id: str, child_number: int) -> str:
    """Create a hierarchical child ID.

    Format: parent.N (e.g., "bd-af78e9a2.1", "bd-af78e9a2.1.2")
    """
    return f"{parent_id}.{child_number}"


def parse_hierarchical_id(issue_id: str) -> tuple[str, str, int]:
    """Extract root ID, parent ID, and depth from a hierarchical ID.

    Returns: (root_id, parent_id, depth)

    Examples:
        "bd-af78e9a2" → ("bd-af78e9a2", "", 0)
        "bd-af78e9a2.1" → ("bd-af78e9a2", "bd-af78e9a2", 1)
        "bd-af78e9a2.1.2" → ("bd-af78e9a2", "bd-af78e9a2.1", 2)
    """
    # Count dots for depth, but only count segments where suffix is all digits
    parts = issue_id.split(".")
    depth = 0
    for i in range(1, len(parts)):
        if parts[i].isdigit():
            depth += 1
        else:
            # Not a hierarchical dot
            return issue_id, "", 0

    if depth == 0:
        return issue_id, "", 0

    # Find first dot position
    first_dot = issue_id.index(".")
    root_id = issue_id[:first_dot]

    # Find last dot position
    last_dot = issue_id.rindex(".")
    parent_id = issue_id[:last_dot]

    return root_id, parent_id, depth


MAX_HIERARCHY_DEPTH = 3


def check_hierarchy_depth(parent_id: str, max_depth: int = 0) -> str | None:
    """Check if adding a child would exceed max depth. Returns error msg or None."""
    if max_depth < 1:
        max_depth = MAX_HIERARCHY_DEPTH
    depth = parent_id.count(".")
    if depth >= max_depth:
        return f"maximum hierarchy depth ({max_depth}) exceeded for parent {parent_id}"
    return None
