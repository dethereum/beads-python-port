"""Utility functions for beads CLI."""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

from beads.models import Issue, Status


def resolve_partial_id(partial: str, all_ids: list[str]) -> str | None:
    """Resolve a partial ID to a full ID.

    Matches if the partial is a prefix of any ID. Returns None if zero or
    multiple matches.
    """
    matches = [id for id in all_ids if id.startswith(partial)]
    if len(matches) == 1:
        return matches[0]
    # Try exact match first
    if partial in all_ids:
        return partial
    return None


def extract_issue_prefix(issue_id: str) -> str:
    """Extract the prefix from an issue ID.

    Examples:
        "bd-a3f2dd" → "bd"
        "myproject-abc123" → "myproject"
        "bd-a3f2dd.1" → "bd"
    """
    # Remove hierarchical suffix first (everything after first .)
    base = issue_id.split(".")[0]
    # Find last hyphen in the base
    last_hyphen = base.rfind("-")
    if last_hyphen < 0:
        return base
    return base[:last_hyphen]


def format_priority(priority: int) -> str:
    """Format priority as P0-P4."""
    return f"P{priority}"


def parse_priority(s: str) -> int | None:
    """Parse priority from string. Accepts P0-P4 or 0-4."""
    s = s.strip().upper()
    if s.startswith("P"):
        s = s[1:]
    try:
        p = int(s)
        if 0 <= p <= 4:
            return p
    except ValueError:
        pass
    return None


def priority_label(priority: int) -> str:
    """Return human-readable priority label."""
    labels = {0: "critical", 1: "high", 2: "medium", 3: "low", 4: "backlog"}
    return labels.get(priority, f"P{priority}")


def status_symbol(status: str) -> str:
    """Return a symbol for status display."""
    symbols = {
        Status.OPEN: " ",
        Status.IN_PROGRESS: ">",
        Status.BLOCKED: "!",
        Status.DEFERRED: "~",
        Status.CLOSED: "x",
        Status.TOMBSTONE: "-",
        Status.PINNED: "^",
        Status.HOOKED: "@",
    }
    return symbols.get(status, "?")


def format_time_ago(dt: datetime) -> str:
    """Format a datetime as a relative time string."""
    now = datetime.now(timezone.utc)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    delta = now - dt
    seconds = int(delta.total_seconds())
    if seconds < 60:
        return "just now"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    if hours < 24:
        return f"{hours}h ago"
    days = hours // 24
    if days < 30:
        return f"{days}d ago"
    months = days // 30
    if months < 12:
        return f"{months}mo ago"
    years = days // 365
    return f"{years}y ago"


def parse_duration(s: str) -> timedelta | None:
    """Parse a Go-style duration string (e.g., '5s', '1h30m', '2d')."""
    if not s:
        return None
    total_seconds = 0.0
    remaining = s.strip()

    # Support day suffix
    m = re.match(r"(\d+)d", remaining)
    if m:
        total_seconds += int(m.group(1)) * 86400
        remaining = remaining[m.end():]

    m = re.match(r"(\d+)h", remaining)
    if m:
        total_seconds += int(m.group(1)) * 3600
        remaining = remaining[m.end():]

    m = re.match(r"(\d+)m(?!s)", remaining)
    if m:
        total_seconds += int(m.group(1)) * 60
        remaining = remaining[m.end():]

    m = re.match(r"(\d+)s", remaining)
    if m:
        total_seconds += int(m.group(1))
        remaining = remaining[m.end():]

    m = re.match(r"(\d+)ms", remaining)
    if m:
        total_seconds += int(m.group(1)) / 1000
        remaining = remaining[m.end():]

    if total_seconds == 0 and s:
        return None
    return timedelta(seconds=total_seconds)


def truncate(s: str, max_len: int = 60) -> str:
    """Truncate a string with ellipsis."""
    if len(s) <= max_len:
        return s
    return s[:max_len - 3] + "..."


def format_issue_row(issue: Issue, long_format: bool = False) -> str:
    """Format an issue as a single-line row for list display."""
    sym = status_symbol(issue.status)
    pri = format_priority(issue.priority)
    age = format_time_ago(issue.created_at)
    title = truncate(issue.title, 50)

    if long_format:
        assignee = issue.assignee or "-"
        itype = issue.issue_type or "task"
        return f"[{sym}] {issue.id:<20} {pri} {itype:<8} {assignee:<15} {title}  ({age})"
    return f"[{sym}] {issue.id:<20} {pri} {title}  ({age})"
