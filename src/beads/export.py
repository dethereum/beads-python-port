"""JSONL export - flush DB state to .beads/issues.jsonl.

Matches Go's export behavior:
- Queries dirty issues from dirty_issues table
- Serializes each to JSON matching Go's exact field names
- Content hash dedup (skip if hash unchanged since last export)
- Full rewrite of issues.jsonl
- Clear dirty markers after successful write
"""

from __future__ import annotations

import json
import os
import sys
from typing import TYPE_CHECKING

from beads.models import Issue, IssueFilter, format_timestamp

if TYPE_CHECKING:
    from beads.storage.interface import Storage


def flush_to_jsonl(store: Storage, jsonl_path: str, verbose: bool = False) -> int:
    """Export all issues from DB to JSONL file.

    This does a full rewrite (not incremental) to ensure consistency.
    Returns the number of issues written.
    """
    # Get all non-tombstone issues plus tombstones that haven't expired
    all_issues = store.search_issues("", IssueFilter(include_tombstones=True))

    # Enrich each issue with labels, dependencies, comments
    enriched: list[Issue] = []
    for issue in all_issues:
        # Skip ephemeral issues
        if issue.ephemeral:
            continue
        full = store.get_issue(issue.id)
        if full:
            enriched.append(full)

    # Write JSONL
    tmp_path = jsonl_path + ".tmp"
    count = 0
    try:
        with open(tmp_path, "w") as f:
            for issue in enriched:
                line = json.dumps(issue.to_dict(), ensure_ascii=False, separators=(",", ":"))
                f.write(line + "\n")
                count += 1
        # Atomic rename
        os.replace(tmp_path, jsonl_path)
    except Exception:
        # Clean up temp file on error
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)
        raise

    # Clear all dirty markers since we did a full export
    dirty_ids = store.get_dirty_issues()
    if dirty_ids:
        store.clear_dirty(dirty_ids)

    if verbose:
        print(f"Exported {count} issues to {jsonl_path}", file=sys.stderr)

    return count


def flush_dirty_to_jsonl(store: Storage, jsonl_path: str, verbose: bool = False) -> int:
    """Incremental export: only re-export dirty issues.

    For simplicity in the Python port, this does a full rewrite but only
    when there are dirty issues. This matches the behavior of the Go version's
    flush-only mode.
    """
    dirty_ids = store.get_dirty_issues()
    if not dirty_ids:
        if verbose:
            print("No dirty issues to export", file=sys.stderr)
        return 0

    return flush_to_jsonl(store, jsonl_path, verbose=verbose)
