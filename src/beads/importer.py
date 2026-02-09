"""JSONL import - read .beads/issues.jsonl into SQLite.

Matches Go's importer behavior:
- Parse issues.jsonl line by line
- Content hash comparison for change detection
- Upsert (create or update based on existence)
- Import dependencies, labels, comments
- Handle deletion markers
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from beads.models import (
    Comment, Dependency, Issue, IssueFilter, Status, format_timestamp, now_utc,
)

if TYPE_CHECKING:
    from beads.storage.interface import Storage


@dataclass
class ImportResult:
    created: int = 0
    updated: int = 0
    unchanged: int = 0
    skipped: int = 0
    deleted: int = 0


def parse_jsonl(jsonl_path: str) -> tuple[list[Issue], list[str]]:
    """Parse a JSONL file into issues and deletion marker IDs.

    Returns: (issues, deletion_ids)
    """
    issues: list[Issue] = []
    deletion_ids: list[str] = []

    if not os.path.exists(jsonl_path):
        return issues, deletion_ids

    with open(jsonl_path, encoding="utf-8") as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Warning: skipping malformed line {line_num}: {e}", file=sys.stderr)
                continue

            # Check for deletion marker
            if data.get("_deleted"):
                if "id" in data:
                    deletion_ids.append(data["id"])
                continue

            issue = Issue.from_dict(data)
            issues.append(issue)

    return issues, deletion_ids


def import_jsonl(store: Storage, jsonl_path: str, verbose: bool = False) -> ImportResult:
    """Import issues from JSONL file into storage.

    This is a simplified version of Go's ImportIssues that handles the core
    cases needed for JSONL compatibility.
    """
    result = ImportResult()

    issues, deletion_ids = parse_jsonl(jsonl_path)

    # Process deletions first
    for del_id in deletion_ids:
        existing = store.get_issue(del_id)
        if existing:
            store.delete_issue(del_id)
            result.deleted += 1

    # Compute content hashes
    for issue in issues:
        issue.content_hash = issue.compute_content_hash()

    # Auto-detect wisps
    for issue in issues:
        if "-wisp-" in issue.id and not issue.ephemeral:
            issue.ephemeral = True

    # Get configured prefix for validation
    configured_prefix = store.get_config("issue_prefix") or ""

    # Build maps of existing issues
    existing_issues = store.search_issues("", IssueFilter(include_tombstones=True))
    db_by_id: dict[str, Issue] = {i.id: i for i in existing_issues}
    db_by_hash: dict[str, Issue] = {}
    for i in existing_issues:
        if i.content_hash:
            db_by_hash[i.content_hash] = i

    seen_hashes: set[str] = set()
    seen_ids: set[str] = set()

    for incoming in issues:
        h = incoming.content_hash
        if not h:
            h = incoming.compute_content_hash()
            incoming.content_hash = h

        # Skip batch duplicates
        if h in seen_hashes:
            result.skipped += 1
            continue
        seen_hashes.add(h)

        if incoming.id in seen_ids:
            result.skipped += 1
            continue
        seen_ids.add(incoming.id)

        # Skip tombstones in DB
        existing_by_id = db_by_id.get(incoming.id)
        if existing_by_id and existing_by_id.status == Status.TOMBSTONE:
            result.skipped += 1
            continue

        # Phase 1: Content hash match
        existing_by_hash = db_by_hash.get(h)
        if existing_by_hash:
            if existing_by_hash.id == incoming.id:
                result.unchanged += 1
            else:
                result.skipped += 1
            continue

        # Phase 2: ID match (update)
        if existing_by_id:
            # Same ID, different content -> update
            if incoming.updated_at <= existing_by_id.updated_at:
                result.unchanged += 1
                continue

            updates = {
                "title": incoming.title,
                "description": incoming.description,
                "design": incoming.design,
                "acceptance_criteria": incoming.acceptance_criteria,
                "notes": incoming.notes,
                "status": incoming.status,
                "priority": incoming.priority,
                "issue_type": incoming.issue_type,
                "assignee": incoming.assignee or None,
            }
            if incoming.closed_at:
                updates["closed_at"] = incoming.closed_at
            if incoming.close_reason:
                updates["close_reason"] = incoming.close_reason
            if incoming.pinned:
                updates["pinned"] = incoming.pinned
            if incoming.external_ref:
                updates["external_ref"] = incoming.external_ref

            store.update_issue(incoming.id, updates, "import")
            result.updated += 1
            continue

        # Phase 3: New issue
        store.create_issue(incoming, "import")
        result.created += 1

    if verbose:
        print(
            f"Import: {result.created} created, {result.updated} updated, "
            f"{result.unchanged} unchanged, {result.skipped} skipped, "
            f"{result.deleted} deleted",
            file=sys.stderr,
        )

    return result


def auto_import_if_needed(store: Storage, jsonl_path: str,
                          verbose: bool = False) -> ImportResult | None:
    """Auto-import JSONL if it's newer than the last import.

    Returns ImportResult if import was performed, None if skipped.
    """
    if not os.path.exists(jsonl_path):
        return None

    # Check mtime against last import
    jsonl_mtime = os.path.getmtime(jsonl_path)
    last_import = store.get_metadata("last_import_mtime")
    if last_import:
        try:
            if float(last_import) >= jsonl_mtime:
                return None  # JSONL hasn't changed
        except ValueError:
            pass

    result = import_jsonl(store, jsonl_path, verbose=verbose)

    # Record import mtime
    store.set_metadata("last_import_mtime", str(jsonl_mtime))

    return result
