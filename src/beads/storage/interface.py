"""Storage interface (abstract base) for beads."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

from beads.models import Comment, Dependency, Event, Issue, IssueFilter, Statistics


class Storage(ABC):
    """Abstract base class defining all storage operations."""

    @abstractmethod
    def path(self) -> str:
        """Return the database file path."""

    @abstractmethod
    def close(self) -> None:
        """Close the storage connection."""

    # --- Issue CRUD ---

    @abstractmethod
    def create_issue(self, issue: Issue, actor: str) -> None:
        """Create a new issue. Marks it dirty for JSONL export."""

    @abstractmethod
    def get_issue(self, issue_id: str) -> Issue | None:
        """Get an issue by ID. Returns None if not found."""

    @abstractmethod
    def update_issue(self, issue_id: str, updates: dict[str, Any], actor: str) -> None:
        """Update an issue with partial field updates."""

    @abstractmethod
    def close_issue(self, issue_id: str, reason: str, actor: str) -> None:
        """Close an issue with optional reason."""

    @abstractmethod
    def reopen_issue(self, issue_id: str, actor: str) -> None:
        """Reopen a closed issue."""

    @abstractmethod
    def delete_issue(self, issue_id: str) -> None:
        """Hard-delete an issue."""

    # --- Query ---

    @abstractmethod
    def search_issues(self, query: str, filter: IssueFilter) -> list[Issue]:
        """Search issues with text query and filters."""

    @abstractmethod
    def list_issues(self, filter: IssueFilter, sort_by: str = "created_at",
                    reverse: bool = False) -> list[Issue]:
        """List issues with filters and sorting."""

    @abstractmethod
    def get_ready_work(self, filter: dict[str, Any] | None = None) -> list[Issue]:
        """Get issues that are ready to work on (open, not blocked, not deferred)."""

    @abstractmethod
    def get_blocked_issues(self) -> list[tuple[Issue, list[str]]]:
        """Get blocked issues with their blocker IDs."""

    # --- Dependencies ---

    @abstractmethod
    def add_dependency(self, dep: Dependency, actor: str) -> None:
        """Add a dependency relationship."""

    @abstractmethod
    def remove_dependency(self, issue_id: str, depends_on_id: str, actor: str) -> None:
        """Remove a dependency relationship."""

    @abstractmethod
    def get_dependencies(self, issue_id: str) -> list[Issue]:
        """Get issues that this issue depends on."""

    @abstractmethod
    def get_dependents(self, issue_id: str) -> list[Issue]:
        """Get issues that depend on this issue."""

    @abstractmethod
    def get_dependency_records(self, issue_id: str) -> list[Dependency]:
        """Get raw dependency records for an issue."""

    @abstractmethod
    def has_cycle(self, issue_id: str, depends_on_id: str) -> bool:
        """Check if adding this dependency would create a cycle."""

    # --- Labels ---

    @abstractmethod
    def add_label(self, issue_id: str, label: str, actor: str) -> None:
        """Add a label to an issue."""

    @abstractmethod
    def remove_label(self, issue_id: str, label: str, actor: str) -> None:
        """Remove a label from an issue."""

    @abstractmethod
    def get_labels(self, issue_id: str) -> list[str]:
        """Get all labels for an issue."""

    # --- Comments ---

    @abstractmethod
    def add_comment(self, issue_id: str, author: str, text: str) -> int:
        """Add a comment. Returns the comment ID."""

    @abstractmethod
    def get_comments(self, issue_id: str) -> list[Comment]:
        """Get all comments for an issue."""

    @abstractmethod
    def import_comment(self, issue_id: str, author: str, text: str,
                       created_at: datetime) -> int:
        """Import a comment with preserved timestamp. Returns comment ID."""

    # --- Events ---

    @abstractmethod
    def get_events(self, issue_id: str) -> list[Event]:
        """Get audit trail events for an issue."""

    # --- Dirty tracking ---

    @abstractmethod
    def mark_dirty(self, issue_id: str) -> None:
        """Mark an issue as dirty (needs JSONL export)."""

    @abstractmethod
    def get_dirty_issues(self) -> list[str]:
        """Get IDs of all dirty issues."""

    @abstractmethod
    def clear_dirty(self, issue_ids: list[str]) -> None:
        """Clear dirty flags for specific issues."""

    # --- Export hashes ---

    @abstractmethod
    def get_export_hash(self, issue_id: str) -> str | None:
        """Get the last export content hash for an issue."""

    @abstractmethod
    def set_export_hash(self, issue_id: str, content_hash: str) -> None:
        """Set the export content hash for an issue."""

    @abstractmethod
    def clear_all_export_hashes(self) -> None:
        """Clear all export hashes."""

    # --- Config (DB-stored settings) ---

    @abstractmethod
    def get_config(self, key: str) -> str | None:
        """Get a config value from the DB."""

    @abstractmethod
    def set_config(self, key: str, value: str) -> None:
        """Set a config value in the DB."""

    @abstractmethod
    def list_config(self) -> dict[str, str]:
        """List all config key-value pairs."""

    # --- Metadata ---

    @abstractmethod
    def get_metadata(self, key: str) -> str | None:
        """Get a metadata value."""

    @abstractmethod
    def set_metadata(self, key: str, value: str) -> None:
        """Set a metadata value."""

    # --- Statistics ---

    @abstractmethod
    def get_statistics(self) -> Statistics:
        """Get aggregate statistics."""

    # --- Child counters ---

    @abstractmethod
    def next_child_number(self, parent_id: str) -> int:
        """Get and increment the child counter for a parent issue."""

    # --- Transactions ---

    @abstractmethod
    def run_in_transaction(self, fn: Any) -> None:
        """Run a function within a database transaction."""

    # --- Partial ID resolution ---

    @abstractmethod
    def resolve_id(self, partial: str) -> str | None:
        """Resolve a partial ID to a full ID. Returns None if ambiguous/not found."""
