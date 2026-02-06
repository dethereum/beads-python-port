"""Core data models matching Go types exactly for JSONL compatibility."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Optional


# --- Status constants ---

class Status:
    OPEN = "open"
    IN_PROGRESS = "in_progress"
    BLOCKED = "blocked"
    DEFERRED = "deferred"
    CLOSED = "closed"
    TOMBSTONE = "tombstone"
    PINNED = "pinned"
    HOOKED = "hooked"

    _VALID = {OPEN, IN_PROGRESS, BLOCKED, DEFERRED, CLOSED, TOMBSTONE, PINNED, HOOKED}

    @classmethod
    def is_valid(cls, s: str) -> bool:
        return s in cls._VALID


# --- IssueType constants ---

class IssueType:
    BUG = "bug"
    FEATURE = "feature"
    TASK = "task"
    EPIC = "epic"
    CHORE = "chore"
    EVENT = "event"  # System-internal type

    _CORE = {BUG, FEATURE, TASK, EPIC, CHORE}
    _BUILTIN = {BUG, FEATURE, TASK, EPIC, CHORE, EVENT}

    @classmethod
    def is_valid(cls, t: str) -> bool:
        return t in cls._CORE

    @classmethod
    def is_builtin(cls, t: str) -> bool:
        return t in cls._BUILTIN

    @classmethod
    def normalize(cls, t: str) -> str:
        lower = t.lower()
        if lower in ("enhancement", "feat"):
            return cls.FEATURE
        return t


# --- DependencyType constants ---

class DepType:
    BLOCKS = "blocks"
    PARENT_CHILD = "parent-child"
    CONDITIONAL_BLOCKS = "conditional-blocks"
    WAITS_FOR = "waits-for"
    RELATED = "related"
    DISCOVERED_FROM = "discovered-from"
    REPLIES_TO = "replies-to"
    RELATES_TO = "relates-to"
    DUPLICATES = "duplicates"
    SUPERSEDES = "supersedes"
    AUTHORED_BY = "authored-by"
    ASSIGNED_TO = "assigned-to"
    APPROVED_BY = "approved-by"
    ATTESTS = "attests"
    TRACKS = "tracks"
    UNTIL = "until"
    CAUSED_BY = "caused-by"
    VALIDATES = "validates"
    DELEGATED_FROM = "delegated-from"

    _BLOCKING = {BLOCKS, PARENT_CHILD, CONDITIONAL_BLOCKS, WAITS_FOR}

    @classmethod
    def affects_ready_work(cls, dep_type: str) -> bool:
        return dep_type in cls._BLOCKING


# --- EventType constants ---

class EventType:
    CREATED = "created"
    UPDATED = "updated"
    STATUS_CHANGED = "status_changed"
    COMMENTED = "commented"
    CLOSED = "closed"
    REOPENED = "reopened"
    DEPENDENCY_ADDED = "dependency_added"
    DEPENDENCY_REMOVED = "dependency_removed"
    LABEL_ADDED = "label_added"
    LABEL_REMOVED = "label_removed"
    COMPACTED = "compacted"


# --- Helper: RFC3339 timestamp handling ---

def parse_timestamp(s: str | None) -> datetime | None:
    """Parse RFC3339 timestamp string to datetime."""
    if not s:
        return None
    # Handle Go's RFC3339Nano format and standard RFC3339
    s = s.strip()
    if not s:
        return None
    try:
        # Python 3.11+ fromisoformat handles timezone
        return datetime.fromisoformat(s)
    except ValueError:
        pass
    # Handle Z suffix
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(s)
        except ValueError:
            pass
    # Fallback: try common formats
    for fmt in ("%Y-%m-%dT%H:%M:%S%z", "%Y-%m-%dT%H:%M:%S.%f%z", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(s, fmt)
        except ValueError:
            continue
    raise ValueError(f"Cannot parse timestamp: {s}")


def format_timestamp(dt: datetime | None) -> str | None:
    """Format datetime to RFC3339 string."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Use ISO format with Z suffix for UTC
    s = dt.isoformat()
    if s.endswith("+00:00"):
        s = s[:-6] + "Z"
    return s


def now_utc() -> datetime:
    """Current time in UTC."""
    return datetime.now(timezone.utc)


# --- Dataclasses ---

@dataclass
class Dependency:
    issue_id: str
    depends_on_id: str
    type: str = DepType.BLOCKS
    created_at: datetime = field(default_factory=now_utc)
    created_by: str = ""
    metadata: str = ""
    thread_id: str = ""

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "issue_id": self.issue_id,
            "depends_on_id": self.depends_on_id,
            "type": self.type,
            "created_at": format_timestamp(self.created_at),
        }
        if self.created_by:
            d["created_by"] = self.created_by
        if self.metadata:
            d["metadata"] = self.metadata
        if self.thread_id:
            d["thread_id"] = self.thread_id
        return d

    @classmethod
    def from_dict(cls, d: dict) -> Dependency:
        return cls(
            issue_id=d["issue_id"],
            depends_on_id=d["depends_on_id"],
            type=d.get("type", DepType.BLOCKS),
            created_at=parse_timestamp(d.get("created_at")) or now_utc(),
            created_by=d.get("created_by", ""),
            metadata=d.get("metadata", ""),
            thread_id=d.get("thread_id", ""),
        )


@dataclass
class Comment:
    id: int = 0
    issue_id: str = ""
    author: str = ""
    text: str = ""
    created_at: datetime = field(default_factory=now_utc)

    def to_dict(self) -> dict:
        d: dict[str, Any] = {
            "id": self.id,
            "issue_id": self.issue_id,
            "author": self.author,
            "text": self.text,
            "created_at": format_timestamp(self.created_at),
        }
        return d

    @classmethod
    def from_dict(cls, d: dict) -> Comment:
        return cls(
            id=d.get("id", 0),
            issue_id=d.get("issue_id", ""),
            author=d.get("author", ""),
            text=d.get("text", ""),
            created_at=parse_timestamp(d.get("created_at")) or now_utc(),
        )


@dataclass
class Event:
    id: int = 0
    issue_id: str = ""
    event_type: str = ""
    actor: str = ""
    old_value: str | None = None
    new_value: str | None = None
    comment: str | None = None
    created_at: datetime = field(default_factory=now_utc)


@dataclass
class Issue:
    """Issue model matching Go's types.Issue for JSONL compatibility."""

    # Core identification
    id: str = ""
    content_hash: str = ""  # Internal, not exported to JSONL

    # Issue content
    title: str = ""
    description: str = ""
    design: str = ""
    acceptance_criteria: str = ""
    notes: str = ""
    spec_id: str = ""

    # Status & workflow
    status: str = Status.OPEN
    priority: int = 2
    issue_type: str = IssueType.TASK

    # Assignment
    assignee: str = ""
    owner: str = ""
    estimated_minutes: int | None = None

    # Timestamps
    created_at: datetime = field(default_factory=now_utc)
    created_by: str = ""
    updated_at: datetime = field(default_factory=now_utc)
    closed_at: datetime | None = None
    close_reason: str = ""
    closed_by_session: str = ""

    # Time-based scheduling
    due_at: datetime | None = None
    defer_until: datetime | None = None

    # External integration
    external_ref: str | None = None
    source_system: str = ""

    # Custom metadata
    metadata: str = ""  # JSON string

    # Compaction metadata
    compaction_level: int = 0
    compacted_at: datetime | None = None
    compacted_at_commit: str | None = None
    original_size: int = 0

    # Relational data (populated for export/import)
    labels: list[str] = field(default_factory=list)
    dependencies: list[Dependency] = field(default_factory=list)
    comments: list[Comment] = field(default_factory=list)

    # Tombstone fields
    deleted_at: datetime | None = None
    deleted_by: str = ""
    delete_reason: str = ""
    original_type: str = ""

    # Messaging fields
    sender: str = ""
    ephemeral: bool = False
    wisp_type: str = ""

    # Context markers
    pinned: bool = False
    is_template: bool = False

    # Bonded molecules
    bonded_from: list[dict] = field(default_factory=list)

    # HOP fields
    creator: dict | None = None
    validations: list[dict] = field(default_factory=list)
    quality_score: float | None = None
    crystallizes: bool = False

    # Gate fields
    await_type: str = ""
    await_id: str = ""
    timeout: int = 0  # Duration in nanoseconds (Go time.Duration)
    waiters: list[str] = field(default_factory=list)

    # Slot fields
    holder: str = ""

    # Agent identity fields
    hook_bead: str = ""
    role_bead: str = ""
    agent_state: str = ""
    role_type: str = ""
    rig: str = ""
    last_activity: datetime | None = None

    # Molecule type
    mol_type: str = ""

    # Work type
    work_type: str = ""

    # Event fields
    event_kind: str = ""
    actor: str = ""
    target: str = ""
    payload: str = ""

    def compute_content_hash(self) -> str:
        """Compute deterministic content hash matching Go's ComputeContentHash.

        Uses SHA256 with null-separated fields in the exact same order as Go.
        """
        h = hashlib.sha256()

        def write_str(s: str) -> None:
            h.update(s.encode("utf-8"))
            h.update(b"\x00")

        def write_int(n: int) -> None:
            h.update(str(n).encode("utf-8"))
            h.update(b"\x00")

        def write_str_optional(s: str | None) -> None:
            if s is not None:
                h.update(s.encode("utf-8"))
            h.update(b"\x00")

        def write_flag(b: bool, label: str) -> None:
            if b:
                h.update(label.encode("utf-8"))
            h.update(b"\x00")

        def write_float_optional(f: float | None) -> None:
            if f is not None:
                # Match Go's %f formatting (6 decimal places)
                h.update(f"{f:f}".encode("utf-8"))
            h.update(b"\x00")

        def write_duration(d: int) -> None:
            h.update(str(d).encode("utf-8"))
            h.update(b"\x00")

        def write_entity_ref(e: dict | None) -> None:
            if e is not None:
                write_str(e.get("name", ""))
                write_str(e.get("platform", ""))
                write_str(e.get("org", ""))
                write_str(e.get("id", ""))

        # Core fields in stable order (must match Go exactly)
        write_str(self.title)
        write_str(self.description)
        write_str(self.design)
        write_str(self.acceptance_criteria)
        write_str(self.notes)
        write_str(self.spec_id)
        write_str(self.status)
        write_int(self.priority)
        write_str(self.issue_type)
        write_str(self.assignee)
        write_str(self.owner)
        write_str(self.created_by)

        # Optional fields
        write_str_optional(self.external_ref)
        write_str(self.source_system)
        write_flag(self.pinned, "pinned")
        write_str(self.metadata)  # Include metadata in content hash
        write_flag(self.is_template, "template")

        # Bonded molecules
        for br in self.bonded_from:
            write_str(br.get("source_id", ""))
            write_str(br.get("bond_type", ""))
            write_str(br.get("bond_point", ""))

        # HOP entity tracking
        write_entity_ref(self.creator)

        # HOP validations
        for v in self.validations:
            write_entity_ref(v.get("validator"))
            write_str(v.get("outcome", ""))
            ts = v.get("timestamp", "")
            if isinstance(ts, datetime):
                ts = ts.isoformat()
                if ts.endswith("+00:00"):
                    ts = ts[:-6] + "Z"
            write_str(ts)
            write_float_optional(v.get("score"))

        # HOP aggregate quality score and crystallizes
        write_float_optional(self.quality_score)
        write_flag(self.crystallizes, "crystallizes")

        # Gate fields
        write_str(self.await_type)
        write_str(self.await_id)
        write_duration(self.timeout)
        for waiter in self.waiters:
            write_str(waiter)

        # Slot fields
        write_str(self.holder)

        # Agent identity fields
        write_str(self.hook_bead)
        write_str(self.role_bead)
        write_str(self.agent_state)
        write_str(self.role_type)
        write_str(self.rig)

        # Molecule type
        write_str(self.mol_type)

        # Work type
        write_str(self.work_type)

        # Event fields
        write_str(self.event_kind)
        write_str(self.actor)
        write_str(self.target)
        write_str(self.payload)

        return h.hexdigest()

    def is_tombstone(self) -> bool:
        return self.status == Status.TOMBSTONE

    def validate(self) -> str | None:
        """Validate issue fields. Returns error message or None if valid."""
        if not self.title:
            return "title is required"
        if len(self.title) > 500:
            return f"title must be 500 characters or less (got {len(self.title)})"
        if self.priority < 0 or self.priority > 4:
            return f"priority must be between 0 and 4 (got {self.priority})"
        if self.status and not Status.is_valid(self.status):
            return f"invalid status: {self.status}"
        if self.issue_type and not IssueType.is_valid(self.issue_type):
            return f"invalid issue type: {self.issue_type}"
        if self.estimated_minutes is not None and self.estimated_minutes < 0:
            return "estimated_minutes cannot be negative"
        if self.status == Status.CLOSED and self.closed_at is None:
            return "closed issues must have closed_at timestamp"
        if self.status not in (Status.CLOSED, Status.TOMBSTONE) and self.closed_at is not None:
            return "non-closed issues cannot have closed_at timestamp"
        if self.status == Status.TOMBSTONE and self.deleted_at is None:
            return "tombstone issues must have deleted_at timestamp"
        if self.status != Status.TOMBSTONE and self.deleted_at is not None:
            return "non-tombstone issues cannot have deleted_at timestamp"
        if self.metadata and self.metadata != "{}":
            try:
                json.loads(self.metadata)
            except (json.JSONDecodeError, TypeError):
                return "metadata must be valid JSON"
        return None

    def set_defaults(self) -> None:
        """Apply default values for fields omitted during JSONL import."""
        if not self.status:
            self.status = Status.OPEN
        if not self.issue_type:
            self.issue_type = IssueType.TASK

    def to_dict(self) -> dict:
        """Serialize to dict matching Go's JSON field names with omitempty behavior."""
        d: dict[str, Any] = {
            "id": self.id,
            "title": self.title,
        }

        # Fields with omitempty in Go
        if self.description:
            d["description"] = self.description
        if self.design:
            d["design"] = self.design
        if self.acceptance_criteria:
            d["acceptance_criteria"] = self.acceptance_criteria
        if self.notes:
            d["notes"] = self.notes
        if self.spec_id:
            d["spec_id"] = self.spec_id
        if self.status:
            d["status"] = self.status
        # priority has no omitempty in Go - 0 is valid (P0)
        d["priority"] = self.priority
        if self.issue_type:
            d["issue_type"] = self.issue_type
        if self.assignee:
            d["assignee"] = self.assignee
        if self.owner:
            d["owner"] = self.owner
        if self.estimated_minutes is not None:
            d["estimated_minutes"] = self.estimated_minutes

        # Timestamps
        d["created_at"] = format_timestamp(self.created_at)
        if self.created_by:
            d["created_by"] = self.created_by
        d["updated_at"] = format_timestamp(self.updated_at)
        if self.closed_at:
            d["closed_at"] = format_timestamp(self.closed_at)
        if self.close_reason:
            d["close_reason"] = self.close_reason
        if self.closed_by_session:
            d["closed_by_session"] = self.closed_by_session

        # Time-based scheduling
        if self.due_at:
            d["due_at"] = format_timestamp(self.due_at)
        if self.defer_until:
            d["defer_until"] = format_timestamp(self.defer_until)

        # External integration
        if self.external_ref:
            d["external_ref"] = self.external_ref
        if self.source_system:
            d["source_system"] = self.source_system

        # Metadata
        if self.metadata and self.metadata != "{}":
            d["metadata"] = json.loads(self.metadata) if isinstance(self.metadata, str) else self.metadata

        # Compaction
        if self.compaction_level:
            d["compaction_level"] = self.compaction_level
        if self.compacted_at:
            d["compacted_at"] = format_timestamp(self.compacted_at)
        if self.compacted_at_commit:
            d["compacted_at_commit"] = self.compacted_at_commit
        if self.original_size:
            d["original_size"] = self.original_size

        # Relational data
        if self.labels:
            d["labels"] = self.labels
        if self.dependencies:
            d["dependencies"] = [dep.to_dict() for dep in self.dependencies]
        if self.comments:
            d["comments"] = [c.to_dict() for c in self.comments]

        # Tombstone
        if self.deleted_at:
            d["deleted_at"] = format_timestamp(self.deleted_at)
        if self.deleted_by:
            d["deleted_by"] = self.deleted_by
        if self.delete_reason:
            d["delete_reason"] = self.delete_reason
        if self.original_type:
            d["original_type"] = self.original_type

        # Messaging
        if self.sender:
            d["sender"] = self.sender
        if self.ephemeral:
            d["ephemeral"] = self.ephemeral
        if self.wisp_type:
            d["wisp_type"] = self.wisp_type

        # Context markers
        if self.pinned:
            d["pinned"] = self.pinned
        if self.is_template:
            d["is_template"] = self.is_template

        # Bonded molecules
        if self.bonded_from:
            d["bonded_from"] = self.bonded_from

        # HOP fields
        if self.creator:
            d["creator"] = self.creator
        if self.validations:
            d["validations"] = self.validations
        if self.quality_score is not None:
            d["quality_score"] = self.quality_score
        if self.crystallizes:
            d["crystallizes"] = self.crystallizes

        # Gate fields
        if self.await_type:
            d["await_type"] = self.await_type
        if self.await_id:
            d["await_id"] = self.await_id
        if self.timeout:
            d["timeout"] = self.timeout
        if self.waiters:
            d["waiters"] = self.waiters

        # Slot
        if self.holder:
            d["holder"] = self.holder

        # Agent identity
        if self.hook_bead:
            d["hook_bead"] = self.hook_bead
        if self.role_bead:
            d["role_bead"] = self.role_bead
        if self.agent_state:
            d["agent_state"] = self.agent_state
        if self.last_activity:
            d["last_activity"] = format_timestamp(self.last_activity)
        if self.role_type:
            d["role_type"] = self.role_type
        if self.rig:
            d["rig"] = self.rig

        # Molecule type
        if self.mol_type:
            d["mol_type"] = self.mol_type
        if self.work_type:
            d["work_type"] = self.work_type

        # Event fields
        if self.event_kind:
            d["event_kind"] = self.event_kind
        if self.actor:
            d["actor"] = self.actor
        if self.target:
            d["target"] = self.target
        if self.payload:
            d["payload"] = self.payload

        return d

    @classmethod
    def from_dict(cls, d: dict) -> Issue:
        """Deserialize from dict (JSONL line)."""
        issue = cls()
        issue.id = d.get("id", "")
        issue.title = d.get("title", "")
        issue.description = d.get("description", "")
        issue.design = d.get("design", "")
        issue.acceptance_criteria = d.get("acceptance_criteria", "")
        issue.notes = d.get("notes", "")
        issue.spec_id = d.get("spec_id", "")
        issue.status = d.get("status", "")
        issue.priority = d.get("priority", 0)
        issue.issue_type = d.get("issue_type", "")
        issue.assignee = d.get("assignee", "")
        issue.owner = d.get("owner", "")
        issue.estimated_minutes = d.get("estimated_minutes")
        issue.created_at = parse_timestamp(d.get("created_at")) or now_utc()
        issue.created_by = d.get("created_by", "")
        issue.updated_at = parse_timestamp(d.get("updated_at")) or now_utc()
        issue.closed_at = parse_timestamp(d.get("closed_at"))
        issue.close_reason = d.get("close_reason", "")
        issue.closed_by_session = d.get("closed_by_session", "")
        issue.due_at = parse_timestamp(d.get("due_at"))
        issue.defer_until = parse_timestamp(d.get("defer_until"))
        issue.external_ref = d.get("external_ref")
        issue.source_system = d.get("source_system", "")

        md = d.get("metadata")
        if md is not None:
            if isinstance(md, str):
                issue.metadata = md
            else:
                issue.metadata = json.dumps(md)
        else:
            issue.metadata = ""

        issue.compaction_level = d.get("compaction_level", 0)
        issue.compacted_at = parse_timestamp(d.get("compacted_at"))
        issue.compacted_at_commit = d.get("compacted_at_commit")
        issue.original_size = d.get("original_size", 0)

        issue.labels = d.get("labels", []) or []
        issue.dependencies = [
            Dependency.from_dict(dep) for dep in (d.get("dependencies") or [])
        ]
        issue.comments = [
            Comment.from_dict(c) for c in (d.get("comments") or [])
        ]

        issue.deleted_at = parse_timestamp(d.get("deleted_at"))
        issue.deleted_by = d.get("deleted_by", "")
        issue.delete_reason = d.get("delete_reason", "")
        issue.original_type = d.get("original_type", "")

        issue.sender = d.get("sender", "")
        issue.ephemeral = bool(d.get("ephemeral", False))
        issue.wisp_type = d.get("wisp_type", "")

        issue.pinned = bool(d.get("pinned", False))
        issue.is_template = bool(d.get("is_template", False))

        issue.bonded_from = d.get("bonded_from", []) or []
        issue.creator = d.get("creator")
        issue.validations = d.get("validations", []) or []
        issue.quality_score = d.get("quality_score")
        issue.crystallizes = bool(d.get("crystallizes", False))

        issue.await_type = d.get("await_type", "")
        issue.await_id = d.get("await_id", "")
        issue.timeout = d.get("timeout", 0) or 0
        issue.waiters = d.get("waiters", []) or []

        issue.holder = d.get("holder", "")

        issue.hook_bead = d.get("hook_bead", "")
        issue.role_bead = d.get("role_bead", "")
        issue.agent_state = d.get("agent_state", "")
        issue.last_activity = parse_timestamp(d.get("last_activity"))
        issue.role_type = d.get("role_type", "")
        issue.rig = d.get("rig", "")

        issue.mol_type = d.get("mol_type", "")
        issue.work_type = d.get("work_type", "")

        issue.event_kind = d.get("event_kind", "")
        issue.actor = d.get("actor", "")
        issue.target = d.get("target", "")
        issue.payload = d.get("payload", "")

        issue.set_defaults()
        return issue


@dataclass
class IssueFilter:
    """Filter for issue queries."""
    status: str | None = None
    priority: int | None = None
    issue_type: str | None = None
    assignee: str | None = None
    labels: list[str] = field(default_factory=list)
    labels_any: list[str] = field(default_factory=list)
    title_search: str = ""
    ids: list[str] = field(default_factory=list)
    id_prefix: str = ""
    limit: int = 0
    title_contains: str = ""
    description_contains: str = ""
    notes_contains: str = ""
    created_after: datetime | None = None
    created_before: datetime | None = None
    updated_after: datetime | None = None
    updated_before: datetime | None = None
    closed_after: datetime | None = None
    closed_before: datetime | None = None
    no_assignee: bool = False
    priority_min: int | None = None
    priority_max: int | None = None
    include_tombstones: bool = False
    ephemeral: bool | None = None
    pinned: bool | None = None
    is_template: bool | None = None
    parent_id: str | None = None
    exclude_status: list[str] = field(default_factory=list)
    exclude_types: list[str] = field(default_factory=list)
    deferred: bool = False
    overdue: bool = False


@dataclass
class Statistics:
    total_issues: int = 0
    open_issues: int = 0
    in_progress_issues: int = 0
    closed_issues: int = 0
    blocked_issues: int = 0
    deferred_issues: int = 0
    ready_issues: int = 0
    tombstone_issues: int = 0
    pinned_issues: int = 0
    by_type: dict[str, int] = field(default_factory=dict)
    by_priority: dict[int, int] = field(default_factory=dict)
