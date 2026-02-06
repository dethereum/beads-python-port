"""SQLite storage implementation for beads."""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from typing import Any

from beads.models import (
    Comment, Dependency, DepType, Event, EventType, Issue, IssueFilter,
    Statistics, Status, format_timestamp, now_utc, parse_timestamp,
)
from beads.storage.interface import Storage
from beads.storage.schema import SCHEMA


class SQLiteStorage(Storage):
    """SQLite-based storage backend."""

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._conn = sqlite3.connect(db_path)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._conn.execute("PRAGMA busy_timeout=5000")
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema.

        If the database already exists (e.g., created by Go version), we only
        add missing tables and columns. If it's a fresh DB, we create everything.
        """
        # Check if this is an existing DB by looking for the issues table
        row = self._conn.execute(
            "SELECT count(*) FROM sqlite_master WHERE type='table' AND name='issues'"
        ).fetchone()
        is_existing = row[0] > 0

        if is_existing:
            self._migrate_existing_schema()
        else:
            self._conn.executescript(SCHEMA)
            self._conn.commit()

    def _migrate_existing_schema(self) -> None:
        """Add missing tables and columns to an existing Go-created database."""
        # Get existing columns in issues table
        existing_cols = set()
        for row in self._conn.execute("PRAGMA table_info(issues)").fetchall():
            existing_cols.add(row[1])  # column name

        # Columns that may be missing from older Go DBs or need to be added
        issue_columns = [
            ("spec_id", "TEXT"),
            ("close_reason", "TEXT DEFAULT ''"),
            ("due_at", "DATETIME"),
            ("defer_until", "DATETIME"),
            ("hook_bead", "TEXT DEFAULT ''"),
            ("role_bead", "TEXT DEFAULT ''"),
            ("agent_state", "TEXT DEFAULT ''"),
            ("last_activity", "DATETIME"),
            ("role_type", "TEXT DEFAULT ''"),
            ("rig", "TEXT DEFAULT ''"),
            ("bonded_from", "TEXT DEFAULT '[]'"),
            ("creator_json", "TEXT DEFAULT ''"),
            ("validations_json", "TEXT DEFAULT '[]'"),
            ("await_type", "TEXT DEFAULT ''"),
            ("await_id", "TEXT DEFAULT ''"),
            ("timeout", "INTEGER DEFAULT 0"),
            ("waiters_json", "TEXT DEFAULT '[]'"),
            ("holder", "TEXT DEFAULT ''"),
        ]

        for col_name, col_type in issue_columns:
            if col_name not in existing_cols:
                try:
                    self._conn.execute(f"ALTER TABLE issues ADD COLUMN {col_name} {col_type}")
                except Exception:
                    pass  # Column may already exist

        # Create missing tables (IF NOT EXISTS handles idempotency)
        missing_tables_sql = """
        CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS dirty_issues (
            issue_id TEXT PRIMARY KEY,
            marked_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS export_hashes (
            issue_id TEXT PRIMARY KEY,
            content_hash TEXT NOT NULL,
            exported_at DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
        CREATE TABLE IF NOT EXISTS child_counters (
            parent_id TEXT PRIMARY KEY,
            last_child INTEGER NOT NULL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_dirty_issues_marked_at ON dirty_issues(marked_at);
        """
        self._conn.executescript(missing_tables_sql)
        self._conn.commit()

    def path(self) -> str:
        return self._db_path

    def close(self) -> None:
        self._conn.close()

    # --- Helpers ---

    def _row_to_issue(self, row: sqlite3.Row) -> Issue:
        """Convert a database row to an Issue object.

        Handles missing columns gracefully for Go-created databases.
        """
        issue = Issue()
        keys = set(row.keys())

        def get(col: str, default: Any = "") -> Any:
            return row[col] if col in keys else default

        issue.id = row["id"]
        issue.content_hash = get("content_hash") or ""
        issue.title = row["title"]
        issue.description = get("description") or ""
        issue.design = get("design") or ""
        issue.acceptance_criteria = get("acceptance_criteria") or ""
        issue.notes = get("notes") or ""
        issue.status = get("status") or Status.OPEN
        issue.priority = row["priority"]
        issue.issue_type = get("issue_type") or "task"
        issue.assignee = get("assignee") or ""
        issue.estimated_minutes = get("estimated_minutes", None)
        issue.created_at = parse_timestamp(get("created_at")) or now_utc()
        issue.created_by = get("created_by") or ""
        issue.owner = get("owner") or ""
        issue.updated_at = parse_timestamp(get("updated_at")) or now_utc()
        issue.closed_at = parse_timestamp(get("closed_at", None))
        issue.closed_by_session = get("closed_by_session") or ""
        issue.close_reason = get("close_reason") or ""
        issue.external_ref = get("external_ref", None)
        issue.spec_id = get("spec_id") or ""
        issue.compaction_level = get("compaction_level", 0) or 0
        issue.compacted_at = parse_timestamp(get("compacted_at", None))
        issue.compacted_at_commit = get("compacted_at_commit", None)
        issue.original_size = get("original_size", 0) or 0
        issue.deleted_at = parse_timestamp(get("deleted_at", None))
        issue.deleted_by = get("deleted_by") or ""
        issue.delete_reason = get("delete_reason") or ""
        issue.original_type = get("original_type") or ""
        issue.sender = get("sender") or ""
        issue.ephemeral = bool(get("ephemeral", 0))
        issue.wisp_type = get("wisp_type") or ""
        issue.pinned = bool(get("pinned", 0))
        issue.is_template = bool(get("is_template", 0))
        issue.crystallizes = bool(get("crystallizes", 0))
        issue.mol_type = get("mol_type") or ""
        issue.work_type = get("work_type") or ""
        issue.quality_score = get("quality_score", None)
        issue.source_system = get("source_system") or ""
        issue.metadata = get("metadata") or ""
        issue.event_kind = get("event_kind") or ""
        issue.actor = get("actor") or ""
        issue.target = get("target") or ""
        issue.payload = get("payload") or ""
        issue.due_at = parse_timestamp(get("due_at", None))
        issue.defer_until = parse_timestamp(get("defer_until", None))
        issue.hook_bead = get("hook_bead") or ""
        issue.role_bead = get("role_bead") or ""
        issue.agent_state = get("agent_state") or ""
        issue.last_activity = parse_timestamp(get("last_activity", None))
        issue.role_type = get("role_type") or ""
        issue.rig = get("rig") or ""
        issue.await_type = get("await_type") or ""
        issue.await_id = get("await_id") or ""
        issue.timeout = get("timeout", 0) or 0
        issue.holder = get("holder") or ""

        # JSON-stored fields (may not exist in Go DBs)
        bf = get("bonded_from", "[]") or "[]"
        try:
            issue.bonded_from = json.loads(bf) if bf else []
        except (json.JSONDecodeError, TypeError):
            issue.bonded_from = []

        cj = get("creator_json", "") or ""
        try:
            issue.creator = json.loads(cj) if cj else None
        except (json.JSONDecodeError, TypeError):
            issue.creator = None

        vj = get("validations_json", "[]") or "[]"
        try:
            issue.validations = json.loads(vj) if vj else []
        except (json.JSONDecodeError, TypeError):
            issue.validations = []

        wj = get("waiters_json", "[]") or "[]"
        try:
            issue.waiters = json.loads(wj) if wj else []
        except (json.JSONDecodeError, TypeError):
            issue.waiters = []

        return issue

    def _record_event(self, issue_id: str, event_type: str, actor: str,
                      old_value: str | None = None, new_value: str | None = None,
                      comment: str | None = None) -> None:
        """Record an audit trail event."""
        self._conn.execute(
            "INSERT INTO events (issue_id, event_type, actor, old_value, new_value, comment, created_at) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (issue_id, event_type, actor, old_value, new_value, comment,
             format_timestamp(now_utc()))
        )

    # --- Issue CRUD ---

    def create_issue(self, issue: Issue, actor: str) -> None:
        issue.content_hash = issue.compute_content_hash()
        now = now_utc()
        if not issue.created_at:
            issue.created_at = now
        if not issue.updated_at:
            issue.updated_at = now

        self._conn.execute(
            """INSERT INTO issues (
                id, content_hash, title, description, design, acceptance_criteria,
                notes, status, priority, issue_type, assignee, estimated_minutes,
                created_at, created_by, owner, updated_at, closed_at, closed_by_session,
                close_reason, external_ref, spec_id, compaction_level, compacted_at,
                compacted_at_commit, original_size, deleted_at, deleted_by,
                delete_reason, original_type, sender, ephemeral, wisp_type,
                pinned, is_template, crystallizes, mol_type, work_type,
                quality_score, source_system, metadata, event_kind, actor, target,
                payload, due_at, defer_until, hook_bead, role_bead, agent_state,
                last_activity, role_type, rig, bonded_from, creator_json,
                validations_json, await_type, await_id, timeout, waiters_json, holder
            ) VALUES (
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?
            )""",
            (
                issue.id, issue.content_hash, issue.title, issue.description,
                issue.design, issue.acceptance_criteria, issue.notes,
                issue.status, issue.priority, issue.issue_type,
                issue.assignee or None, issue.estimated_minutes,
                format_timestamp(issue.created_at), issue.created_by,
                issue.owner, format_timestamp(issue.updated_at),
                format_timestamp(issue.closed_at),
                issue.closed_by_session, issue.close_reason,
                issue.external_ref, issue.spec_id,
                issue.compaction_level, format_timestamp(issue.compacted_at),
                issue.compacted_at_commit, issue.original_size,
                format_timestamp(issue.deleted_at), issue.deleted_by,
                issue.delete_reason, issue.original_type,
                issue.sender, int(issue.ephemeral), issue.wisp_type,
                int(issue.pinned), int(issue.is_template),
                int(issue.crystallizes), issue.mol_type, issue.work_type,
                issue.quality_score, issue.source_system,
                issue.metadata or "{}", issue.event_kind, issue.actor,
                issue.target, issue.payload,
                format_timestamp(issue.due_at), format_timestamp(issue.defer_until),
                issue.hook_bead, issue.role_bead, issue.agent_state,
                format_timestamp(issue.last_activity), issue.role_type, issue.rig,
                json.dumps(issue.bonded_from),
                json.dumps(issue.creator) if issue.creator else "",
                json.dumps(issue.validations),
                issue.await_type, issue.await_id, issue.timeout,
                json.dumps(issue.waiters), issue.holder,
            )
        )

        # Import labels
        for label in issue.labels:
            self._conn.execute(
                "INSERT OR IGNORE INTO labels (issue_id, label) VALUES (?, ?)",
                (issue.id, label)
            )

        # Import dependencies
        for dep in issue.dependencies:
            try:
                self._conn.execute(
                    "INSERT OR IGNORE INTO dependencies (issue_id, depends_on_id, type, created_at, created_by, metadata, thread_id) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (dep.issue_id, dep.depends_on_id, dep.type,
                     format_timestamp(dep.created_at), dep.created_by,
                     dep.metadata, dep.thread_id)
                )
            except sqlite3.IntegrityError:
                pass  # FK constraint - target doesn't exist yet

        # Import comments
        for comment in issue.comments:
            self._conn.execute(
                "INSERT INTO comments (issue_id, author, text, created_at) VALUES (?, ?, ?, ?)",
                (comment.issue_id or issue.id, comment.author, comment.text,
                 format_timestamp(comment.created_at))
            )

        self._record_event(issue.id, EventType.CREATED, actor)
        self.mark_dirty(issue.id)
        self._conn.commit()

    def get_issue(self, issue_id: str) -> Issue | None:
        row = self._conn.execute(
            "SELECT * FROM issues WHERE id = ?", (issue_id,)
        ).fetchone()
        if row is None:
            return None
        issue = self._row_to_issue(row)
        # Load labels
        issue.labels = self.get_labels(issue_id)
        # Load dependencies
        issue.dependencies = self.get_dependency_records(issue_id)
        # Load comments
        issue.comments = self.get_comments(issue_id)
        return issue

    def update_issue(self, issue_id: str, updates: dict[str, Any], actor: str) -> None:
        # Fetch current for event recording
        current = self.get_issue(issue_id)
        if current is None:
            raise ValueError(f"Issue not found: {issue_id}")

        set_clauses = []
        params: list[Any] = []

        field_map = {
            "title": "title", "description": "description", "design": "design",
            "acceptance_criteria": "acceptance_criteria", "notes": "notes",
            "status": "status", "priority": "priority", "issue_type": "issue_type",
            "assignee": "assignee", "owner": "owner", "estimated_minutes": "estimated_minutes",
            "closed_at": "closed_at", "close_reason": "close_reason",
            "closed_by_session": "closed_by_session", "external_ref": "external_ref",
            "spec_id": "spec_id", "pinned": "pinned", "is_template": "is_template",
            "ephemeral": "ephemeral", "sender": "sender", "wisp_type": "wisp_type",
            "due_at": "due_at", "defer_until": "defer_until",
            "source_system": "source_system", "metadata": "metadata",
            "created_by": "created_by",
            "hook_bead": "hook_bead", "role_bead": "role_bead",
            "agent_state": "agent_state", "role_type": "role_type", "rig": "rig",
            "mol_type": "mol_type", "work_type": "work_type",
            "event_kind": "event_kind", "actor": "actor", "target": "target",
            "payload": "payload", "holder": "holder",
        }

        for key, value in updates.items():
            col = field_map.get(key, key)
            if col in ("pinned", "is_template", "ephemeral", "crystallizes"):
                value = int(bool(value)) if value is not None else 0
            elif col in ("closed_at", "due_at", "defer_until", "last_activity"):
                if isinstance(value, datetime):
                    value = format_timestamp(value)
            set_clauses.append(f"{col} = ?")
            params.append(value)

        # Always update updated_at
        set_clauses.append("updated_at = ?")
        params.append(format_timestamp(now_utc()))

        # Recompute content hash
        for key, value in updates.items():
            if hasattr(current, key):
                setattr(current, key, value)
        current.content_hash = current.compute_content_hash()
        set_clauses.append("content_hash = ?")
        params.append(current.content_hash)

        params.append(issue_id)
        sql = f"UPDATE issues SET {', '.join(set_clauses)} WHERE id = ?"
        self._conn.execute(sql, params)

        # Record status change event
        if "status" in updates:
            old_status = getattr(current, "status", "")
            self._record_event(issue_id, EventType.STATUS_CHANGED, actor,
                               old_status, updates["status"])
        else:
            self._record_event(issue_id, EventType.UPDATED, actor)

        self.mark_dirty(issue_id)
        self._conn.commit()

    def close_issue(self, issue_id: str, reason: str, actor: str) -> None:
        now = now_utc()
        self._conn.execute(
            "UPDATE issues SET status = ?, closed_at = ?, close_reason = ?, updated_at = ? WHERE id = ?",
            (Status.CLOSED, format_timestamp(now), reason, format_timestamp(now), issue_id)
        )
        self._record_event(issue_id, EventType.CLOSED, actor, comment=reason)
        self.mark_dirty(issue_id)
        self._conn.commit()

    def reopen_issue(self, issue_id: str, actor: str) -> None:
        now = now_utc()
        self._conn.execute(
            "UPDATE issues SET status = ?, closed_at = NULL, close_reason = '', updated_at = ? WHERE id = ?",
            (Status.OPEN, format_timestamp(now), issue_id)
        )
        self._record_event(issue_id, EventType.REOPENED, actor)
        self.mark_dirty(issue_id)
        self._conn.commit()

    def delete_issue(self, issue_id: str) -> None:
        self._conn.execute("DELETE FROM issues WHERE id = ?", (issue_id,))
        self._conn.commit()

    # --- Query ---

    def _build_filter_sql(self, f: IssueFilter, base_where: str = "1=1") -> tuple[str, list[Any]]:
        """Build WHERE clause from IssueFilter."""
        clauses = [base_where]
        params: list[Any] = []

        if not f.include_tombstones:
            clauses.append("i.status != 'tombstone'")

        if f.status is not None:
            clauses.append("i.status = ?")
            params.append(f.status)

        if f.priority is not None:
            clauses.append("i.priority = ?")
            params.append(f.priority)

        if f.priority_min is not None:
            clauses.append("i.priority >= ?")
            params.append(f.priority_min)

        if f.priority_max is not None:
            clauses.append("i.priority <= ?")
            params.append(f.priority_max)

        if f.issue_type is not None:
            clauses.append("i.issue_type = ?")
            params.append(f.issue_type)

        if f.assignee is not None:
            clauses.append("i.assignee = ?")
            params.append(f.assignee)

        if f.no_assignee:
            clauses.append("(i.assignee IS NULL OR i.assignee = '')")

        if f.title_search:
            clauses.append("(i.title LIKE ? OR i.description LIKE ? OR i.id LIKE ?)")
            pattern = f"%{f.title_search}%"
            params.extend([pattern, pattern, pattern])

        if f.title_contains:
            clauses.append("i.title LIKE ?")
            params.append(f"%{f.title_contains}%")

        if f.description_contains:
            clauses.append("i.description LIKE ?")
            params.append(f"%{f.description_contains}%")

        if f.notes_contains:
            clauses.append("i.notes LIKE ?")
            params.append(f"%{f.notes_contains}%")

        if f.ids:
            placeholders = ",".join("?" * len(f.ids))
            clauses.append(f"i.id IN ({placeholders})")
            params.extend(f.ids)

        if f.id_prefix:
            clauses.append("i.id LIKE ?")
            params.append(f"{f.id_prefix}%")

        if f.labels:
            for label in f.labels:
                clauses.append("EXISTS (SELECT 1 FROM labels l WHERE l.issue_id = i.id AND l.label = ?)")
                params.append(label)

        if f.labels_any:
            placeholders = ",".join("?" * len(f.labels_any))
            clauses.append(f"EXISTS (SELECT 1 FROM labels l WHERE l.issue_id = i.id AND l.label IN ({placeholders}))")
            params.extend(f.labels_any)

        if f.created_after:
            clauses.append("i.created_at > ?")
            params.append(format_timestamp(f.created_after))

        if f.created_before:
            clauses.append("i.created_at < ?")
            params.append(format_timestamp(f.created_before))

        if f.updated_after:
            clauses.append("i.updated_at > ?")
            params.append(format_timestamp(f.updated_after))

        if f.updated_before:
            clauses.append("i.updated_at < ?")
            params.append(format_timestamp(f.updated_before))

        if f.ephemeral is not None:
            clauses.append("i.ephemeral = ?")
            params.append(int(f.ephemeral))

        if f.pinned is not None:
            clauses.append("i.pinned = ?")
            params.append(int(f.pinned))

        if f.is_template is not None:
            clauses.append("i.is_template = ?")
            params.append(int(f.is_template))

        if f.exclude_status:
            placeholders = ",".join("?" * len(f.exclude_status))
            clauses.append(f"i.status NOT IN ({placeholders})")
            params.extend(f.exclude_status)

        if f.exclude_types:
            placeholders = ",".join("?" * len(f.exclude_types))
            clauses.append(f"i.issue_type NOT IN ({placeholders})")
            params.extend(f.exclude_types)

        if f.parent_id is not None:
            clauses.append("EXISTS (SELECT 1 FROM dependencies d WHERE d.issue_id = i.id AND d.depends_on_id = ? AND d.type = 'parent-child')")
            params.append(f.parent_id)

        if f.overdue:
            clauses.append("i.due_at IS NOT NULL AND i.due_at < ? AND i.status != 'closed'")
            params.append(format_timestamp(now_utc()))

        where = " AND ".join(clauses)
        return where, params

    def search_issues(self, query: str, filter: IssueFilter) -> list[Issue]:
        if query:
            filter.title_search = query
        where, params = self._build_filter_sql(filter)
        sql = f"SELECT * FROM issues i WHERE {where} ORDER BY i.created_at DESC"
        if filter.limit > 0:
            sql += f" LIMIT {filter.limit}"
        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_issue(row) for row in rows]

    def list_issues(self, filter: IssueFilter, sort_by: str = "created_at",
                    reverse: bool = False) -> list[Issue]:
        where, params = self._build_filter_sql(filter)

        # Map sort field names
        sort_map = {
            "created": "i.created_at",
            "created_at": "i.created_at",
            "updated": "i.updated_at",
            "updated_at": "i.updated_at",
            "priority": "i.priority",
            "status": "i.status",
            "title": "i.title",
            "id": "i.id",
            "type": "i.issue_type",
        }
        order_col = sort_map.get(sort_by, "i.created_at")
        order_dir = "ASC" if reverse else "DESC"
        if sort_by == "priority":
            order_dir = "DESC" if reverse else "ASC"  # Lower number = higher priority

        sql = f"SELECT * FROM issues i WHERE {where} ORDER BY {order_col} {order_dir}"
        if filter.limit > 0:
            sql += f" LIMIT {filter.limit}"
        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_issue(row) for row in rows]

    def get_ready_work(self, filter: dict[str, Any] | None = None) -> list[Issue]:
        """Get issues ready to work on: open, not blocked, not deferred, not ephemeral."""
        now = format_timestamp(now_utc())
        sql = """
            SELECT i.* FROM issues i
            WHERE i.status = 'open'
              AND (i.ephemeral = 0 OR i.ephemeral IS NULL)
              AND (i.pinned = 0 OR i.pinned IS NULL)
              AND (i.defer_until IS NULL OR i.defer_until <= ?)
              AND NOT EXISTS (
                SELECT 1 FROM dependencies d
                JOIN issues blocker ON d.depends_on_id = blocker.id
                WHERE d.issue_id = i.id
                  AND d.type IN ('blocks', 'parent-child', 'conditional-blocks', 'waits-for')
                  AND blocker.status IN ('open', 'in_progress', 'blocked', 'deferred', 'hooked')
              )
        """
        params: list[Any] = [now]

        if filter:
            if filter.get("type"):
                sql += " AND i.issue_type = ?"
                params.append(filter["type"])
            if filter.get("priority") is not None:
                sql += " AND i.priority = ?"
                params.append(filter["priority"])
            if filter.get("assignee") is not None:
                sql += " AND i.assignee = ?"
                params.append(filter["assignee"])
            if filter.get("unassigned"):
                sql += " AND (i.assignee IS NULL OR i.assignee = '')"
            if filter.get("labels"):
                for label in filter["labels"]:
                    sql += " AND EXISTS (SELECT 1 FROM labels l WHERE l.issue_id = i.id AND l.label = ?)"
                    params.append(label)

        # Default sort: priority ASC (0 first), then created_at ASC (oldest first)
        sql += " ORDER BY i.priority ASC, i.created_at ASC"

        if filter and filter.get("limit"):
            sql += f" LIMIT {filter['limit']}"

        rows = self._conn.execute(sql, params).fetchall()
        return [self._row_to_issue(row) for row in rows]

    def get_blocked_issues(self) -> list[tuple[Issue, list[str]]]:
        sql = """
            SELECT i.*, GROUP_CONCAT(d.depends_on_id) as blocker_ids
            FROM issues i
            JOIN dependencies d ON i.id = d.issue_id
            JOIN issues blocker ON d.depends_on_id = blocker.id
            WHERE i.status IN ('open', 'in_progress', 'blocked', 'deferred', 'hooked')
              AND d.type IN ('blocks', 'parent-child', 'conditional-blocks', 'waits-for')
              AND blocker.status IN ('open', 'in_progress', 'blocked', 'deferred', 'hooked')
            GROUP BY i.id
            ORDER BY i.priority ASC, i.created_at ASC
        """
        rows = self._conn.execute(sql).fetchall()
        result = []
        for row in rows:
            issue = self._row_to_issue(row)
            blocker_ids = (row["blocker_ids"] or "").split(",")
            result.append((issue, blocker_ids))
        return result

    # --- Dependencies ---

    def add_dependency(self, dep: Dependency, actor: str) -> None:
        # Cycle detection
        if self.has_cycle(dep.issue_id, dep.depends_on_id):
            raise ValueError(
                f"Adding dependency {dep.issue_id} → {dep.depends_on_id} would create a cycle"
            )
        self._conn.execute(
            "INSERT OR IGNORE INTO dependencies (issue_id, depends_on_id, type, created_at, created_by, metadata, thread_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (dep.issue_id, dep.depends_on_id, dep.type,
             format_timestamp(dep.created_at), dep.created_by or actor,
             dep.metadata, dep.thread_id)
        )
        self._record_event(dep.issue_id, EventType.DEPENDENCY_ADDED, actor,
                           new_value=dep.depends_on_id)
        self.mark_dirty(dep.issue_id)
        self.mark_dirty(dep.depends_on_id)
        self._conn.commit()

    def remove_dependency(self, issue_id: str, depends_on_id: str, actor: str) -> None:
        self._conn.execute(
            "DELETE FROM dependencies WHERE issue_id = ? AND depends_on_id = ?",
            (issue_id, depends_on_id)
        )
        self._record_event(issue_id, EventType.DEPENDENCY_REMOVED, actor,
                           old_value=depends_on_id)
        self.mark_dirty(issue_id)
        self.mark_dirty(depends_on_id)
        self._conn.commit()

    def get_dependencies(self, issue_id: str) -> list[Issue]:
        rows = self._conn.execute(
            "SELECT i.* FROM issues i JOIN dependencies d ON i.id = d.depends_on_id "
            "WHERE d.issue_id = ?",
            (issue_id,)
        ).fetchall()
        return [self._row_to_issue(row) for row in rows]

    def get_dependents(self, issue_id: str) -> list[Issue]:
        rows = self._conn.execute(
            "SELECT i.* FROM issues i JOIN dependencies d ON i.id = d.issue_id "
            "WHERE d.depends_on_id = ?",
            (issue_id,)
        ).fetchall()
        return [self._row_to_issue(row) for row in rows]

    def get_dependency_records(self, issue_id: str) -> list[Dependency]:
        rows = self._conn.execute(
            "SELECT * FROM dependencies WHERE issue_id = ?", (issue_id,)
        ).fetchall()
        return [
            Dependency(
                issue_id=row["issue_id"],
                depends_on_id=row["depends_on_id"],
                type=row["type"],
                created_at=parse_timestamp(row["created_at"]) or now_utc(),
                created_by=row["created_by"] or "",
                metadata=row["metadata"] or "",
                thread_id=row["thread_id"] or "",
            )
            for row in rows
        ]

    def has_cycle(self, issue_id: str, depends_on_id: str) -> bool:
        """Check if adding issue_id → depends_on_id would create a cycle.

        Uses recursive CTE to walk the dependency graph from depends_on_id
        and check if it reaches issue_id.
        """
        # If they're the same, it's a self-cycle
        if issue_id == depends_on_id:
            return True
        row = self._conn.execute("""
            WITH RECURSIVE reachable(id, depth) AS (
                SELECT ?, 0
                UNION ALL
                SELECT d.depends_on_id, r.depth + 1
                FROM reachable r
                JOIN dependencies d ON d.issue_id = r.id
                WHERE r.depth < 100
            )
            SELECT 1 FROM reachable WHERE id = ? LIMIT 1
        """, (depends_on_id, issue_id)).fetchone()
        return row is not None

    # --- Labels ---

    def add_label(self, issue_id: str, label: str, actor: str) -> None:
        self._conn.execute(
            "INSERT OR IGNORE INTO labels (issue_id, label) VALUES (?, ?)",
            (issue_id, label)
        )
        self._record_event(issue_id, EventType.LABEL_ADDED, actor, new_value=label)
        self.mark_dirty(issue_id)
        self._conn.commit()

    def remove_label(self, issue_id: str, label: str, actor: str) -> None:
        self._conn.execute(
            "DELETE FROM labels WHERE issue_id = ? AND label = ?",
            (issue_id, label)
        )
        self._record_event(issue_id, EventType.LABEL_REMOVED, actor, old_value=label)
        self.mark_dirty(issue_id)
        self._conn.commit()

    def get_labels(self, issue_id: str) -> list[str]:
        rows = self._conn.execute(
            "SELECT label FROM labels WHERE issue_id = ? ORDER BY label",
            (issue_id,)
        ).fetchall()
        return [row["label"] for row in rows]

    # --- Comments ---

    def add_comment(self, issue_id: str, author: str, text: str) -> int:
        cur = self._conn.execute(
            "INSERT INTO comments (issue_id, author, text, created_at) VALUES (?, ?, ?, ?)",
            (issue_id, author, text, format_timestamp(now_utc()))
        )
        self._record_event(issue_id, EventType.COMMENTED, author, new_value=text)
        self.mark_dirty(issue_id)
        self._conn.commit()
        return cur.lastrowid or 0

    def get_comments(self, issue_id: str) -> list[Comment]:
        rows = self._conn.execute(
            "SELECT * FROM comments WHERE issue_id = ? ORDER BY created_at ASC",
            (issue_id,)
        ).fetchall()
        return [
            Comment(
                id=row["id"],
                issue_id=row["issue_id"],
                author=row["author"],
                text=row["text"],
                created_at=parse_timestamp(row["created_at"]) or now_utc(),
            )
            for row in rows
        ]

    def import_comment(self, issue_id: str, author: str, text: str,
                       created_at: datetime) -> int:
        cur = self._conn.execute(
            "INSERT INTO comments (issue_id, author, text, created_at) VALUES (?, ?, ?, ?)",
            (issue_id, author, text, format_timestamp(created_at))
        )
        self._conn.commit()
        return cur.lastrowid or 0

    # --- Events ---

    def get_events(self, issue_id: str) -> list[Event]:
        rows = self._conn.execute(
            "SELECT * FROM events WHERE issue_id = ? ORDER BY created_at ASC",
            (issue_id,)
        ).fetchall()
        return [
            Event(
                id=row["id"],
                issue_id=row["issue_id"],
                event_type=row["event_type"],
                actor=row["actor"],
                old_value=row["old_value"],
                new_value=row["new_value"],
                comment=row["comment"],
                created_at=parse_timestamp(row["created_at"]) or now_utc(),
            )
            for row in rows
        ]

    # --- Dirty tracking ---

    def mark_dirty(self, issue_id: str) -> None:
        self._conn.execute(
            "INSERT INTO dirty_issues (issue_id, marked_at) VALUES (?, ?) "
            "ON CONFLICT (issue_id) DO UPDATE SET marked_at = excluded.marked_at",
            (issue_id, format_timestamp(now_utc()))
        )

    def get_dirty_issues(self) -> list[str]:
        rows = self._conn.execute(
            "SELECT issue_id FROM dirty_issues ORDER BY marked_at ASC"
        ).fetchall()
        return [row["issue_id"] for row in rows]

    def clear_dirty(self, issue_ids: list[str]) -> None:
        if not issue_ids:
            return
        for issue_id in issue_ids:
            self._conn.execute(
                "DELETE FROM dirty_issues WHERE issue_id = ?", (issue_id,)
            )
        self._conn.commit()

    # --- Export hashes ---

    def get_export_hash(self, issue_id: str) -> str | None:
        row = self._conn.execute(
            "SELECT content_hash FROM export_hashes WHERE issue_id = ?",
            (issue_id,)
        ).fetchone()
        return row["content_hash"] if row else None

    def set_export_hash(self, issue_id: str, content_hash: str) -> None:
        self._conn.execute(
            "INSERT INTO export_hashes (issue_id, content_hash, exported_at) VALUES (?, ?, ?) "
            "ON CONFLICT (issue_id) DO UPDATE SET content_hash = excluded.content_hash, exported_at = excluded.exported_at",
            (issue_id, content_hash, format_timestamp(now_utc()))
        )
        self._conn.commit()

    def clear_all_export_hashes(self) -> None:
        self._conn.execute("DELETE FROM export_hashes")
        self._conn.commit()

    # --- Config ---

    def get_config(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM config WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_config(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO config (key, value) VALUES (?, ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (key, value)
        )
        self._conn.commit()

    def list_config(self) -> dict[str, str]:
        rows = self._conn.execute("SELECT key, value FROM config ORDER BY key").fetchall()
        return {row["key"]: row["value"] for row in rows}

    # --- Metadata ---

    def get_metadata(self, key: str) -> str | None:
        row = self._conn.execute(
            "SELECT value FROM metadata WHERE key = ?", (key,)
        ).fetchone()
        return row["value"] if row else None

    def set_metadata(self, key: str, value: str) -> None:
        self._conn.execute(
            "INSERT INTO metadata (key, value) VALUES (?, ?) "
            "ON CONFLICT (key) DO UPDATE SET value = excluded.value",
            (key, value)
        )
        self._conn.commit()

    # --- Statistics ---

    def get_statistics(self) -> Statistics:
        stats = Statistics()

        # Count by status
        rows = self._conn.execute(
            "SELECT status, COUNT(*) as cnt FROM issues WHERE status != 'tombstone' GROUP BY status"
        ).fetchall()
        for row in rows:
            status = row["status"]
            cnt = row["cnt"]
            if status == Status.OPEN:
                stats.open_issues = cnt
            elif status == Status.IN_PROGRESS:
                stats.in_progress_issues = cnt
            elif status == Status.CLOSED:
                stats.closed_issues = cnt
            elif status == Status.BLOCKED:
                stats.blocked_issues = cnt
            elif status == Status.DEFERRED:
                stats.deferred_issues = cnt
            elif status == Status.PINNED:
                stats.pinned_issues = cnt
            stats.total_issues += cnt

        # Tombstones counted separately
        row = self._conn.execute(
            "SELECT COUNT(*) as cnt FROM issues WHERE status = 'tombstone'"
        ).fetchone()
        stats.tombstone_issues = row["cnt"] if row else 0

        # Ready issues (not blocked among open)
        ready = self.get_ready_work()
        stats.ready_issues = len(ready)

        # By type
        rows = self._conn.execute(
            "SELECT issue_type, COUNT(*) as cnt FROM issues WHERE status != 'tombstone' GROUP BY issue_type"
        ).fetchall()
        stats.by_type = {row["issue_type"]: row["cnt"] for row in rows}

        # By priority
        rows = self._conn.execute(
            "SELECT priority, COUNT(*) as cnt FROM issues WHERE status != 'tombstone' GROUP BY priority"
        ).fetchall()
        stats.by_priority = {row["priority"]: row["cnt"] for row in rows}

        return stats

    # --- Child counters ---

    def next_child_number(self, parent_id: str) -> int:
        row = self._conn.execute(
            "SELECT last_child FROM child_counters WHERE parent_id = ?",
            (parent_id,)
        ).fetchone()
        if row:
            next_num = row["last_child"] + 1
            self._conn.execute(
                "UPDATE child_counters SET last_child = ? WHERE parent_id = ?",
                (next_num, parent_id)
            )
        else:
            next_num = 1
            self._conn.execute(
                "INSERT INTO child_counters (parent_id, last_child) VALUES (?, ?)",
                (parent_id, next_num)
            )
        self._conn.commit()
        return next_num

    # --- Transactions ---

    def run_in_transaction(self, fn: Any) -> None:
        try:
            fn(self)
            self._conn.commit()
        except Exception:
            self._conn.rollback()
            raise

    # --- Partial ID resolution ---

    def resolve_id(self, partial: str) -> str | None:
        """Resolve a partial ID to a full ID."""
        # Try exact match first
        row = self._conn.execute(
            "SELECT id FROM issues WHERE id = ?", (partial,)
        ).fetchone()
        if row:
            return row["id"]

        # Try prefix match
        rows = self._conn.execute(
            "SELECT id FROM issues WHERE id LIKE ?", (f"{partial}%",)
        ).fetchall()
        if len(rows) == 1:
            return rows[0]["id"]
        return None


def open_storage(db_path: str) -> SQLiteStorage:
    """Open or create a SQLite storage at the given path."""
    return SQLiteStorage(db_path)
