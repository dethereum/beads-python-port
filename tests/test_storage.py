"""Tests for SQLite storage."""

import os
import tempfile

import pytest

from beads.models import (
    Dependency, DepType, Issue, IssueFilter, Status, now_utc,
)
from beads.storage.sqlite_store import SQLiteStorage


@pytest.fixture
def store():
    """Create a temporary storage for testing."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    s = SQLiteStorage(path)
    s.set_config("issue_prefix", "test")
    yield s
    s.close()
    os.unlink(path)


def _make_issue(id: str, title: str = "Test", **kwargs) -> Issue:
    defaults = dict(
        id=id, title=title, status=Status.OPEN, priority=2,
        issue_type="task", created_at=now_utc(), updated_at=now_utc(),
    )
    defaults.update(kwargs)
    return Issue(**defaults)


class TestIssueCRUD:
    def test_create_and_get(self, store: SQLiteStorage):
        issue = _make_issue("test-abc123", "My Issue")
        store.create_issue(issue, "alice")
        got = store.get_issue("test-abc123")
        assert got is not None
        assert got.title == "My Issue"
        assert got.status == Status.OPEN

    def test_get_nonexistent(self, store: SQLiteStorage):
        assert store.get_issue("nonexistent") is None

    def test_update(self, store: SQLiteStorage):
        issue = _make_issue("test-1", "Original")
        store.create_issue(issue, "alice")
        store.update_issue("test-1", {"title": "Updated", "priority": 0}, "alice")
        got = store.get_issue("test-1")
        assert got.title == "Updated"
        assert got.priority == 0

    def test_close(self, store: SQLiteStorage):
        issue = _make_issue("test-1")
        store.create_issue(issue, "alice")
        store.close_issue("test-1", "Done", "alice")
        got = store.get_issue("test-1")
        assert got.status == Status.CLOSED
        assert got.close_reason == "Done"
        assert got.closed_at is not None

    def test_reopen(self, store: SQLiteStorage):
        issue = _make_issue("test-1")
        store.create_issue(issue, "alice")
        store.close_issue("test-1", "Done", "alice")
        store.reopen_issue("test-1", "alice")
        got = store.get_issue("test-1")
        assert got.status == Status.OPEN
        assert got.closed_at is None

    def test_delete(self, store: SQLiteStorage):
        issue = _make_issue("test-1")
        store.create_issue(issue, "alice")
        store.delete_issue("test-1")
        assert store.get_issue("test-1") is None


class TestDependencies:
    def test_add_and_get(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1", "Blocker"), "alice")
        store.create_issue(_make_issue("test-2", "Blocked"), "alice")
        dep = Dependency(
            issue_id="test-2", depends_on_id="test-1",
            type=DepType.BLOCKS, created_by="alice",
        )
        store.add_dependency(dep, "alice")
        deps = store.get_dependencies("test-2")
        assert len(deps) == 1
        assert deps[0].id == "test-1"

    def test_cycle_detection(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        store.create_issue(_make_issue("test-2"), "alice")
        dep1 = Dependency(issue_id="test-2", depends_on_id="test-1",
                          type=DepType.BLOCKS, created_by="alice")
        store.add_dependency(dep1, "alice")
        # Adding reverse should fail
        dep2 = Dependency(issue_id="test-1", depends_on_id="test-2",
                          type=DepType.BLOCKS, created_by="alice")
        with pytest.raises(ValueError, match="cycle"):
            store.add_dependency(dep2, "alice")

    def test_self_cycle(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        dep = Dependency(issue_id="test-1", depends_on_id="test-1",
                         type=DepType.BLOCKS, created_by="alice")
        with pytest.raises(ValueError, match="cycle"):
            store.add_dependency(dep, "alice")

    def test_remove(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        store.create_issue(_make_issue("test-2"), "alice")
        dep = Dependency(issue_id="test-2", depends_on_id="test-1",
                         type=DepType.BLOCKS, created_by="alice")
        store.add_dependency(dep, "alice")
        store.remove_dependency("test-2", "test-1", "alice")
        assert len(store.get_dependencies("test-2")) == 0


class TestReadyWork:
    def test_unblocked_is_ready(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        ready = store.get_ready_work()
        assert len(ready) == 1
        assert ready[0].id == "test-1"

    def test_blocked_not_ready(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        store.create_issue(_make_issue("test-2"), "alice")
        dep = Dependency(issue_id="test-2", depends_on_id="test-1",
                         type=DepType.BLOCKS, created_by="alice")
        store.add_dependency(dep, "alice")
        ready = store.get_ready_work()
        assert len(ready) == 1
        assert ready[0].id == "test-1"

    def test_unblocked_after_close(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        store.create_issue(_make_issue("test-2"), "alice")
        dep = Dependency(issue_id="test-2", depends_on_id="test-1",
                         type=DepType.BLOCKS, created_by="alice")
        store.add_dependency(dep, "alice")
        store.close_issue("test-1", "done", "alice")
        ready = store.get_ready_work()
        assert len(ready) == 1
        assert ready[0].id == "test-2"

    def test_closed_not_ready(self, store: SQLiteStorage):
        issue = _make_issue("test-1")
        store.create_issue(issue, "alice")
        store.close_issue("test-1", "done", "alice")
        ready = store.get_ready_work()
        assert len(ready) == 0


class TestLabels:
    def test_add_and_get(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        store.add_label("test-1", "urgent", "alice")
        store.add_label("test-1", "backend", "alice")
        labels = store.get_labels("test-1")
        assert sorted(labels) == ["backend", "urgent"]

    def test_remove(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        store.add_label("test-1", "urgent", "alice")
        store.remove_label("test-1", "urgent", "alice")
        assert store.get_labels("test-1") == []


class TestComments:
    def test_add_and_get(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        store.add_comment("test-1", "alice", "Hello world")
        comments = store.get_comments("test-1")
        assert len(comments) == 1
        assert comments[0].text == "Hello world"
        assert comments[0].author == "alice"


class TestPartialIDResolution:
    def test_exact_match(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-abc123"), "alice")
        assert store.resolve_id("test-abc123") == "test-abc123"

    def test_prefix_match(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-abc123"), "alice")
        assert store.resolve_id("test-abc") == "test-abc123"

    def test_ambiguous(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-abc1"), "alice")
        store.create_issue(_make_issue("test-abc2"), "alice")
        assert store.resolve_id("test-abc") is None

    def test_not_found(self, store: SQLiteStorage):
        assert store.resolve_id("nonexistent") is None


class TestDirtyTracking:
    def test_create_marks_dirty(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        dirty = store.get_dirty_issues()
        assert "test-1" in dirty

    def test_clear_dirty(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1"), "alice")
        store.clear_dirty(["test-1"])
        assert store.get_dirty_issues() == []


class TestStatistics:
    def test_basic_stats(self, store: SQLiteStorage):
        store.create_issue(_make_issue("test-1", issue_type="task"), "alice")
        store.create_issue(_make_issue("test-2", issue_type="bug"), "alice")
        store.close_issue("test-2", "fixed", "alice")
        stats = store.get_statistics()
        assert stats.total_issues == 2
        assert stats.open_issues == 1
        assert stats.closed_issues == 1
