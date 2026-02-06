"""Tests for JSONL export and import."""

import json
import os
import tempfile

import pytest

from beads.export import flush_to_jsonl
from beads.importer import import_jsonl, parse_jsonl
from beads.models import Dependency, DepType, Issue, Status, now_utc
from beads.storage.sqlite_store import SQLiteStorage


@pytest.fixture
def store():
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    s = SQLiteStorage(path)
    s.set_config("issue_prefix", "test")
    yield s
    s.close()
    os.unlink(path)


@pytest.fixture
def jsonl_path():
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    yield path
    if os.path.exists(path):
        os.unlink(path)


def _make_issue(id: str, title: str = "Test", **kwargs) -> Issue:
    return Issue(
        id=id, title=title, status=Status.OPEN, priority=2,
        issue_type="task", created_at=now_utc(), updated_at=now_utc(),
        **kwargs,
    )


class TestExport:
    def test_export_basic(self, store: SQLiteStorage, jsonl_path: str):
        store.create_issue(_make_issue("test-1", "Issue One"), "alice")
        store.create_issue(_make_issue("test-2", "Issue Two"), "alice")
        count = flush_to_jsonl(store, jsonl_path)
        assert count == 2

        # Verify JSONL format
        with open(jsonl_path) as f:
            lines = f.readlines()
        assert len(lines) == 2
        for line in lines:
            data = json.loads(line)
            assert "id" in data
            assert "title" in data
            assert "created_at" in data

    def test_export_with_labels(self, store: SQLiteStorage, jsonl_path: str):
        store.create_issue(_make_issue("test-1"), "alice")
        store.add_label("test-1", "urgent", "alice")
        flush_to_jsonl(store, jsonl_path)

        with open(jsonl_path) as f:
            data = json.loads(f.readline())
        assert data["labels"] == ["urgent"]

    def test_export_clears_dirty(self, store: SQLiteStorage, jsonl_path: str):
        store.create_issue(_make_issue("test-1"), "alice")
        assert len(store.get_dirty_issues()) > 0
        flush_to_jsonl(store, jsonl_path)
        assert len(store.get_dirty_issues()) == 0

    def test_export_with_dependencies(self, store: SQLiteStorage, jsonl_path: str):
        store.create_issue(_make_issue("test-1"), "alice")
        store.create_issue(_make_issue("test-2"), "alice")
        dep = Dependency(issue_id="test-2", depends_on_id="test-1",
                         type=DepType.BLOCKS, created_by="alice")
        store.add_dependency(dep, "alice")
        flush_to_jsonl(store, jsonl_path)

        with open(jsonl_path) as f:
            for line in f:
                data = json.loads(line)
                if data["id"] == "test-2":
                    assert len(data["dependencies"]) == 1
                    assert data["dependencies"][0]["depends_on_id"] == "test-1"


class TestImport:
    def test_import_basic(self, store: SQLiteStorage, jsonl_path: str):
        # Write test JSONL
        issue_data = {
            "id": "test-new1",
            "title": "Imported Issue",
            "status": "open",
            "priority": 1,
            "issue_type": "bug",
            "created_at": "2026-01-15T10:00:00Z",
            "updated_at": "2026-01-15T10:00:00Z",
        }
        with open(jsonl_path, "w") as f:
            f.write(json.dumps(issue_data) + "\n")

        result = import_jsonl(store, jsonl_path)
        assert result.created == 1

        got = store.get_issue("test-new1")
        assert got is not None
        assert got.title == "Imported Issue"
        assert got.priority == 1

    def test_import_idempotent(self, store: SQLiteStorage, jsonl_path: str):
        """Importing the same JSONL twice should not create duplicates."""
        issue_data = {
            "id": "test-idem",
            "title": "Idempotent",
            "status": "open",
            "priority": 2,
            "issue_type": "task",
            "created_at": "2026-01-15T10:00:00Z",
            "updated_at": "2026-01-15T10:00:00Z",
        }
        with open(jsonl_path, "w") as f:
            f.write(json.dumps(issue_data) + "\n")

        result1 = import_jsonl(store, jsonl_path)
        assert result1.created == 1
        result2 = import_jsonl(store, jsonl_path)
        assert result2.created == 0
        assert result2.unchanged == 1

    def test_roundtrip(self, store: SQLiteStorage, jsonl_path: str):
        """Create in Python, export, import into fresh DB - should match."""
        store.create_issue(_make_issue("test-rt", "Roundtrip", description="Test roundtrip"), "alice")
        store.add_label("test-rt", "important", "alice")
        flush_to_jsonl(store, jsonl_path)

        # Import into a fresh store
        fd, path2 = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        store2 = SQLiteStorage(path2)
        store2.set_config("issue_prefix", "test")
        result = import_jsonl(store2, jsonl_path)
        assert result.created == 1

        got = store2.get_issue("test-rt")
        assert got is not None
        assert got.title == "Roundtrip"
        assert got.description == "Test roundtrip"

        store2.close()
        os.unlink(path2)


class TestParseJSONL:
    def test_parse_deletion_markers(self, jsonl_path: str):
        with open(jsonl_path, "w") as f:
            f.write('{"id":"test-1","title":"Keep","status":"open","priority":2,"created_at":"2026-01-15T10:00:00Z","updated_at":"2026-01-15T10:00:00Z"}\n')
            f.write('{"id":"test-2","_deleted":true}\n')
        issues, deletion_ids = parse_jsonl(jsonl_path)
        assert len(issues) == 1
        assert issues[0].id == "test-1"
        assert deletion_ids == ["test-2"]

    def test_skip_malformed(self, jsonl_path: str):
        with open(jsonl_path, "w") as f:
            f.write('{"id":"test-1","title":"Good","status":"open","priority":2,"created_at":"2026-01-15T10:00:00Z","updated_at":"2026-01-15T10:00:00Z"}\n')
            f.write('not valid json\n')
            f.write('{"id":"test-2","title":"Also Good","status":"open","priority":2,"created_at":"2026-01-15T10:00:00Z","updated_at":"2026-01-15T10:00:00Z"}\n')
        issues, _ = parse_jsonl(jsonl_path)
        assert len(issues) == 2
